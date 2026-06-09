"""Kafka Consumer — async QUE pipeline execution from message queue.

Consumes messages from configurable Kafka topic and feeds them into QUE.
Results can be published to an output topic or stored via callback.

Message format (JSON):
{
  "query": "...",
  "context": {"kb_ids": "1,2,3"},
  "tenant_id": "default",
  "reply_topic": "que.results.{tenant_id}",
  "trace_id": "...",
  "params": {}
}
"""
import json
import threading
import time
import uuid
from typing import Any, Callable

from loguru import logger

from common.config_loader import get_config


class QueKafkaConsumer:
    """Kafka consumer that feeds messages into QUE pipeline.

    Usage:
        consumer = QueKafkaConsumer(
            bootstrap_servers="localhost:9092",
            topic="que.requests",
            group_id="que-consumer",
        )
        consumer.on_result = lambda msg, response: print(response)
        consumer.start()
    """

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        topic: str | None = None,
        group_id: str = "que-consumer",
        max_poll_records: int = 10,
        session_timeout_ms: int = 30000,
    ):
        cfg = get_config().get("kafka", {})
        self._bootstrap_servers = bootstrap_servers or cfg.get("bootstrap_servers", "localhost:9092")
        self._topic = topic or cfg.get("request_topic", "que.requests")
        self._group_id = group_id
        self._max_poll_records = max_poll_records
        self._session_timeout_ms = session_timeout_ms
        self._running = False
        self._thread: threading.Thread | None = None
        self._consumer = None
        self.on_result: Callable | None = None

    def start(self) -> None:
        """Start consuming in a background thread."""
        if self._running:
            return

        try:
            from kafka import KafkaConsumer
            self._consumer = KafkaConsumer(
                self._topic,
                bootstrap_servers=self._bootstrap_servers,
                group_id=self._group_id,
                max_poll_records=self._max_poll_records,
                session_timeout_ms=self._session_timeout_ms,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
            )
        except ImportError:
            logger.error("kafka-python not installed. Install with: pip install kafka-python")
            return
        except Exception as e:
            logger.error(f"Failed to create Kafka consumer: {e}")
            return

        self._running = True
        self._thread = threading.Thread(target=self._consume_loop, daemon=True)
        self._thread.start()
        logger.info(f"Kafka consumer started: {self._topic} @ {self._bootstrap_servers}")

    def stop(self) -> None:
        self._running = False
        if self._consumer:
            self._consumer.close()
        if self._thread:
            self._thread.join(timeout=10)
        logger.info("Kafka consumer stopped")

    def _consume_loop(self) -> None:
        """Main consume loop — processes messages and feeds QUE."""
        from engine.intent_recognizer import recognize
        from engine.query_rewriter import rewrite
        from engine.query_planner import plan
        from engine.execution_router import route
        from engine.dag_executor import DAGExecutor
        from engine.result_synthesizer import synthesize
        from engine.models import ExecutionTrace

        executor = DAGExecutor()

        for msg in self._consumer:
            if not self._running:
                break

            try:
                data = msg.value
                query = data.get("query", "")
                context = data.get("context", {})
                tenant_id = data.get("tenant_id", "default")
                trace_id = data.get("trace_id", str(uuid.uuid4())[:8])

                logger.info(f"Kafka msg: trace={trace_id}, query='{query[:80]}'")

                trace = ExecutionTrace()
                t0 = time.time()

                intent = recognize(query)
                rw = rewrite(query, enable_hyde=True, enable_multi_query=True, intent=intent)
                dag = plan(query, intent, rw)
                for sq in dag.sub_queries:
                    sq.route = route(sq, intent)
                results = executor.execute(dag, context, trace, 30000)
                wq = rw.completed_query or rw.coreferenced_query or query
                ctx = synthesize(wq, intent, dag, results, trace)

                response = {
                    "trace_id": trace_id,
                    "original_query": query,
                    "synthesized_context": ctx,
                    "intent": intent.primary_intent,
                    "sub_queries": dag.total_queries,
                    "ok_results": sum(1 for r in results if r.success),
                    "total_latency_ms": (time.time() - t0) * 1000,
                }

                if self.on_result:
                    self.on_result(data, response)

                # Optionally produce to reply topic
                reply_topic = data.get("reply_topic")
                if reply_topic:
                    self._produce_reply(reply_topic, response)

            except Exception as e:
                logger.error(f"Kafka message processing failed: {e}")

    def _produce_reply(self, topic: str, response: dict) -> None:
        """Produce response to a reply topic."""
        try:
            from kafka import KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=self._bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            producer.send(topic, response)
            producer.flush()
        except Exception as e:
            logger.error(f"Failed to produce reply to {topic}: {e}")
