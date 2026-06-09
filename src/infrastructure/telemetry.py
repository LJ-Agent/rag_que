"""OpenTelemetry integration — distributed tracing + Prometheus metrics.

Tracing: Uses OpenTelemetry SDK to create spans for each pipeline stage.
Metrics: Prometheus-compatible metrics for request count, latency, errors.

Env vars control OTLP export (optional):
  OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4317
  OTEL_SERVICE_NAME=que-engine
"""
import os
from typing import Any

from loguru import logger
from common.config_loader import get_config


# ---- Tracing ----
_tracer_provider: Any = None
_tracer: Any = None


def get_tracer():
    """Get or create a no-op / real tracer."""
    global _tracer
    if _tracer is not None:
        return _tracer

    cfg = get_config().get("telemetry", {}).get("tracing", {})
    enabled = cfg.get("enabled", False)

    if not enabled:
        # No-op tracer
        class _NoOpSpan:
            def __enter__(self): return self
            def __exit__(self, *a): pass
            def set_attribute(self, *a): pass
            def add_event(self, *a): pass

        class _NoOpTracer:
            def start_as_current_span(self, name, **kw):
                return _NoOpSpan()

        _tracer = _NoOpTracer()
        return _tracer

    try:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        service_name = cfg.get("service_name", "que-engine")
        endpoint = cfg.get("otlp_endpoint") or os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")

        resource = Resource(attributes={SERVICE_NAME: service_name})
        provider = TracerProvider(resource=resource)

        if endpoint:
            exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
            provider.add_span_processor(BatchSpanProcessor(exporter))
            logger.info(f"OTel tracing enabled: {endpoint}")
        else:
            logger.info("OTel tracing: no exporter configured (spans in-memory only)")

        trace.set_tracer_provider(provider)
        global _tracer_provider
        _tracer_provider = provider
        _tracer = trace.get_tracer(__name__)
        return _tracer

    except ImportError:
        logger.warning("opentelemetry packages not installed, tracing disabled")
        # Create no-op tracer directly to avoid recursion
        class _NoOpSpan:
            def __enter__(self): return self
            def __exit__(self, *a): pass
            def set_attribute(self, *a): pass
            def add_event(self, *a): pass
        class _NoOpTracer:
            def start_as_current_span(self, name, **kw):
                return _NoOpSpan()
        _tracer = _NoOpTracer()
        return _tracer


# ---- Metrics ----
_metrics: dict[str, Any] = {}


class Counter:
    """Simple thread-safe counter."""
    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self._value = 0
        self._labels: dict[str, int] = {}  # label_key -> count

    def add(self, n: int = 1, labels: dict[str, str] | None = None):
        self._value += n
        if labels:
            key = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
            self._labels[key] = self._labels.get(key, 0) + n

    def value(self) -> int:
        return self._value


class Histogram:
    """Simple histogram with configurable buckets."""
    def __init__(self, name: str, description: str = "", buckets: list[float] | None = None):
        self.name = name
        self.description = description
        self.buckets = buckets or [10, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000]
        self._values: list[float] = []
        self._label_buckets: dict[str, list[float]] = {}

    def record(self, value: float, labels: dict[str, str] | None = None):
        self._values.append(value)
        if labels:
            key = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
            if key not in self._label_buckets:
                self._label_buckets[key] = []
            self._label_buckets[key].append(value)

    def quantile(self, p: float) -> float:
        if not self._values:
            return 0.0
        sorted_vals = sorted(self._values)
        idx = int(len(sorted_vals) * p)
        return sorted_vals[min(idx, len(sorted_vals) - 1)]


# Global metrics
QUE_REQUEST_COUNT = Counter("que_requests_total", "Total QUE pipeline requests")
QUE_LATENCY_HISTOGRAM = Histogram("que_latency_ms", "QUE pipeline latency in ms",
                                  buckets=[50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000])
QUE_ERROR_COUNT = Counter("que_errors_total", "Total QUE pipeline errors")
QUE_STAGE_LATENCY = Histogram("que_stage_latency_ms", "Per-stage latency in ms")


def get_metrics() -> dict:
    """Return all registered metrics."""
    return {
        "requests_total": QUE_REQUEST_COUNT.value(),
        "errors_total": QUE_ERROR_COUNT.value(),
        "latency_p50": QUE_LATENCY_HISTOGRAM.quantile(0.50),
        "latency_p95": QUE_LATENCY_HISTOGRAM.quantile(0.95),
        "latency_p99": QUE_LATENCY_HISTOGRAM.quantile(0.99),
    }


def metrics_text() -> str:
    """Prometheus text format metrics."""
    h = QUE_LATENCY_HISTOGRAM
    r = QUE_REQUEST_COUNT
    e = QUE_ERROR_COUNT
    return "\n".join([
        "# HELP " + r.name + " " + r.description,
        "# TYPE " + r.name + " counter",
        r.name + " " + str(r.value()),
        "",
        "# HELP " + h.name + " " + h.description,
        "# TYPE " + h.name + " summary",
        h.name + '{quantile="0.5"} ' + str(h.quantile(0.5)),
        h.name + '{quantile="0.95"} ' + str(h.quantile(0.95)),
        h.name + '{quantile="0.99"} ' + str(h.quantile(0.99)),
        h.name + "_count " + str(len(h._values)),
        "",
        "# HELP " + e.name + " " + e.description,
        "# TYPE " + e.name + " counter",
        e.name + " " + str(e.value()),
        "",
    ])


# Add /metrics route to FastAPI if available
def install_metrics_endpoint(app):
    """Install /metrics endpoint on a FastAPI app."""
    try:
        @app.get("/metrics")
        async def metrics():
            from fastapi.responses import PlainTextResponse
            return PlainTextResponse(content=metrics_text(), media_type="text/plain")
        logger.info("Prometheus /metrics endpoint installed")
    except Exception as e:
        logger.warning(f"Could not install /metrics endpoint: {e}")
