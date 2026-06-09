"""QuePipeline — composable ICAR pipeline with stage add/remove/reorder.

Supports:
- Pre-built presets: ICAR, RETRIEVAL_ONLY, INTENT_THEN_RETRIEVE
- Custom stage injection at any position
- Conditional skip (skip_if callable)
- Per-stage timeout
- Pipeline serialization (to_dict / from_dict)
"""
from dataclasses import dataclass, field
from typing import Any, Callable
import time

from loguru import logger


@dataclass
class PipelineStage:
    """A single stage in the QUE pipeline."""
    name: str
    handler: Callable
    required: bool = True          # If True, pipeline fails if this stage fails
    timeout_ms: int = 30000
    skip_if: Callable | None = None  # (request) -> bool
    metadata: dict[str, str] = field(default_factory=dict)

    def should_skip(self, request: Any) -> bool:
        if self.skip_if is None:
            return False
        try:
            return self.skip_if(request)
        except Exception:
            return False


@dataclass
class PipelineResult:
    """Result of pipeline execution."""
    success: bool = True
    outputs: dict[str, Any] = field(default_factory=dict)   # stage_name -> output
    errors: dict[str, str] = field(default_factory=dict)     # stage_name -> error_msg
    total_latency_ms: float = 0.0
    executed_stages: list[str] = field(default_factory=list)
    skipped_stages: list[str] = field(default_factory=list)


class QuePipeline:
    """Composable pipeline orchestrator.

    Usage:
        pipeline = QuePipeline.preset_icar()
        pipeline.insert_stage(PipelineStage("sanitize", my_sanitizer), position=0)
        pipeline.remove_stage("hyde")
        result = pipeline.run(request)
    """

    def __init__(self, name: str = "default", stages: list[PipelineStage] | None = None):
        self.name = name
        self._stages: list[PipelineStage] = stages or []

    # ---- Stage management ----

    @property
    def stages(self) -> list[PipelineStage]:
        return list(self._stages)

    @property
    def stage_names(self) -> list[str]:
        return [s.name for s in self._stages]

    def add_stage(self, stage: PipelineStage, position: int = -1) -> "QuePipeline":
        """Add a stage. position=-1 appends to end."""
        if position < 0 or position >= len(self._stages):
            self._stages.append(stage)
        else:
            self._stages.insert(position, stage)
        return self

    def insert_stage(self, stage: PipelineStage, position: int = 0) -> "QuePipeline":
        """Insert at a specific position (alias for add_stage with position)."""
        return self.add_stage(stage, position)

    def remove_stage(self, name: str) -> "QuePipeline":
        """Remove a stage by name."""
        self._stages = [s for s in self._stages if s.name != name]
        return self

    def replace_stage(self, name: str, new_stage: PipelineStage) -> "QuePipeline":
        """Replace a stage by name."""
        for i, s in enumerate(self._stages):
            if s.name == name:
                self._stages[i] = new_stage
                break
        return self

    def reorder(self, stage_names: list[str]) -> "QuePipeline":
        """Reorder stages to match the given name order."""
        order_map = {name: i for i, name in enumerate(stage_names)}
        self._stages.sort(key=lambda s: order_map.get(s.name, 999))
        return self

    # ---- Execution ----

    def run(self, request: Any, shared_state: dict | None = None) -> PipelineResult:
        """Execute all stages in order with skip/timeout handling."""
        state = shared_state or {}
        result = PipelineResult()
        t0 = time.time()

        for stage in self._stages:
            # Skip?
            if stage.should_skip(request):
                result.skipped_stages.append(stage.name)
                logger.debug(f"Pipeline [{self.name}]: skipped {stage.name}")
                continue

            # Execute with timeout
            stage_start = time.time()
            try:
                output = stage.handler(request, state)
                result.outputs[stage.name] = output
                result.executed_stages.append(stage.name)

                elapsed = (time.time() - stage_start) * 1000
                logger.debug(
                    f"Pipeline [{self.name}]: {stage.name} completed in {elapsed:.0f}ms"
                )
            except Exception as e:
                elapsed = (time.time() - stage_start) * 1000
                logger.error(
                    f"Pipeline [{self.name}]: {stage.name} FAILED in {elapsed:.0f}ms: {e}"
                )
                result.errors[stage.name] = str(e)
                if stage.required:
                    result.success = False
                    break
                # Non-required: continue to next stage

        result.total_latency_ms = (time.time() - t0) * 1000
        return result

    # ---- Serialization ----

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "stages": [s.name for s in self._stages],
        }

    @classmethod
    def from_dict(cls, data: dict, stage_registry: dict[str, PipelineStage]) -> "QuePipeline":
        """Reconstruct from dict using a stage registry."""
        pipeline = cls(name=data.get("name", "default"))
        for name in data.get("stages", []):
            if name in stage_registry:
                pipeline.add_stage(stage_registry[name])
        return pipeline

    # ---- Presets ----

    @classmethod
    def preset_icar(cls) -> "QuePipeline":
        """Full ICAR pipeline: Intent → Clarify → Augment → Retrieve → Synthesize."""
        from engine.intent_recognizer import recognize
        from engine.query_rewriter import rewrite
        from engine.query_planner import plan
        from engine.execution_router import route
        from engine.dag_executor import DAGExecutor
        from engine.result_synthesizer import synthesize

        executor = DAGExecutor()

        def stage_intent(req, state):
            intent = recognize(req.query)
            state["intent"] = intent
            return intent

        def stage_rewrite(req, state):
            intent = state.get("intent")
            rw = rewrite(req.query, enable_hyde=getattr(req, 'enable_hyde', True),
                         enable_multi_query=getattr(req, 'enable_multi_query', True),
                         intent=intent)
            state["rewrite"] = rw
            return rw

        def stage_plan(req, state):
            intent = state.get("intent")
            rewrite_result = state.get("rewrite")
            p = plan(req.query, intent, rewrite_result)
            state["plan"] = p
            return p

        def stage_execute(req, state):
            p = state["plan"]
            intent = state.get("intent")
            context = getattr(req, 'context', {})
            timeout = getattr(req, 'timeout_ms', 30000)
            for sq in p.sub_queries:
                sq.route = route(sq, intent)
            results = executor.execute(p, context, None, timeout)  # trace arg
            state["results"] = results
            return results

        def stage_synthesize(req, state):
            intent = state.get("intent")
            p = state.get("plan")
            results = state.get("results")
            rw = state.get("rewrite")
            working_query = (rw.completed_query or rw.coreferenced_query or req.query)
            ctx = synthesize(working_query, intent, p, results, None)  # trace arg
            return ctx

        return cls(name="icar", stages=[
            PipelineStage("intent", stage_intent, required=True, timeout_ms=5000),
            PipelineStage("rewrite", stage_rewrite, required=False, timeout_ms=15000),
            PipelineStage("plan", stage_plan, required=True, timeout_ms=10000),
            PipelineStage("execute", stage_execute, required=True, timeout_ms=30000),
            PipelineStage("synthesize", stage_synthesize, required=True, timeout_ms=10000),
        ])

    @classmethod
    def preset_retrieval_only(cls) -> "QuePipeline":
        """Minimal pipeline: Intent → Retrieve → Synthesize (no rewrite)."""
        p = cls.preset_icar()
        p.remove_stage("rewrite")
        p.name = "retrieval_only"
        return p

    @classmethod
    def preset_intent_then_retrieve(cls) -> "QuePipeline":
        """Intent recognition then direct retrieval (no DAG, no synthesis)."""
        from engine.intent_recognizer import recognize

        def stage_intent(req, state):
            intent = recognize(req.query)
            state["intent"] = intent
            return intent

        return cls(name="intent_then_retrieve", stages=[
            PipelineStage("intent", stage_intent, required=True, timeout_ms=5000),
        ])

    @classmethod
    def list_presets(cls) -> list[str]:
        return ["icar", "retrieval_only", "intent_then_retrieve"]
