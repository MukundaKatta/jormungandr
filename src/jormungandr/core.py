"""
Core pipeline engine for Jormungandr.

Provides the foundational building blocks for constructing end-to-end data
pipelines: stages, results, executors, batches, and a fluent builder API.
Like the World Serpent coiling around Midgard, the pipeline wraps data from
ingestion through transformation, validation, and loading.
"""

from __future__ import annotations

import time
import uuid
import copy
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Sequence

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class StageStatus(Enum):
    """Possible outcomes for a pipeline stage execution."""
    SUCCESS = "success"
    FAILURE = "failure"
    SKIPPED = "skipped"
    PARTIAL = "partial"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class StageResult:
    """Captures the outcome of executing a single pipeline stage.

    Attributes:
        status: Whether the stage succeeded, failed, was skipped, or partial.
        data: The output data produced by the stage (may be ``None``).
        errors: A list of error messages encountered during execution.
        duration_ms: Wall-clock time the stage took, in milliseconds.
        stage_name: Name of the stage that produced this result.
        metadata: Arbitrary key/value metadata attached by the handler.
    """
    status: StageStatus
    data: Any = None
    errors: List[str] = field(default_factory=list)
    duration_ms: float = 0.0
    stage_name: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_success(self) -> bool:
        """Return ``True`` when the stage completed without failure."""
        return self.status in (StageStatus.SUCCESS, StageStatus.PARTIAL)

    @property
    def is_failure(self) -> bool:
        """Return ``True`` when the stage failed outright."""
        return self.status == StageStatus.FAILURE

    def __repr__(self) -> str:
        return (
            f"StageResult(stage={self.stage_name!r}, status={self.status.value}, "
            f"errors={len(self.errors)}, duration_ms={self.duration_ms:.1f})"
        )


@dataclass
class PipelineStage:
    """A single named step in a pipeline.

    Attributes:
        name: Human-readable identifier for the stage.
        handler: Callable that receives ``(data, config)`` and returns data.
        config: Stage-specific configuration dictionary.
        skip_on_error: If ``True``, this stage is skipped when a previous
            stage has failed (default ``False``).
        retry_count: Number of automatic retries on failure (default ``0``).
        timeout_ms: Optional maximum execution time in milliseconds.
    """
    name: str
    handler: Callable[..., Any]
    config: Dict[str, Any] = field(default_factory=dict)
    skip_on_error: bool = False
    retry_count: int = 0
    timeout_ms: Optional[float] = None

    def execute(self, data: Any) -> StageResult:
        """Run the handler, measuring elapsed time and capturing errors."""
        attempts = 1 + max(0, self.retry_count)
        last_error: Optional[str] = None

        for attempt in range(attempts):
            start = time.monotonic()
            try:
                result_data = self.handler(data, self.config)
                elapsed = (time.monotonic() - start) * 1000.0
                return StageResult(
                    status=StageStatus.SUCCESS,
                    data=result_data,
                    duration_ms=elapsed,
                    stage_name=self.name,
                )
            except Exception as exc:
                elapsed = (time.monotonic() - start) * 1000.0
                last_error = f"[attempt {attempt + 1}] {type(exc).__name__}: {exc}"
                logger.warning(
                    "Stage %r attempt %d failed: %s",
                    self.name, attempt + 1, last_error,
                )

        return StageResult(
            status=StageStatus.FAILURE,
            errors=[last_error or "unknown error"],
            duration_ms=elapsed,
            stage_name=self.name,
        )

    def __repr__(self) -> str:
        return f"PipelineStage(name={self.name!r})"


@dataclass
class DataBatch:
    """A container that groups records for batch processing.

    Attributes:
        batch_id: Unique identifier for the batch.
        records: The list of data records in this batch.
        source: Optional label describing where the data came from.
        created_at: Epoch timestamp when the batch was created.
        metadata: Arbitrary key/value metadata about the batch.
    """
    records: List[Dict[str, Any]]
    batch_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    source: str = ""
    created_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def size(self) -> int:
        """Return the number of records in this batch."""
        return len(self.records)

    def is_empty(self) -> bool:
        """Return ``True`` if the batch contains no records."""
        return len(self.records) == 0

    def split(self, chunk_size: int) -> List["DataBatch"]:
        """Split the batch into smaller batches of at most *chunk_size*."""
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        chunks: List[DataBatch] = []
        for i in range(0, len(self.records), chunk_size):
            chunks.append(
                DataBatch(
                    records=self.records[i : i + chunk_size],
                    source=self.source,
                    metadata=dict(self.metadata),
                )
            )
        return chunks

    def merge(self, other: "DataBatch") -> "DataBatch":
        """Return a new batch combining this batch's records with *other*."""
        return DataBatch(
            records=self.records + other.records,
            source=self.source or other.source,
            metadata={**self.metadata, **other.metadata},
        )

    def clone(self) -> "DataBatch":
        """Return a deep copy of this batch."""
        return DataBatch(
            records=copy.deepcopy(self.records),
            batch_id=self.batch_id,
            source=self.source,
            created_at=self.created_at,
            metadata=dict(self.metadata),
        )

    def __repr__(self) -> str:
        return (
            f"DataBatch(id={self.batch_id!r}, records={self.size}, "
            f"source={self.source!r})"
        )


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

class Pipeline:
    """An ordered collection of stages that process data sequentially.

    Attributes:
        name: A human-readable name for the pipeline.
        stages: The ordered list of ``PipelineStage`` objects.
    """

    def __init__(self, name: str, stages: Optional[List[PipelineStage]] = None):
        self.name = name
        self.stages: List[PipelineStage] = list(stages) if stages else []
        self._hooks: Dict[str, List[Callable]] = {
            "before_stage": [],
            "after_stage": [],
            "on_error": [],
            "on_complete": [],
        }

    def add_stage(self, stage: PipelineStage) -> None:
        """Append a stage to the pipeline."""
        self.stages.append(stage)

    def remove_stage(self, name: str) -> bool:
        """Remove the first stage matching *name*. Returns ``True`` if found."""
        for i, s in enumerate(self.stages):
            if s.name == name:
                self.stages.pop(i)
                return True
        return False

    def get_stage(self, name: str) -> Optional[PipelineStage]:
        """Return the stage with the given *name*, or ``None``."""
        for s in self.stages:
            if s.name == name:
                return s
        return None

    def register_hook(self, event: str, callback: Callable) -> None:
        """Register a callback for a pipeline lifecycle event."""
        if event not in self._hooks:
            raise ValueError(f"Unknown hook event: {event!r}")
        self._hooks[event].append(callback)

    def _fire_hooks(self, event: str, **kwargs: Any) -> None:
        for cb in self._hooks.get(event, []):
            try:
                cb(**kwargs)
            except Exception as exc:
                logger.error("Hook %r raised: %s", event, exc)

    @property
    def stage_count(self) -> int:
        return len(self.stages)

    def __repr__(self) -> str:
        return f"Pipeline(name={self.name!r}, stages={self.stage_count})"


# ---------------------------------------------------------------------------
# PipelineExecutor
# ---------------------------------------------------------------------------

class PipelineExecutor:
    """Runs a pipeline's stages sequentially, collecting results and metrics.

    The executor passes each stage's output as the next stage's input. If a
    stage fails and the next stage has ``skip_on_error=True``, that stage is
    skipped rather than executed.
    """

    def __init__(self, pipeline: Pipeline, *, stop_on_failure: bool = True):
        self.pipeline = pipeline
        self.stop_on_failure = stop_on_failure
        self.results: List[StageResult] = []
        self._start_time: float = 0.0
        self._end_time: float = 0.0

    def run(self, initial_data: Any = None) -> List[StageResult]:
        """Execute every stage in order and return the list of results."""
        self.results = []
        data = initial_data
        has_failure = False
        self._start_time = time.monotonic()

        for stage in self.pipeline.stages:
            # Honour skip_on_error
            if has_failure and stage.skip_on_error:
                result = StageResult(
                    status=StageStatus.SKIPPED,
                    stage_name=stage.name,
                )
                self.results.append(result)
                logger.info("Skipping stage %r due to prior failure", stage.name)
                continue

            # Fire before-hook
            self.pipeline._fire_hooks("before_stage", stage=stage, data=data)

            result = stage.execute(data)
            self.results.append(result)

            # Fire after-hook
            self.pipeline._fire_hooks("after_stage", stage=stage, result=result)

            if result.is_failure:
                has_failure = True
                self.pipeline._fire_hooks("on_error", stage=stage, result=result)
                if self.stop_on_failure:
                    logger.error(
                        "Pipeline %r stopped: stage %r failed",
                        self.pipeline.name, stage.name,
                    )
                    break
            else:
                data = result.data

        self._end_time = time.monotonic()
        self.pipeline._fire_hooks("on_complete", results=self.results)
        return self.results

    @property
    def total_duration_ms(self) -> float:
        """Total wall-clock time of the most recent ``run()``, in ms."""
        return (self._end_time - self._start_time) * 1000.0

    @property
    def all_succeeded(self) -> bool:
        """Return ``True`` if every executed stage succeeded or was skipped."""
        return all(
            r.status in (StageStatus.SUCCESS, StageStatus.SKIPPED, StageStatus.PARTIAL)
            for r in self.results
        )

    @property
    def failed_stages(self) -> List[StageResult]:
        """Return only the results whose status is FAILURE."""
        return [r for r in self.results if r.is_failure]

    def summary(self) -> Dict[str, Any]:
        """Return a dictionary summarising the pipeline run."""
        return {
            "pipeline": self.pipeline.name,
            "stages_total": len(self.pipeline.stages),
            "stages_executed": len(self.results),
            "stages_succeeded": sum(1 for r in self.results if r.is_success),
            "stages_failed": sum(1 for r in self.results if r.is_failure),
            "stages_skipped": sum(
                1 for r in self.results if r.status == StageStatus.SKIPPED
            ),
            "total_duration_ms": round(self.total_duration_ms, 2),
        }

    def __repr__(self) -> str:
        return (
            f"PipelineExecutor(pipeline={self.pipeline.name!r}, "
            f"results={len(self.results)})"
        )


# ---------------------------------------------------------------------------
# PipelineBuilder  (fluent API)
# ---------------------------------------------------------------------------

class PipelineBuilder:
    """Fluent builder for constructing pipelines step by step.

    Example::

        pipeline = (
            PipelineBuilder("my-etl")
            .add("ingest", ingest_handler)
            .add("transform", transform_handler, config={"key": "val"})
            .add("validate", validate_handler, skip_on_error=True)
            .add("load", load_handler)
            .with_hook("on_error", error_callback)
            .build()
        )
    """

    def __init__(self, name: str):
        self._name = name
        self._stages: List[PipelineStage] = []
        self._hooks: Dict[str, List[Callable]] = {}

    def add(
        self,
        name: str,
        handler: Callable[..., Any],
        *,
        config: Optional[Dict[str, Any]] = None,
        skip_on_error: bool = False,
        retry_count: int = 0,
        timeout_ms: Optional[float] = None,
    ) -> "PipelineBuilder":
        """Append a stage to the pipeline being built."""
        self._stages.append(
            PipelineStage(
                name=name,
                handler=handler,
                config=config or {},
                skip_on_error=skip_on_error,
                retry_count=retry_count,
                timeout_ms=timeout_ms,
            )
        )
        return self

    def with_hook(self, event: str, callback: Callable) -> "PipelineBuilder":
        """Register a lifecycle hook on the pipeline being built."""
        self._hooks.setdefault(event, []).append(callback)
        return self

    def build(self) -> Pipeline:
        """Construct and return the configured ``Pipeline``."""
        if not self._stages:
            raise ValueError("Cannot build a pipeline with no stages")
        pipeline = Pipeline(self._name, list(self._stages))
        for event, callbacks in self._hooks.items():
            for cb in callbacks:
                pipeline.register_hook(event, cb)
        return pipeline

    def __repr__(self) -> str:
        return (
            f"PipelineBuilder(name={self._name!r}, stages={len(self._stages)})"
        )
