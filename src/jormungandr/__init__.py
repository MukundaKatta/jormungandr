"""
Jormungandr - End-to-end data pipeline framework.

Named after the Norse World Serpent that encircles the entire world,
Jormungandr wraps your data end-to-end: ingest, transform, validate, and load.
"""

from jormungandr.core import (
    Pipeline,
    PipelineStage,
    StageResult,
    PipelineExecutor,
    DataBatch,
    PipelineBuilder,
    StageStatus,
)
from jormungandr.transforms import (
    FilterTransform,
    MapTransform,
    AggregateTransform,
    JoinTransform,
    SortTransform,
    DeduplicateTransform,
)
from jormungandr.validators import (
    DataValidator,
    SchemaRule,
    ValidationReport,
    NullChecker,
    TypeChecker,
    RangeChecker,
)

__version__ = "0.1.0"
__all__ = [
    "Pipeline",
    "PipelineStage",
    "StageResult",
    "PipelineExecutor",
    "DataBatch",
    "PipelineBuilder",
    "StageStatus",
    "FilterTransform",
    "MapTransform",
    "AggregateTransform",
    "JoinTransform",
    "SortTransform",
    "DeduplicateTransform",
    "DataValidator",
    "SchemaRule",
    "ValidationReport",
    "NullChecker",
    "TypeChecker",
    "RangeChecker",
]
