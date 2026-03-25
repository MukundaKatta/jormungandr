"""
Data validation for Jormungandr pipelines.

Provides schema-based validation for ``list[dict]`` datasets, with pluggable
checkers for nulls, types, ranges, and pattern matching.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Type


# ---------------------------------------------------------------------------
# Schema & report data classes
# ---------------------------------------------------------------------------

@dataclass
class SchemaRule:
    """A single validation rule for one field.

    Attributes:
        field: Name of the field to validate.
        type: Expected Python type (e.g. ``int``, ``str``, ``float``).
        required: If ``True``, the field must be present (non-``None``).
        min: Minimum value (inclusive) for numeric fields.
        max: Maximum value (inclusive) for numeric fields.
        pattern: Regex pattern the string value must match.
    """
    field: str
    type: Optional[Type] = None
    required: bool = False
    min: Optional[float] = None
    max: Optional[float] = None
    pattern: Optional[str] = None


@dataclass
class ValidationError:
    """One specific validation failure."""
    row_index: int
    field: str
    rule: str
    message: str


@dataclass
class ValidationReport:
    """Summary of a validation pass over a dataset.

    Attributes:
        is_valid: ``True`` when zero errors were found.
        errors: Ordered list of ``ValidationError`` instances.
        total_rows: Number of rows that were validated.
        error_count: Number of errors found.
    """
    is_valid: bool = True
    errors: List[ValidationError] = field(default_factory=list)
    total_rows: int = 0
    error_count: int = 0

    def add_error(self, row_index: int, field_name: str, rule: str, message: str) -> None:
        self.errors.append(ValidationError(row_index, field_name, rule, message))
        self.error_count += 1
        self.is_valid = False

    def summary(self) -> Dict[str, Any]:
        return {
            "is_valid": self.is_valid,
            "total_rows": self.total_rows,
            "error_count": self.error_count,
        }


# ---------------------------------------------------------------------------
# Individual checkers
# ---------------------------------------------------------------------------

class NullChecker:
    """Check that required fields are present and non-``None``."""

    def check(self, row: Dict[str, Any], rule: SchemaRule, row_index: int, report: ValidationReport) -> None:
        if not rule.required:
            return
        if rule.field not in row or row[rule.field] is None:
            report.add_error(
                row_index, rule.field, "required",
                f"Field '{rule.field}' is required but missing or None",
            )


class TypeChecker:
    """Check that a field's value matches the expected type."""

    def check(self, row: Dict[str, Any], rule: SchemaRule, row_index: int, report: ValidationReport) -> None:
        if rule.type is None:
            return
        value = row.get(rule.field)
        if value is None:
            return  # NullChecker handles missing values
        if not isinstance(value, rule.type):
            report.add_error(
                row_index, rule.field, "type",
                f"Expected {rule.type.__name__}, got {type(value).__name__}",
            )


class RangeChecker:
    """Check that a numeric field falls within [min, max]."""

    def check(self, row: Dict[str, Any], rule: SchemaRule, row_index: int, report: ValidationReport) -> None:
        value = row.get(rule.field)
        if value is None:
            return
        if rule.min is not None and value < rule.min:
            report.add_error(
                row_index, rule.field, "min",
                f"Value {value} is below minimum {rule.min}",
            )
        if rule.max is not None and value > rule.max:
            report.add_error(
                row_index, rule.field, "max",
                f"Value {value} exceeds maximum {rule.max}",
            )


class PatternChecker:
    """Check that a string field matches a regex pattern."""

    def check(self, row: Dict[str, Any], rule: SchemaRule, row_index: int, report: ValidationReport) -> None:
        if rule.pattern is None:
            return
        value = row.get(rule.field)
        if value is None:
            return
        if not isinstance(value, str):
            return
        if not re.match(rule.pattern, value):
            report.add_error(
                row_index, rule.field, "pattern",
                f"Value '{value}' does not match pattern '{rule.pattern}'",
            )


# ---------------------------------------------------------------------------
# DataValidator
# ---------------------------------------------------------------------------

class DataValidator:
    """Schema-based validator for ``list[dict]`` datasets.

    Uses a set of ``SchemaRule`` objects and runs each through the registered
    checkers (null, type, range, pattern) for every row.
    """

    def __init__(self, rules: Optional[List[SchemaRule]] = None):
        self.rules: List[SchemaRule] = list(rules) if rules else []
        self._checkers = [
            NullChecker(),
            TypeChecker(),
            RangeChecker(),
            PatternChecker(),
        ]

    def add_rule(self, rule: SchemaRule) -> None:
        """Append a rule to the validator."""
        self.rules.append(rule)

    def validate(self, data: List[Dict[str, Any]]) -> ValidationReport:
        """Validate *data* against all registered rules.

        Returns a ``ValidationReport`` summarising any problems found.
        """
        report = ValidationReport(total_rows=len(data))

        for idx, row in enumerate(data):
            for rule in self.rules:
                for checker in self._checkers:
                    checker.check(row, rule, idx, report)

        return report

    def validate_as_handler(self, data: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Pipeline-stage compatible handler.

        Validates *data* and raises ``ValueError`` if any errors are found,
        otherwise passes *data* through unchanged.
        """
        report = self.validate(data)
        if not report.is_valid:
            messages = [e.message for e in report.errors[:5]]
            raise ValueError(
                f"Validation failed with {report.error_count} error(s): "
                + "; ".join(messages)
            )
        return data
