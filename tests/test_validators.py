"""Tests for jormungandr.validators — data validation."""

import pytest
from jormungandr.validators import (
    DataValidator,
    NullChecker,
    RangeChecker,
    SchemaRule,
    TypeChecker,
    ValidationReport,
)


SAMPLE_DATA = [
    {"name": "Alice", "age": 30, "email": "alice@example.com"},
    {"name": "Bob", "age": 25, "email": "bob@example.com"},
]


class TestValidationReport:
    def test_initially_valid(self):
        report = ValidationReport(total_rows=2)
        assert report.is_valid is True
        assert report.error_count == 0

    def test_add_error_marks_invalid(self):
        report = ValidationReport(total_rows=1)
        report.add_error(0, "field", "rule", "msg")
        assert report.is_valid is False
        assert report.error_count == 1

    def test_summary(self):
        report = ValidationReport(total_rows=5)
        report.add_error(0, "f", "r", "m")
        s = report.summary()
        assert s["total_rows"] == 5
        assert s["error_count"] == 1


class TestDataValidator:
    def test_valid_data(self):
        v = DataValidator(rules=[
            SchemaRule(field="name", type=str, required=True),
            SchemaRule(field="age", type=int, required=True),
        ])
        report = v.validate(SAMPLE_DATA)
        assert report.is_valid

    def test_required_field_missing(self):
        data = [{"name": "Alice"}]
        v = DataValidator(rules=[SchemaRule(field="age", required=True)])
        report = v.validate(data)
        assert not report.is_valid

    def test_type_mismatch(self):
        data = [{"age": "thirty"}]
        v = DataValidator(rules=[SchemaRule(field="age", type=int)])
        report = v.validate(data)
        assert not report.is_valid
        assert report.errors[0].rule == "type"

    def test_range_violation(self):
        data = [{"age": 5}]
        v = DataValidator(rules=[SchemaRule(field="age", type=int, min=18, max=99)])
        report = v.validate(data)
        assert not report.is_valid
        assert report.errors[0].rule == "min"

    def test_max_violation(self):
        data = [{"age": 200}]
        v = DataValidator(rules=[SchemaRule(field="age", type=int, min=0, max=150)])
        report = v.validate(data)
        assert not report.is_valid
        assert report.errors[0].rule == "max"

    def test_pattern_validation(self):
        data = [{"email": "not-an-email"}]
        v = DataValidator(rules=[SchemaRule(field="email", pattern=r"^[\w.+-]+@[\w-]+\.[\w.]+$")])
        report = v.validate(data)
        assert not report.is_valid
        assert report.errors[0].rule == "pattern"

    def test_pattern_valid(self):
        v = DataValidator(rules=[SchemaRule(field="email", pattern=r"^[\w.+-]+@[\w-]+\.[\w.]+$")])
        report = v.validate(SAMPLE_DATA)
        assert report.is_valid

    def test_validate_as_handler_pass(self):
        v = DataValidator(rules=[SchemaRule(field="name", type=str, required=True)])
        result = v.validate_as_handler(SAMPLE_DATA, {})
        assert result is SAMPLE_DATA

    def test_validate_as_handler_fail(self):
        v = DataValidator(rules=[SchemaRule(field="missing", required=True)])
        with pytest.raises(ValueError, match="Validation failed"):
            v.validate_as_handler(SAMPLE_DATA, {})

    def test_add_rule(self):
        v = DataValidator()
        v.add_rule(SchemaRule(field="x", required=True))
        assert len(v.rules) == 1


class TestIndividualCheckers:
    def test_null_checker_passes_when_not_required(self):
        report = ValidationReport(total_rows=1)
        NullChecker().check({"x": None}, SchemaRule(field="x", required=False), 0, report)
        assert report.is_valid

    def test_type_checker_skips_none(self):
        report = ValidationReport(total_rows=1)
        TypeChecker().check({"x": None}, SchemaRule(field="x", type=int), 0, report)
        assert report.is_valid

    def test_range_checker_skips_none(self):
        report = ValidationReport(total_rows=1)
        RangeChecker().check({"x": None}, SchemaRule(field="x", min=0), 0, report)
        assert report.is_valid
