"""Tests for jormungandr.transforms — built-in data transforms."""

import pytest
from jormungandr.transforms import (
    AggregateTransform,
    DeduplicateTransform,
    FilterTransform,
    JoinTransform,
    MapTransform,
    SortTransform,
)


SAMPLE_DATA = [
    {"name": "Alice", "age": 30, "dept": "eng"},
    {"name": "Bob", "age": 25, "dept": "eng"},
    {"name": "Carol", "age": 35, "dept": "sales"},
    {"name": "Dave", "age": 28, "dept": "sales"},
]


class TestFilterTransform:
    def test_eq(self):
        result = FilterTransform()(SAMPLE_DATA, {"field": "dept", "operator": "eq", "value": "eng"})
        assert len(result) == 2
        assert all(r["dept"] == "eng" for r in result)

    def test_gt(self):
        result = FilterTransform()(SAMPLE_DATA, {"field": "age", "operator": "gt", "value": 28})
        assert len(result) == 2

    def test_in(self):
        result = FilterTransform()(SAMPLE_DATA, {"field": "name", "operator": "in", "value": ["Alice", "Carol"]})
        assert len(result) == 2

    def test_contains(self):
        result = FilterTransform()(SAMPLE_DATA, {"field": "name", "operator": "contains", "value": "li"})
        assert len(result) == 1
        assert result[0]["name"] == "Alice"

    def test_unknown_operator(self):
        with pytest.raises(ValueError, match="Unknown filter operator"):
            FilterTransform()(SAMPLE_DATA, {"field": "age", "operator": "xor", "value": 1})

    def test_default_operator_is_eq(self):
        result = FilterTransform()(SAMPLE_DATA, {"field": "age", "value": 30})
        assert len(result) == 1


class TestMapTransform:
    def test_function(self):
        fn = lambda row: {**row, "upper_name": row["name"].upper()}
        result = MapTransform()(SAMPLE_DATA, {"function": fn})
        assert result[0]["upper_name"] == "ALICE"

    def test_fields(self):
        result = MapTransform()(SAMPLE_DATA, {"fields": {"age_plus_10": lambda r: r["age"] + 10}})
        assert result[0]["age_plus_10"] == 40


class TestAggregateTransform:
    def test_group_and_sum(self):
        result = AggregateTransform()(SAMPLE_DATA, {
            "group_by": "dept",
            "aggregations": {
                "total_age": {"func": sum, "field": "age"},
                "count": {"func": len, "field": "name"},
            },
        })
        by_dept = {r["dept"]: r for r in result}
        assert by_dept["eng"]["total_age"] == 55
        assert by_dept["eng"]["count"] == 2

    def test_group_by_list(self):
        data = [{"a": 1, "b": 2, "v": 10}, {"a": 1, "b": 2, "v": 20}]
        result = AggregateTransform()(data, {
            "group_by": ["a", "b"],
            "aggregations": {"total": {"func": sum, "field": "v"}},
        })
        assert len(result) == 1
        assert result[0]["total"] == 30


class TestJoinTransform:
    def test_inner_join(self):
        right = [{"dept": "eng", "budget": 1000}, {"dept": "hr", "budget": 500}]
        result = JoinTransform()(SAMPLE_DATA, {"right": right, "on": "dept", "how": "inner"})
        assert all(r.get("budget") == 1000 for r in result if r["dept"] == "eng")
        assert not any(r["dept"] == "hr" for r in result)

    def test_left_join(self):
        right = [{"dept": "eng", "budget": 1000}]
        result = JoinTransform()(SAMPLE_DATA, {"right": right, "on": "dept", "how": "left"})
        assert len(result) == 4
        sales = [r for r in result if r["dept"] == "sales"]
        assert "budget" not in sales[0]


class TestSortTransform:
    def test_sort_ascending(self):
        result = SortTransform()(SAMPLE_DATA, {"fields": "age"})
        ages = [r["age"] for r in result]
        assert ages == sorted(ages)

    def test_sort_descending(self):
        result = SortTransform()(SAMPLE_DATA, {"fields": "age", "reverse": True})
        ages = [r["age"] for r in result]
        assert ages == sorted(ages, reverse=True)


class TestDeduplicateTransform:
    def test_dedup_keep_first(self):
        data = [{"id": 1, "v": "a"}, {"id": 1, "v": "b"}, {"id": 2, "v": "c"}]
        result = DeduplicateTransform()(data, {"fields": "id"})
        assert len(result) == 2
        assert result[0]["v"] == "a"

    def test_dedup_keep_last(self):
        data = [{"id": 1, "v": "a"}, {"id": 1, "v": "b"}, {"id": 2, "v": "c"}]
        result = DeduplicateTransform()(data, {"fields": "id", "keep": "last"})
        assert len(result) == 2
        assert result[0]["v"] == "b"
