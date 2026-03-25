"""
Built-in data transforms for Jormungandr pipelines.

Each transform operates on ``list[dict]`` and returns a new ``list[dict]``.
Transforms are designed to be used as pipeline stage handlers — they accept
``(data, config)`` and return the transformed data.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Sequence


# ---------------------------------------------------------------------------
# FilterTransform
# ---------------------------------------------------------------------------

class FilterTransform:
    """Retain only records that satisfy a predicate.

    Config keys:
        field (str): The field to test.
        operator (str): One of ``eq``, ``ne``, ``gt``, ``lt``, ``gte``,
            ``lte``, ``in``, ``not_in``, ``contains``.
        value: The comparison value.
    """

    OPERATORS = {
        "eq": lambda a, b: a == b,
        "ne": lambda a, b: a != b,
        "gt": lambda a, b: a > b,
        "lt": lambda a, b: a < b,
        "gte": lambda a, b: a >= b,
        "lte": lambda a, b: a <= b,
        "in": lambda a, b: a in b,
        "not_in": lambda a, b: a not in b,
        "contains": lambda a, b: b in a,
    }

    def __call__(self, data: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        field = config["field"]
        op_name = config.get("operator", "eq")
        value = config["value"]

        op_fn = self.OPERATORS.get(op_name)
        if op_fn is None:
            raise ValueError(f"Unknown filter operator: {op_name!r}")

        return [row for row in data if field in row and op_fn(row[field], value)]


# ---------------------------------------------------------------------------
# MapTransform
# ---------------------------------------------------------------------------

class MapTransform:
    """Apply a mapping function to every record.

    Config keys:
        function (callable): A function that receives a dict and returns a dict.
        fields (dict, optional): A mapping of ``{new_field: expression}`` where
            *expression* is a callable ``(record) -> value``.
    """

    def __call__(self, data: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        fn: Optional[Callable] = config.get("function")
        field_fns: Dict[str, Callable] = config.get("fields", {})

        result: List[Dict[str, Any]] = []
        for row in data:
            new_row = dict(row)
            if fn is not None:
                new_row = fn(new_row)
            for target, expr in field_fns.items():
                new_row[target] = expr(row)
            result.append(new_row)
        return result


# ---------------------------------------------------------------------------
# AggregateTransform
# ---------------------------------------------------------------------------

class AggregateTransform:
    """Group records by key(s) and compute aggregate values.

    Config keys:
        group_by (str | list[str]): Field(s) to group on.
        aggregations (dict): ``{output_field: {"func": callable, "field": str}}``
            where *func* receives a list of values and returns a scalar.
    """

    def __call__(self, data: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        group_by = config["group_by"]
        if isinstance(group_by, str):
            group_by = [group_by]
        aggregations: Dict[str, Dict[str, Any]] = config.get("aggregations", {})

        groups: Dict[tuple, List[Dict[str, Any]]] = {}
        for row in data:
            key = tuple(row.get(g) for g in group_by)
            groups.setdefault(key, []).append(row)

        results: List[Dict[str, Any]] = []
        for key, rows in groups.items():
            out: Dict[str, Any] = {}
            for gb_field, gb_val in zip(group_by, key):
                out[gb_field] = gb_val
            for agg_name, agg_spec in aggregations.items():
                func = agg_spec["func"]
                src = agg_spec["field"]
                values = [r[src] for r in rows if src in r]
                out[agg_name] = func(values)
            results.append(out)
        return results


# ---------------------------------------------------------------------------
# JoinTransform
# ---------------------------------------------------------------------------

class JoinTransform:
    """Join the incoming data with a second dataset on a shared key.

    Config keys:
        right (list[dict]): The right-hand dataset.
        on (str): The join key present in both datasets.
        how (str): ``inner`` (default) or ``left``.
    """

    def __call__(self, data: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        right: List[Dict[str, Any]] = config["right"]
        on_key: str = config["on"]
        how: str = config.get("how", "inner")

        right_index: Dict[Any, Dict[str, Any]] = {}
        for row in right:
            if on_key in row:
                right_index[row[on_key]] = row

        results: List[Dict[str, Any]] = []
        for left_row in data:
            key_val = left_row.get(on_key)
            if key_val in right_index:
                merged = {**left_row, **right_index[key_val]}
                results.append(merged)
            elif how == "left":
                results.append(dict(left_row))
        return results


# ---------------------------------------------------------------------------
# SortTransform
# ---------------------------------------------------------------------------

class SortTransform:
    """Sort records by one or more fields.

    Config keys:
        fields (str | list[str]): Field(s) to sort on.
        reverse (bool): If ``True``, sort in descending order. Default ``False``.
    """

    def __call__(self, data: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        fields = config["fields"]
        if isinstance(fields, str):
            fields = [fields]
        reverse = config.get("reverse", False)

        def sort_key(row: Dict[str, Any]) -> tuple:
            return tuple(row.get(f) for f in fields)

        return sorted(data, key=sort_key, reverse=reverse)


# ---------------------------------------------------------------------------
# DeduplicateTransform
# ---------------------------------------------------------------------------

class DeduplicateTransform:
    """Remove duplicate records based on specified field(s).

    Config keys:
        fields (str | list[str]): The field(s) that define uniqueness.
        keep (str): ``first`` (default) or ``last`` — which duplicate to keep.
    """

    def __call__(self, data: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        fields = config["fields"]
        if isinstance(fields, str):
            fields = [fields]
        keep = config.get("keep", "first")

        if keep == "last":
            data = list(reversed(data))

        seen: set = set()
        results: List[Dict[str, Any]] = []
        for row in data:
            key = tuple(row.get(f) for f in fields)
            if key not in seen:
                seen.add(key)
                results.append(row)

        if keep == "last":
            results.reverse()
        return results
