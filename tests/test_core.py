"""Tests for jormungandr.core — pipeline engine."""

import pytest
from jormungandr.core import (
    DataBatch,
    Pipeline,
    PipelineBuilder,
    PipelineExecutor,
    PipelineStage,
    StageResult,
    StageStatus,
)


# -- helpers ----------------------------------------------------------------

def passthrough(data, config):
    return data


def doubler(data, config):
    return [x * 2 for x in data]


def failing_handler(data, config):
    raise RuntimeError("boom")


def adder(data, config):
    amount = config.get("amount", 1)
    return [x + amount for x in data]


# -- StageResult tests ------------------------------------------------------

class TestStageResult:
    def test_success_properties(self):
        r = StageResult(status=StageStatus.SUCCESS, data=[1], stage_name="s1")
        assert r.is_success is True
        assert r.is_failure is False

    def test_failure_properties(self):
        r = StageResult(status=StageStatus.FAILURE, errors=["err"], stage_name="s1")
        assert r.is_success is False
        assert r.is_failure is True

    def test_partial_counts_as_success(self):
        r = StageResult(status=StageStatus.PARTIAL, stage_name="s1")
        assert r.is_success is True

    def test_repr(self):
        r = StageResult(status=StageStatus.SUCCESS, stage_name="load")
        assert "load" in repr(r)


# -- PipelineStage tests ---------------------------------------------------

class TestPipelineStage:
    def test_execute_success(self):
        stage = PipelineStage(name="double", handler=doubler)
        result = stage.execute([1, 2, 3])
        assert result.status == StageStatus.SUCCESS
        assert result.data == [2, 4, 6]
        assert result.duration_ms > 0

    def test_execute_failure(self):
        stage = PipelineStage(name="bad", handler=failing_handler)
        result = stage.execute([])
        assert result.status == StageStatus.FAILURE
        assert len(result.errors) == 1
        assert "boom" in result.errors[0]

    def test_retry_on_failure(self):
        call_count = {"n": 0}

        def flaky(data, config):
            call_count["n"] += 1
            if call_count["n"] < 3:
                raise RuntimeError("flaky")
            return data

        stage = PipelineStage(name="flaky", handler=flaky, retry_count=2)
        result = stage.execute([1])
        assert result.status == StageStatus.SUCCESS
        assert call_count["n"] == 3

    def test_handler_receives_config(self):
        stage = PipelineStage(name="add", handler=adder, config={"amount": 10})
        result = stage.execute([1, 2])
        assert result.data == [11, 12]


# -- Pipeline tests ---------------------------------------------------------

class TestPipeline:
    def test_add_and_get_stage(self):
        p = Pipeline("test")
        p.add_stage(PipelineStage(name="a", handler=passthrough))
        assert p.stage_count == 1
        assert p.get_stage("a") is not None
        assert p.get_stage("missing") is None

    def test_remove_stage(self):
        p = Pipeline("test")
        p.add_stage(PipelineStage(name="a", handler=passthrough))
        assert p.remove_stage("a") is True
        assert p.stage_count == 0
        assert p.remove_stage("nope") is False

    def test_repr(self):
        p = Pipeline("demo", [PipelineStage(name="x", handler=passthrough)])
        assert "demo" in repr(p)


# -- PipelineExecutor tests ------------------------------------------------

class TestPipelineExecutor:
    def test_sequential_execution(self):
        p = Pipeline("seq", [
            PipelineStage(name="double", handler=doubler),
            PipelineStage(name="pass", handler=passthrough),
        ])
        ex = PipelineExecutor(p)
        results = ex.run([1, 2])
        assert len(results) == 2
        assert results[0].data == [2, 4]
        assert results[1].data == [2, 4]
        assert ex.all_succeeded

    def test_stop_on_failure(self):
        p = Pipeline("fail", [
            PipelineStage(name="bad", handler=failing_handler),
            PipelineStage(name="never", handler=passthrough),
        ])
        ex = PipelineExecutor(p, stop_on_failure=True)
        results = ex.run([])
        assert len(results) == 1
        assert results[0].is_failure
        assert not ex.all_succeeded

    def test_skip_on_error(self):
        p = Pipeline("skip", [
            PipelineStage(name="bad", handler=failing_handler),
            PipelineStage(name="skippable", handler=passthrough, skip_on_error=True),
        ])
        ex = PipelineExecutor(p, stop_on_failure=False)
        results = ex.run([])
        assert len(results) == 2
        assert results[1].status == StageStatus.SKIPPED

    def test_summary(self):
        p = Pipeline("sum", [PipelineStage(name="p", handler=passthrough)])
        ex = PipelineExecutor(p)
        ex.run("data")
        s = ex.summary()
        assert s["stages_succeeded"] == 1
        assert s["stages_failed"] == 0

    def test_hooks_fire(self):
        events = []
        p = Pipeline("hooks", [PipelineStage(name="p", handler=passthrough)])
        p.register_hook("before_stage", lambda **kw: events.append("before"))
        p.register_hook("after_stage", lambda **kw: events.append("after"))
        p.register_hook("on_complete", lambda **kw: events.append("complete"))
        ex = PipelineExecutor(p)
        ex.run("x")
        assert events == ["before", "after", "complete"]


# -- DataBatch tests --------------------------------------------------------

class TestDataBatch:
    def test_basic_properties(self):
        batch = DataBatch(records=[{"a": 1}, {"a": 2}], source="test")
        assert batch.size == 2
        assert batch.is_empty() is False

    def test_empty_batch(self):
        batch = DataBatch(records=[])
        assert batch.is_empty() is True

    def test_split(self):
        batch = DataBatch(records=[{"i": i} for i in range(5)])
        chunks = batch.split(2)
        assert len(chunks) == 3
        assert chunks[0].size == 2
        assert chunks[2].size == 1

    def test_split_invalid(self):
        batch = DataBatch(records=[{"i": 1}])
        with pytest.raises(ValueError):
            batch.split(0)

    def test_merge(self):
        a = DataBatch(records=[{"x": 1}], source="a")
        b = DataBatch(records=[{"x": 2}], source="b")
        merged = a.merge(b)
        assert merged.size == 2

    def test_clone(self):
        batch = DataBatch(records=[{"x": 1}])
        cloned = batch.clone()
        cloned.records[0]["x"] = 99
        assert batch.records[0]["x"] == 1  # original unchanged


# -- PipelineBuilder tests --------------------------------------------------

class TestPipelineBuilder:
    def test_fluent_build(self):
        pipeline = (
            PipelineBuilder("builder-test")
            .add("s1", passthrough)
            .add("s2", doubler, config={"k": "v"})
            .build()
        )
        assert pipeline.name == "builder-test"
        assert pipeline.stage_count == 2

    def test_build_empty_raises(self):
        with pytest.raises(ValueError):
            PipelineBuilder("empty").build()

    def test_builder_with_hooks(self):
        called = []
        pipeline = (
            PipelineBuilder("hook-test")
            .add("s1", passthrough)
            .with_hook("on_complete", lambda **kw: called.append(True))
            .build()
        )
        PipelineExecutor(pipeline).run("data")
        assert called == [True]
