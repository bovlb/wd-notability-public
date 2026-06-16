from __future__ import annotations

import pytest

from wd_notability.models import Detector, NotabilityCriterion, NotabilityLevel, Source


class LevelDetector(Detector):
    def __init__(self, name: str, criterion: NotabilityCriterion, levels: dict[str, NotabilityLevel]):
        super().__init__(name, criterion)
        self.levels = levels

    async def detect(self, context: dict):
        level = self.levels.get(context["qid"], NotabilityLevel.NONE)
        yield self.make_signal(level=level, key=self.name)


def make_batch_source(name: str, detector: Detector, calls: list[list[str]]) -> Source:
    class BatchSource(Source):
        async def get_contexts(self, qids):
            qid_list = list(qids)
            calls.append(qid_list)
            return {
                qid: {
                    "qid": qid,
                    "_timings": {"get_context": 0.01},
                }
                for qid in qid_list
            }

    return BatchSource(name=name, detectors={detector})


@pytest.mark.asyncio
async def test_evaluate_many_batches_sources_and_honors_per_qid_stop_on_strong():
    from wd_notability import evaluate as evaluate_module

    calls_1: list[list[str]] = []
    calls_2: list[list[str]] = []
    levels_1 = {"Q1": NotabilityLevel.STRONG, "Q2": NotabilityLevel.WEAK}
    levels_2 = {"Q1": NotabilityLevel.WEAK, "Q2": NotabilityLevel.WEAK}

    source_1 = make_batch_source(
        "first",
        LevelDetector("first", NotabilityCriterion.N1, levels_1),
        calls_1,
    )
    source_2 = make_batch_source(
        "second",
        LevelDetector("second", NotabilityCriterion.N3_INLINKS, levels_2),
        calls_2,
    )

    results = await evaluate_module.evaluate_many(
        ["Q1", "Q2"],
        sources=[source_1, source_2],
        stop_on_strong=True,
        update_cache=False,
    )

    assert calls_1 == [["Q1", "Q2"]]
    assert calls_2 == [["Q2"]]
    assert results["Q1"].n == NotabilityLevel.STRONG
    assert results["Q2"].n3 == NotabilityLevel.WEAK


@pytest.mark.asyncio
async def test_evaluate_many_isolates_source_run_errors_per_qid():
    from wd_notability import evaluate as evaluate_module

    class FailingDetector(Detector):
        def __init__(self) -> None:
            super().__init__("failing", NotabilityCriterion.N1)

        async def detect(self, context: dict):
            yield self.make_signal(level=NotabilityLevel.WEAK, key="failing")

    class BatchSource(Source):
        async def get_contexts(self, qids):
            return {qid: {"qid": qid, "_timings": {"get_context": 0.0}} for qid in qids}

        async def _run_context_core(self, qid, context):
            if qid == "Q1":
                raise RuntimeError("boom")
            return await super()._run_context_core(qid, context)

    results = await evaluate_module.evaluate_many(
        ["Q1", "Q2"],
        sources=[BatchSource(name="batch", detectors={FailingDetector()})],
        stop_on_strong=False,
        update_cache=False,
    )

    assert results["Q1"].errors["N1"] == ["failing: boom"]
    assert results["Q2"].n1 == NotabilityLevel.WEAK
