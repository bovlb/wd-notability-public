import pytest

from wd_notability import summary as summary_bits
from wd_notability.models import (
    Detector,
    EvaluationReason,
    EvaluationResult,
    NotabilityCriterion,
    NotabilityLevel,
    Source,
    SignalResult,
)


class ExampleDetector(Detector):
    def __init__(self) -> None:
        super().__init__("example", NotabilityCriterion.N1)

    async def detect(self, entity: dict):
        yield SignalResult(
            criterion=NotabilityCriterion.N1,
            level=NotabilityLevel.WEAK,
            key="example",
            properties={"entity": entity["id"]},
        )


def test_notability_ordering_values():
    assert NotabilityLevel.NONE.value == 0
    assert NotabilityLevel.WEAK.value == 1
    assert NotabilityLevel.UNKNOWN.value == 2
    assert NotabilityLevel.STRONG.value == 3
    assert str(NotabilityLevel.NONE) == "none"
    assert str(NotabilityLevel.UNKNOWN) == "unknown"


def test_evaluation_reason_values_are_not_priorities():
    assert EvaluationReason.USE.value < EvaluationReason.INLINK.value
    assert EvaluationReason.USE.priority > EvaluationReason.INLINK.priority


@pytest.mark.asyncio
async def test_detector_run_streams_signals():
    wrapped = ExampleDetector()
    signals = [signal async for signal in wrapped.run({"id": "Q42"})]

    assert signals == [
        SignalResult(
            detector="example",
            criterion=NotabilityCriterion.N1,
            level=NotabilityLevel.WEAK,
            key="example",
            properties={"entity": "Q42"},
        )
    ]


def test_detector_make_signal_uses_name_and_criterion():
    class SourcesDetector(Detector):
        def __init__(self) -> None:
            super().__init__("sources", NotabilityCriterion.N2b)

        async def detect(self, entity: dict):
            if False:
                yield entity

    wrapped = SourcesDetector()
    signal = wrapped.make_signal(
        level=NotabilityLevel.STRONG,
        key="source_example",
        properties={"origin": "test"},
    )

    assert signal.detector == "sources"
    assert signal.criterion == NotabilityCriterion.N2b
    assert signal.level == NotabilityLevel.STRONG
    assert signal.key == "source_example"
    assert signal.properties == {"origin": "test"}


@pytest.mark.asyncio
async def test_source_run_records_phase_timings():
    class TimingDetector(Detector):
        def __init__(self) -> None:
            super().__init__("timing", NotabilityCriterion.N1)

        async def detect(self, entity: dict):
            yield self.make_signal(level=NotabilityLevel.WEAK, key="timing")

    class TimingSource(Source):
        async def get_context(self, qid: str) -> dict:
            return {
                "qid": qid,
                "_timings": {
                    "get_context_query": 0.25,
                    "get_context_limiter_wait": 0.5,
                    "get_context_retry_wait": 0.75,
                },
            }

        async def update_result(self, result: EvaluationResult, context: dict) -> None:
            result.has_claims = True

        async def extra(self, qid: str, context: dict, result: EvaluationResult) -> None:
            result.is_redirect = True

    source = TimingSource(name="timing_source", detectors={TimingDetector()})
    result = await source.run("Q42")

    assert set(result.source_timings) == {
        "get_context",
        "get_context_query",
        "get_context_limiter_wait",
        "get_context_retry_wait",
        "update_result",
        "detector_timing",
        "detectors",
        "extra",
    }
    assert all(result.source_timings[name] >= 0 for name in result.source_timings)
    assert result.source_timings["get_context_query"] == 0.25
    assert result.source_timings["get_context_limiter_wait"] == 0.5
    assert result.source_timings["get_context_retry_wait"] == 0.75
    assert result.n1 == NotabilityLevel.WEAK
    assert result.has_claims is True
    assert result.is_redirect is True


def test_evaluation_result_set_updates_derived_levels():
    result = EvaluationResult(qid="Q42")

    result.set(NotabilityCriterion.N1, NotabilityLevel.WEAK)
    result.set(NotabilityCriterion.N2a, NotabilityLevel.STRONG)
    result.set(NotabilityCriterion.N2b, NotabilityLevel.WEAK)
    result.set(NotabilityCriterion.N3_INLINKS, NotabilityLevel.NONE)
    result.set(NotabilityCriterion.N3_OSM, NotabilityLevel.NONE)
    result.set(NotabilityCriterion.N3_WIKISUB, NotabilityLevel.NONE)
    result.set(NotabilityCriterion.N3_SDC, NotabilityLevel.NONE)

    assert result.levels[NotabilityCriterion.N1] == NotabilityLevel.WEAK
    assert result.levels[NotabilityCriterion.N2a] == NotabilityLevel.STRONG
    assert result.levels[NotabilityCriterion.N2b] == NotabilityLevel.WEAK
    assert result.levels[NotabilityCriterion.N3_INLINKS] == NotabilityLevel.NONE
    assert result.levels[NotabilityCriterion.N3_OSM] == NotabilityLevel.NONE
    assert result.levels[NotabilityCriterion.N3_WIKISUB] == NotabilityLevel.NONE
    assert result.levels[NotabilityCriterion.N3_SDC] == NotabilityLevel.NONE
    assert result.levels[NotabilityCriterion.N3] == NotabilityLevel.NONE
    assert result.levels[NotabilityCriterion.N2] == NotabilityLevel.WEAK
    assert result.levels[NotabilityCriterion.N12] == NotabilityLevel.WEAK
    assert result.levels[NotabilityCriterion.N] == NotabilityLevel.WEAK
    assert result.levels_str["N"] == "weak"


def test_evaluation_result_errors_defaults_and_append():
    result = EvaluationResult(qid="Q42")
    detector = ExampleDetector()

    assert result.errors == {
        "N1": [],
        "N2a": [],
        "N2b": [],
        "N3_inlinks": [],
        "N3_osm": [],
        "N3_wikisub": [],
        "N3_sdc": [],
    }

    result.add_error(detector, RuntimeError("detector failed"))
    assert result.errors["N1"] == ["example: detector failed"]


def test_evaluation_result_errors_set_unknown_level():
    result = EvaluationResult(qid="Q42")
    detector = ExampleDetector()

    result.add_error(detector, RuntimeError("failed"))

    assert result.n1 == NotabilityLevel.UNKNOWN
    assert result.n12 == NotabilityLevel.UNKNOWN
    assert result.n == NotabilityLevel.UNKNOWN
    assert result.errors["N1"] == ["example: failed"]


def test_summary_unknown_level_round_trip():
    result = EvaluationResult(qid="Q42")
    detector = ExampleDetector()
    result.add_error(detector, RuntimeError("boom"))
    result.is_deleted = True

    packed = result.summary

    assert summary_bits.get_level(packed, NotabilityCriterion.N1) == NotabilityLevel.UNKNOWN
    unpacked = EvaluationResult.from_summary("Q42", packed)

    assert unpacked.is_deleted is True
    assert unpacked.n1 == NotabilityLevel.UNKNOWN
    assert unpacked.n12 == NotabilityLevel.UNKNOWN
    assert unpacked.n == NotabilityLevel.UNKNOWN


def test_summary_round_trip_uses_only_detected_criteria():
    result = EvaluationResult(qid="Q42")
    result.set(NotabilityCriterion.N1, NotabilityLevel.WEAK)
    result.set(NotabilityCriterion.N2a, NotabilityLevel.STRONG)
    result.set(NotabilityCriterion.N2b, NotabilityLevel.WEAK)
    result.set(NotabilityCriterion.N3_INLINKS, NotabilityLevel.NONE)
    result.set(NotabilityCriterion.N3_OSM, NotabilityLevel.WEAK)
    result.set(NotabilityCriterion.N3_WIKISUB, NotabilityLevel.NONE)
    result.set(NotabilityCriterion.N3_SDC, NotabilityLevel.NONE)
    result.is_deleted = True

    packed = result.summary
    unpacked = EvaluationResult.from_summary("Q42", packed)

    assert unpacked.is_deleted is True
    assert unpacked.n1 == NotabilityLevel.WEAK
    assert unpacked.n2a == NotabilityLevel.STRONG
    assert unpacked.n2b == NotabilityLevel.WEAK
    assert unpacked.n3_inlinks == NotabilityLevel.NONE
    assert unpacked.n3_osm == NotabilityLevel.WEAK
    assert unpacked.n3_wikisub == NotabilityLevel.NONE
    assert unpacked.n3_sdc == NotabilityLevel.NONE
    assert unpacked.n3 == NotabilityLevel.WEAK
    assert unpacked.n2 == NotabilityLevel.WEAK
    assert unpacked.n12 == NotabilityLevel.WEAK
    assert unpacked.n == NotabilityLevel.WEAK
