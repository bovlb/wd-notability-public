from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from wd_notability.creations_status import creator_needs_attention, creator_status_bucket
from wd_notability.models import EvaluationResult, NotabilityLevel


def test_creator_status_bucket_recognizes_partial_n2_states():
    partial_strong = EvaluationResult(
        qid="Q1",
        n2a=NotabilityLevel.STRONG,
        n2b=NotabilityLevel.NONE,
    )
    partial_weak = EvaluationResult(
        qid="Q2",
        n2a=NotabilityLevel.NONE,
        n2b=NotabilityLevel.WEAK,
    )

    assert creator_status_bucket(partial_strong) == "partial_strong"
    assert creator_status_bucket(partial_weak) == "partial_weak"


def test_creator_status_bucket_keeps_strong_and_none_behaviour():
    strong = EvaluationResult(
        qid="Q3",
        n2a=NotabilityLevel.STRONG,
        n2b=NotabilityLevel.STRONG,
    )
    none = EvaluationResult(qid="Q4")

    assert creator_status_bucket(strong) == "strong"
    assert creator_status_bucket(none) == "none"


def test_partial_statuses_count_as_attention():
    assert creator_needs_attention("partial_strong") is True
    assert creator_needs_attention("partial_weak") is True
