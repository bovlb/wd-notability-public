from __future__ import annotations

from wd_notability.models import EvaluationResult, NotabilityLevel


def creator_status_bucket(result: EvaluationResult) -> str:
    if result.is_deleted:
        return "deleted"
    if result.is_redirect:
        return "redirect"
    if result.n == NotabilityLevel.PARTIAL_STRONG:
        return "partial_strong"
    if result.n == NotabilityLevel.PARTIAL_WEAK:
        return "partial_weak"
    if result.n == NotabilityLevel.STRONG:
        return "strong"
    if result.n == NotabilityLevel.WEAK:
        return "weak"
    if result.n == NotabilityLevel.UNKNOWN:
        return "unknown"
    return "none"


def creator_needs_attention(bucket: str) -> bool:
    return bucket in {"weak", "unknown", "none", "partial_weak", "partial_strong", "redirect", "deleted"}
