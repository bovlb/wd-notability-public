from __future__ import annotations

from wd_notability.models import EvaluationResult, NotabilityCriterion, NotabilityLevel, Source

DIRECT_CRITERIA = (
    NotabilityCriterion.N1,
    NotabilityCriterion.N2a,
    NotabilityCriterion.N2b,
    NotabilityCriterion.N3_INLINKS,
    NotabilityCriterion.N3_OSM,
    NotabilityCriterion.N3_WIKISUB,
    NotabilityCriterion.N3_SDC,
)


def source_criteria(sources: list[Source]) -> set[NotabilityCriterion]:
    return {criterion for source in sources for criterion in source.criteria}


def with_unfinished_criteria_unknown(
    result: EvaluationResult,
    remaining_sources: list[Source],
) -> EvaluationResult:
    remaining_criteria = source_criteria(remaining_sources)
    for criterion in DIRECT_CRITERIA:
        if criterion in remaining_criteria and result.levels[criterion] != NotabilityLevel.STRONG:
            result.set(criterion, NotabilityLevel.UNKNOWN)
    return result
