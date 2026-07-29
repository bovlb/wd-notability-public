from __future__ import annotations

from server.render_helpers import _render_report_badge


def _example_report(
    qid: str,
    *,
    levels: dict[str, str],
    content_last_revid: int | None = 1,
    has_claims_count: int = 1,
    has_sitelinks_count: int = 1,
    inlinks_count: int = 1,
    is_redirect: bool = False,
    is_deleted: bool = False,
) -> dict[str, object]:
    return {
        "qid": qid,
        "levels": levels,
        "content_last_revid": content_last_revid,
        "has_claims_count": has_claims_count,
        "has_sitelinks_count": has_sitelinks_count,
        "inlinks_count": inlinks_count,
        "is_redirect": is_redirect,
        "is_deleted": is_deleted,
    }


BADGE_EXAMPLES: tuple[dict[str, object], ...] = (
    {
        "id": "strong",
        "title": "Strong overall",
        "description": "All of N1, N2a, N2b, and N3 are strong.",
        "report": _example_report(
            "Q42",
            levels={
                "N": "strong",
                "N1": "strong",
                "N2a": "strong",
                "N2b": "strong",
                "N3": "strong",
                "N3_inlinks": "strong",
                "N3_osm": "none",
                "N3_wikisub": "none",
                "N3_sdc": "none",
            },
            has_claims_count=3,
            has_sitelinks_count=2,
            inlinks_count=5,
        ),
    },
    {
        "id": "n3-partial-weak",
        "title": "N3 partial weak",
        "description": "N3 is partial-weak, while N1 and N2 are none, so the outer ring is also partial-weak.",
        "report": _example_report(
            "Q50",
            levels={
                "N": "partial-weak",
                "N1": "none",
                "N2a": "none",
                "N2b": "none",
                "N3": "partial-weak",
                "N3_inlinks": "partial-weak",
                "N3_osm": "none",
                "N3_wikisub": "none",
                "N3_sdc": "none",
            },
            has_claims_count=1,
            has_sitelinks_count=0,
            inlinks_count=1,
        ),
    },
    {
        "id": "n3-partial-strong",
        "title": "N3 partial strong",
        "description": "N3 is partial-strong, while N1 and N2 are none, so the outer ring is also partial-strong.",
        "report": _example_report(
            "Q51",
            levels={
                "N": "partial-strong",
                "N1": "none",
                "N2a": "none",
                "N2b": "none",
                "N3": "partial-strong",
                "N3_inlinks": "partial-strong",
                "N3_osm": "none",
                "N3_wikisub": "none",
                "N3_sdc": "none",
            },
            has_claims_count=1,
            has_sitelinks_count=0,
            inlinks_count=3,
        ),
    },
    {
        "id": "partial-weak",
        "title": "Partial weak",
        "description": "N2a is weak, while N1, N2b, and N3 are none, so the overall result is partial-weak",
        "report": _example_report(
            "Q43",
            levels={
                "N": "partial-weak",
                "N1": "none",
                "N2a": "weak",
                "N2b": "none",
                "N3": "none",
                "N3_inlinks": "none",
                "N3_osm": "none",
                "N3_wikisub": "none",
                "N3_sdc": "none",
            },
            has_claims_count=1,
            has_sitelinks_count=0,
            inlinks_count=0,
        ),
    },
    {
        "id": "partial-strong",
        "title": "Partial strong",
        "description": "N2b is strong, while N1, N2a, and N3 are none, so the overall result is partial-strong.",
        "report": _example_report(
            "Q44",
            levels={
                "N": "partial-strong",
                "N1": "none",
                "N2a": "none",
                "N2b": "strong",
                "N3": "none",
                "N3_inlinks": "none",
                "N3_osm": "none",
                "N3_wikisub": "none",
                "N3_sdc": "none",
            },
            has_claims_count=1,
            has_sitelinks_count=0,
            inlinks_count=0,
        ),
    },
    {
        "id": "weak",
        "title": "Weak overall",
        "description": "The item has weak sitelinks (N1), so the overall state is weak.",
        "report": _example_report(
            "Q45",
            levels={
                "N": "weak",
                "N1": "weak",
                "N2a": "none",
                "N2b": "none",
                "N3": "none",
                "N3_inlinks": "none",
                "N3_osm": "none",
                "N3_wikisub": "none",
                "N3_sdc": "none",
            },
            has_claims_count=1,
            has_sitelinks_count=0,
            inlinks_count=0,
        ),
    },
    {
        "id": "unknown",
        "title": "Unknown",
        "description": "Nothing has been evaluated yet.",
        "report": _example_report(
            "Q46",
            levels={
                "N": "unknown",
                "N1": "unknown",
                "N2a": "unknown",
                "N2b": "unknown",
                "N3": "unknown",
                "N3_inlinks": "unknown",
                "N3_osm": "unknown",
                "N3_wikisub": "unknown",
                "N3_sdc": "unknown",
            },
            content_last_revid=None,
            has_claims_count=0,
            has_sitelinks_count=0,
            inlinks_count=0,
        ),
    },
    {
        "id": "n3-unknown",
        "title": "N3 Unknown",
        "description": "N1 and N2 are weak, but N3 has not been evaluated yet, so the overall result is unknown.",
        "report": _example_report(
            "Q46",
            levels={
                "N": "unknown",
                "N1": "weak",
                "N2a": "weak",
                "N2b": "weak",
                "N3": "unknown",
                "N3_inlinks": "unknown",
                "N3_osm": "none",
                "N3_wikisub": "none",
                "N3_sdc": "none",
            },
            content_last_revid=None,
            has_claims_count=0,
            has_sitelinks_count=0,
            inlinks_count=0,
        ),
    },
    {
        "id": "empty",
        "title": "Empty",
        "description": "When there are no claims, N2a and N2b are none, but are shown as empty.",
        "report": _example_report(
            "Q47",
            levels={
                "N": "none",
                "N1": "none",
                "N2a": "none",
                "N2b": "none",
                "N3": "none",
                "N3_inlinks": "none",
                "N3_osm": "none",
                "N3_wikisub": "none",
                "N3_sdc": "none",
            },
            has_claims_count=0,
            has_sitelinks_count=0,
            inlinks_count=0,
        ),
    },
    {
        "id": "redirect",
        "title": "Redirect",
        "description": "Redirects have additional purple arrows; N1 and N2 are from redirect target; N3 is from the original item.",
        "report": _example_report(
            "Q48",
            levels={
                "N": "strong",
                "N1": "weak",
                "N2a": "none",
                "N2b": "strong",
                "N3": "strong",
                "N3_inlinks": "strong",
                "N3_osm": "none",
                "N3_wikisub": "none",
                "N3_sdc": "none",
            },
            has_claims_count=2,
            has_sitelinks_count=1,
            inlinks_count=4,
            is_redirect=True,
        ),
    },
    {
        "id": "deleted",
        "title": "Deleted",
        "description": "Deleted items are replaced by a red X.",
        "report": _example_report(
            "Q49",
            levels={
                "N": "strong",
                "N1": "strong",
                "N2a": "strong",
                "N2b": "strong",
                "N3": "strong",
                "N3_inlinks": "strong",
                "N3_osm": "none",
                "N3_wikisub": "none",
                "N3_sdc": "none",
            },
            has_claims_count=1,
            has_sitelinks_count=1,
            inlinks_count=1,
            is_deleted=True,
        ),
    },
)


def list_badge_examples() -> list[dict[str, object]]:
    return [
        {
            "id": example["id"],
            "title": example["title"],
            "description": example["description"],
            "svg_url": f"/api/badge-examples/{example['id']}.svg",
        }
        for example in BADGE_EXAMPLES
    ]


def get_badge_example(example_id: str) -> dict[str, object]:
    normalized_id = example_id.strip().lower()
    for example in BADGE_EXAMPLES:
        if example["id"] == normalized_id:
            return example
    raise KeyError(example_id)


def render_badge_example_svg(example_id: str) -> str:
    example = get_badge_example(example_id)
    rendered = _render_report_badge(
        example["report"], example["report"]["qid"], example["id"])
    svg_end = rendered.find("</svg>")
    if svg_end == -1:
        return rendered
    return rendered[: svg_end + len("</svg>")]
