from __future__ import annotations

from pathlib import Path

from wd_notability.wikidata_api import WIKIDATA_API_URL, WikidataSession

NOTABILITY2_PROD_PAGE_TITLE = "User:Bovlb/notability2.js"
NOTABILITY2_STAGING_PAGE_TITLE = "User:Bovlb/notability2_staging.js"


def resolve_notability2_page_title(target: str) -> str:
    if target == "prod":
        return NOTABILITY2_PROD_PAGE_TITLE
    if target == "staging":
        return NOTABILITY2_STAGING_PAGE_TITLE
    raise ValueError("target must be prod or staging")


def build_notability2_edit_summary(*, source_path: Path, page_title: str) -> str:
    return f"Publish {source_path.name} to {page_title}"


def _confirm_prod_publish(page_title: str) -> None:
    prompt = (
        f"Publish to {page_title}? "
        f"Type {page_title!r} to continue: "
    )
    response = input(prompt).strip()
    if response != page_title:
        raise SystemExit("Aborted")


async def publish_notability2(
    *,
    source_path: Path,
    target: str,
    session: WikidataSession | None = None,
    summary: str | None = None,
) -> str:
    page_title = resolve_notability2_page_title(target)
    if target == "prod":
        _confirm_prod_publish(page_title)

    source_text = source_path.read_text(encoding="utf-8")
    edit_summary = summary or build_notability2_edit_summary(
        source_path=source_path,
        page_title=page_title,
    )
    edit_session = session or WikidataSession()

    token_response, _ = await edit_session.request_with_timings(
        "GET",
        WIKIDATA_API_URL,
        params={
            "action": "query",
            "meta": "tokens",
            "type": "csrf",
            "format": "json",
        },
    )
    token_response.raise_for_status()
    token_payload = token_response.json()
    csrf_token = token_payload.get("query", {}).get("tokens", {}).get("csrftoken")
    if not isinstance(csrf_token, str) or not csrf_token:
        raise RuntimeError(f"Failed to fetch CSRF token: {token_payload}")

    edit_response, _ = await edit_session.request_with_timings(
        "POST",
        WIKIDATA_API_URL,
        data={
            "action": "edit",
            "title": page_title,
            "text": source_text,
            "summary": edit_summary,
            "token": csrf_token,
            "format": "json",
            "contentmodel": "javascript",
            "bot": "1",
        },
    )
    edit_response.raise_for_status()
    edit_payload = edit_response.json()
    edit_result = edit_payload.get("edit")
    if not isinstance(edit_result, dict) or edit_result.get("result") != "Success":
        raise RuntimeError(f"Publish failed: {edit_payload}")

    return page_title
