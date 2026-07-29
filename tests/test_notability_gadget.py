from __future__ import annotations

import argparse
from pathlib import Path

import httpx
import pytest

from wd_notability.notability_gadget import (
    NOTABILITY2_PROD_PAGE_TITLE,
    NOTABILITY2_STAGING_PAGE_TITLE,
    build_notability2_edit_summary,
    publish_notability2,
    resolve_notability2_page_title,
)


class _FakeSession:
    def __init__(self, responses: list[httpx.Response]) -> None:
        self._responses = responses
        self.calls: list[tuple[str, str, dict | None, dict | None]] = []

    async def request_with_timings(self, method: str, url: str, *, params=None, data=None):
        self.calls.append((method, url, params, data))
        response = self._responses.pop(0)
        return response, argparse.Namespace()


def _response(*, json_data: dict) -> httpx.Response:
    request = httpx.Request("GET", "https://www.wikidata.org/w/api.php")
    return httpx.Response(200, json=json_data, request=request)


def test_resolve_notability2_page_title():
    assert resolve_notability2_page_title("prod") == NOTABILITY2_PROD_PAGE_TITLE
    assert resolve_notability2_page_title("staging") == NOTABILITY2_STAGING_PAGE_TITLE


def test_build_notability2_edit_summary_uses_filename():
    summary = build_notability2_edit_summary(
        source_path=Path("/tmp/notability.js"),
        page_title=NOTABILITY2_STAGING_PAGE_TITLE,
    )

    assert summary == "Publish notability.js to User:Bovlb/notability2_staging.js"


@pytest.mark.asyncio
async def test_publish_notability2_uploads_to_staging(monkeypatch, tmp_path):
    source = tmp_path / "notability.js"
    source.write_text("console.log('hello');\n", encoding="utf-8")
    session = _FakeSession(
        responses=[
            _response(json_data={"query": {"tokens": {"csrftoken": "csrf-token"}}}),
            _response(json_data={"edit": {"result": "Success"}}),
        ],
    )

    page_title = await publish_notability2(
        source_path=source,
        target="staging",
        session=session,
    )

    assert page_title == NOTABILITY2_STAGING_PAGE_TITLE
    assert session.calls[0][0] == "GET"
    assert session.calls[0][2] == {
        "action": "query",
        "meta": "tokens",
        "type": "csrf",
        "format": "json",
    }
    assert session.calls[1][0] == "POST"
    assert session.calls[1][3]["title"] == NOTABILITY2_STAGING_PAGE_TITLE
    assert session.calls[1][3]["text"] == "console.log('hello');\n"
    assert session.calls[1][3]["contentmodel"] == "javascript"
    assert session.calls[1][3]["summary"] == "Publish notability.js to User:Bovlb/notability2_staging.js"


@pytest.mark.asyncio
async def test_publish_notability2_prompts_before_prod_upload(monkeypatch, tmp_path):
    source = tmp_path / "notability.js"
    source.write_text("console.log('prod');\n", encoding="utf-8")
    session = _FakeSession(
        responses=[
            _response(json_data={"query": {"tokens": {"csrftoken": "csrf-token"}}}),
            _response(json_data={"edit": {"result": "Success"}}),
        ],
    )
    monkeypatch.setattr("builtins.input", lambda _prompt: NOTABILITY2_PROD_PAGE_TITLE)

    page_title = await publish_notability2(
        source_path=source,
        target="prod",
        session=session,
    )

    assert page_title == NOTABILITY2_PROD_PAGE_TITLE


def test_notability_script_reads_configured_api_base():
    styles = Path("/home/gavin/github/wd-notability/server/static/notability.js").read_text()

    assert "resolveNotabilityApiBase" in styles
    assert "window.NOTABILITY_CONFIG" in styles
    assert "window.NOTABILITY_API_BASE" not in styles


def test_notability_script_injects_creation_summary_bar():
    styles = Path("/home/gavin/github/wd-notability/server/static/notability.js").read_text()

    assert "notability-creation-summary" in styles
    assert "height: 12px;" in styles
    assert "updateCreationSummary" in styles
    assert "rememberCreationQID" in styles
    assert "--creation-strong: #6f9f74;" in styles
    assert "--creation-empty: #ffffff;" in styles
    assert "prefers-color-scheme" not in styles
    assert "segment.title =" not in styles
    assert "summaryText" in styles
    assert "bar.title = summaryText;" in styles


def test_notability_meta_script_shows_bundle_in_toggle_label():
    styles = Path("/home/gavin/github/wd-notability/server/static/notability2_meta.js").read_text()

    assert "shortBundleLabel" in styles
    assert "toggle.textContent = `${shortApiLabel(apiBase)}/${shortBundleLabel(scriptTitle)}`;" in styles
