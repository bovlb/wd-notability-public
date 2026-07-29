from __future__ import annotations

from wd_notability.content import outlinks as outlinks_module
from wd_notability.content.outlinks import extract_outlinks


def test_extract_outlinks_uses_claims_references_and_qualifiers(monkeypatch):
    monkeypatch.setattr(outlinks_module, "OUTLINKS_ENABLED", True)
    entity = {
        "claims": {
            "P1": [
                {
                    "mainsnak": {"datavalue": {"value": {"id": "Q10"}}},
                    "qualifiers": {
                        "P2": [
                            {"datavalue": {"value": {"id": "Q11"}}},
                            {"datavalue": {"value": {"id": "Q1"}}},
                        ]
                    },
                    "references": [
                        {
                            "snaks": {
                                "P3": [
                                    {"datavalue": {"value": {"id": "Q12"}}},
                                    {"datavalue": {"value": {"id": "Q10"}}},
                                ]
                            }
                        }
                    ],
                }
            ],
            "P4": [
                {
                    "mainsnak": {"datavalue": {"value": {"id": "Q13"}}},
                }
            ],
        }
    }

    assert extract_outlinks(entity, self_qid="Q1") == {"Q10", "Q11", "Q12", "Q13"}


def test_extract_outlinks_can_be_disabled(monkeypatch):
    monkeypatch.setattr(outlinks_module, "OUTLINKS_ENABLED", False)

    entity = {
        "claims": {
            "P1": [
                {
                    "mainsnak": {"datavalue": {"value": {"id": "Q10"}}},
                }
            ]
        }
    }

    assert extract_outlinks(entity, self_qid="Q1") == set()
