from __future__ import annotations

from collections.abc import Iterable

from wd_notability.models import QID


def _normalize_qid(value: object) -> QID | None:
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if not isinstance(value, str):
        return None
    value = value.strip().upper()
    return value if value.startswith("Q") and value[1:].isdigit() else None


def _snak_qid(snak: object) -> QID | None:
    if not isinstance(snak, dict):
        return None

    datavalue = snak.get("datavalue")
    if not isinstance(datavalue, dict):
        return None

    value = datavalue.get("value")
    if not isinstance(value, dict):
        return None

    return _normalize_qid(value.get("id"))


def _collect_qids(snaks: object) -> Iterable[QID]:
    if not isinstance(snaks, list):
        return []
    qids: list[QID] = []
    for snak in snaks:
        qid = _snak_qid(snak)
        if qid is not None:
            qids.append(qid)
    return qids


def extract_outlinks(entity: dict, *, self_qid: QID | None = None) -> set[QID]:
    outlinks: set[QID] = set()

    claims = entity.get("claims", {})
    if not isinstance(claims, dict):
        return outlinks

    for claim_list in claims.values():
        if not isinstance(claim_list, list):
            continue
        for claim in claim_list:
            if not isinstance(claim, dict):
                continue

            for qid in _collect_qids([claim.get("mainsnak")]):
                if qid != self_qid:
                    outlinks.add(qid)

            qualifiers = claim.get("qualifiers", {})
            if isinstance(qualifiers, dict):
                for snaks in qualifiers.values():
                    for qid in _collect_qids(snaks):
                        if qid != self_qid:
                            outlinks.add(qid)

            references = claim.get("references", [])
            if isinstance(references, list):
                for reference in references:
                    if not isinstance(reference, dict):
                        continue
                    snaks = reference.get("snaks", {})
                    if not isinstance(snaks, dict):
                        continue
                    for snak_list in snaks.values():
                        for qid in _collect_qids(snak_list):
                            if qid != self_qid:
                                outlinks.add(qid)

    return outlinks

