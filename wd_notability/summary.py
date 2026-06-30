"""Bit-packing helpers for persisted evaluation summaries.

Each detected criterion uses a 3-bit one-hot field:
- 000 = unknown
- 001 = none
- 010 = weak
- 100 = strong

The cache-facing API is mask/value oriented:
- ``mask(criterion)`` returns the bit mask for the criterion field
- ``value(criterion, level)`` returns the encoded field value
- ``get(summary, criterion)`` decodes the stored level
- ``set(summary, criterion, level)`` updates the field in place
"""

from __future__ import annotations

from typing import Final


REDIRECT: Final[int] = 1 << 0
HAS_SITELINKS: Final[int] = 1 << 1
HAS_CLAIMS: Final[int] = 1 << 2
DELETED: Final[int] = 1 << 3

_CRITERION_ORDER: Final[tuple[str, ...]] = (
    "N1",
    "N2a",
    "N2b",
    "N3_inlinks",
    "N3_osm",
    "N3_wikisub",
    "N3_sdc",
)

_CRITERION_SHIFTS: Final[dict[str, int]] = {
    criterion: 29 - 3 * index
    for index, criterion in enumerate(_CRITERION_ORDER)
}

_LEVEL_TO_BITS: Final[dict[int, int]] = {
    2: 0b000,  # unknown
    0: 0b001,  # none
    1: 0b010,  # weak
    3: 0b100,  # strong
}

_BITS_TO_LEVEL: Final[dict[int, int]] = {
    0b000: 2,
    0b001: 0,
    0b010: 1,
    0b100: 3,
}


def criterion_key(criterion: object) -> str:
    value = getattr(criterion, "value", criterion)
    if not isinstance(value, str):
        raise ValueError(f"Unknown criterion: {criterion}")
    return value


def criterion_shift(criterion: object) -> int:
    key = criterion_key(criterion)
    try:
        return _CRITERION_SHIFTS[key]
    except KeyError as exc:
        raise ValueError(f"Unknown criterion: {criterion}") from exc


def criterion_mask(criterion: object) -> int:
    return 0b111 << criterion_shift(criterion)


def criterion_value(criterion: object, level: object) -> int:
    return encode_level(level) << criterion_shift(criterion)


def encode_level(level: object) -> int:
    try:
        return _LEVEL_TO_BITS[int(level)]
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"Unknown level: {level}") from exc


def decode_level(bits: int) -> int:
    return _BITS_TO_LEVEL.get(bits & 0b111, 2)


def get_level(summary: int, criterion: object) -> int:
    shift = criterion_shift(criterion)
    bits = (summary >> shift) & 0b111
    return decode_level(bits)


def set_level(summary: int, criterion: object, level: object) -> int:
    return clear_level(summary, criterion) | criterion_value(criterion, level)


def clear_level(summary: int, criterion: object) -> int:
    return summary & ~criterion_mask(criterion)


def mask(criterion: object) -> int:
    return criterion_mask(criterion)


def value(criterion: object, level: object) -> int:
    return criterion_value(criterion, level)


def get(summary: int, criterion: object) -> int:
    return get_level(summary, criterion)


def clear(summary: int, criterion: object) -> int:
    return clear_level(summary, criterion)


def set(summary: int, criterion: object, level: object) -> int:
    return set_level(summary, criterion, level)


def direct_criteria() -> tuple[str, ...]:
    return _CRITERION_ORDER
