from __future__ import annotations

import json
import time
from pathlib import Path

BACKOFF_STATE_PATH = Path(__file__).resolve().parent / "data" / "wikidata_action_api_backoff.json"


def _read_state() -> dict[str, object]:
    try:
        payload = json.loads(BACKOFF_STATE_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def get_retry_after_at() -> float:
    state = _read_state()
    value = state.get("retry_after_at")
    if isinstance(value, (int, float)):
        return max(0.0, float(value))
    return 0.0


def get_retry_after_remaining() -> float:
    remaining = get_retry_after_at() - time.time()
    return remaining if remaining > 0 else 0.0


def set_retry_after_seconds(delay_seconds: float, *, reason: str = "429") -> float:
    delay = max(0.0, float(delay_seconds))
    until = time.time() + delay
    BACKOFF_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = BACKOFF_STATE_PATH.with_suffix(".tmp")
    tmp_path.write_text(
        json.dumps(
            {
                "retry_after_at": until,
                "updated_at": time.time(),
                "reason": reason,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    tmp_path.replace(BACKOFF_STATE_PATH)
    return until
