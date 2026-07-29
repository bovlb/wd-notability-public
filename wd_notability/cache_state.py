from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence


CONTENT_POLICY_UPDATED_AT_LOOKUP_STATE_KEY = "content_policy_updated_at"
LOOKUP_STATE_TABLE_NAME = "lookup_state"


async def ensure_schema(db) -> None:
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS lookup_state (
            `key` VARCHAR(255) NOT NULL PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )


async def get_lookup_state(cache, key: str) -> str | None:
    await cache.initialize()

    async with cache._connect() as db:
        cursor = await db.execute(
            "SELECT value FROM lookup_state WHERE `key` = %s",
            (key,),
        )
        row = await cursor.fetchone()
    if row is None:
        return None
    value = row[0]
    return None if value is None else str(value)


async def set_lookup_state(cache, key: str, value: str) -> None:
    await cache.initialize()

    async with cache._write_guard():
        async with cache._connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            await db.execute(
                """
                INSERT INTO lookup_state (`key`, value)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE value = VALUES(value)
                """,
                (key, value),
            )
            await db.commit()


async def get_content_policy_updated_at(cache) -> datetime | None:
    value = await get_lookup_state(cache, CONTENT_POLICY_UPDATED_AT_LOOKUP_STATE_KEY)
    if value is None:
        return None
    try:
        text = value.strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


async def set_content_policy_updated_at(cache, value: object | None = None) -> datetime:
    if value is None:
        timestamp = datetime.now(UTC)
    elif isinstance(value, str) and value.strip().lower() == "now":
        timestamp = datetime.now(UTC)
    else:
        if isinstance(value, datetime):
            timestamp = value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
        elif isinstance(value, (int, float)):
            timestamp = datetime.fromtimestamp(value, tz=UTC)
        elif isinstance(value, str):
            text = value.strip()
            if text.endswith("Z"):
                text = f"{text[:-1]}+00:00"
            timestamp = datetime.fromisoformat(text)
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=UTC)
            timestamp = timestamp.astimezone(UTC)
        else:
            raise ValueError(
                "content policy timestamp must be a UTC datetime, epoch seconds, or ISO-8601 value"
            )

    await set_lookup_state(
        cache,
        CONTENT_POLICY_UPDATED_AT_LOOKUP_STATE_KEY,
        timestamp.isoformat(timespec="microseconds").replace("+00:00", "Z"),
    )
    return timestamp


async def clear_lookup_state(cache, keys: Sequence[str] | None = None) -> int:
    await cache.initialize()

    async with cache._write_guard():
        async with cache._connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            if keys is None:
                cursor = await db.execute("DELETE FROM lookup_state")
            else:
                keys = tuple(dict.fromkeys(key for key in keys if key))
                if not keys:
                    await db.commit()
                    return 0
                placeholders = ", ".join("?" for _ in keys)
                cursor = await db.execute(
                    f"DELETE FROM lookup_state WHERE `key` IN ({placeholders})",
                    tuple(keys),
                )
            deleted = max(0, int(cursor.rowcount))
            await db.commit()
    return deleted
