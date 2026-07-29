from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from wd_notability.creations import CREATIONS, _normalize_text

if TYPE_CHECKING:
    from collections.abc import Sequence


def _to_utc_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, int):
        return datetime.fromtimestamp(value, tz=UTC)
    if isinstance(value, float):
        return datetime.fromtimestamp(value, tz=UTC)
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.isdigit():
        try:
            return datetime.fromtimestamp(int(text), tz=UTC)
        except (OverflowError, ValueError):
            return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _to_optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if isinstance(value, datetime):
        dt = value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return int(dt.timestamp())
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_epoch_seconds(value: object) -> int | None:
    dt = _to_utc_datetime(value)
    if dt is None:
        return None
    return int(dt.timestamp())


@dataclass(slots=True, frozen=True)
class UserHistoryRecord:
    username: str
    window_start: str
    window_end: str
    requested_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    last_refresh_at: datetime | None = None
    error_text: str | None = None
    row_count: int | None = None


async def ensure_schema(db) -> None:
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS user_history (
            username VARCHAR(255) NOT NULL PRIMARY KEY,
            window_start TEXT NOT NULL,
            window_end TEXT NOT NULL,
            requested_at DATETIME(6) NULL,
            started_at DATETIME(6) NULL,
            finished_at DATETIME(6) NULL,
            last_refresh_at DATETIME(6) NULL,
            error_text TEXT NULL,
            row_count BIGINT UNSIGNED NULL
        )
        """
    )


async def clear_user_history(cache) -> int:
    await cache.initialize()

    started = time.perf_counter()
    async with cache._write_guard():
        async with cache._connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            cursor = await db.execute("DELETE FROM user_history")
            updated = max(0, int(cursor.rowcount))
            await db.commit()
    cache._warn_slow_write("clear_user_history", started, row_count=updated)
    return updated


async def get_user_history(cache, username: str) -> UserHistoryRecord | None:
    await cache.initialize()

    username_text = _normalize_text(username)
    if username_text is None:
        return None

    async with cache._connect() as db:
        cursor = await db.execute(
            """
            SELECT username, window_start, window_end, requested_at, started_at, finished_at, last_refresh_at, error_text, row_count
            FROM user_history
            WHERE username = %s
            """,
            (username_text,),
        )
        row = await cursor.fetchone()

    if row is None:
        return None

    username_value = _normalize_text(row[0]) or username_text
    window_start = _normalize_text(row[1])
    window_end = _normalize_text(row[2])
    if window_start is None or window_end is None:
        return None

    return UserHistoryRecord(
        username=username_value,
        window_start=window_start,
        window_end=window_end,
        requested_at=_to_utc_datetime(row[3]),
        started_at=_to_utc_datetime(row[4]),
        finished_at=_to_utc_datetime(row[5]),
        last_refresh_at=_to_utc_datetime(row[6]),
        error_text=_normalize_text(row[7]),
        row_count=_to_optional_int(row[8]),
    )


async def upsert_user_history(
    cache,
    *,
    username: str,
    window_start: str,
    window_end: str,
    requested_at: datetime | int | float | str | None = None,
    started_at: datetime | int | float | str | None = None,
    finished_at: datetime | int | float | str | None = None,
    last_refresh_at: datetime | int | float | str | None = None,
    error_text: str | None = None,
    row_count: int | None = None,
) -> None:
    await cache.initialize()

    username_text = _normalize_text(username)
    window_start_text = _normalize_text(window_start)
    window_end_text = _normalize_text(window_end)
    error_text_value = _normalize_text(error_text)
    requested_at_value = _to_utc_datetime(requested_at)
    started_at_value = _to_utc_datetime(started_at)
    finished_at_value = _to_utc_datetime(finished_at)
    last_refresh_at_value = _to_utc_datetime(last_refresh_at)
    if username_text is None:
        raise ValueError("username must not be empty")
    if window_start_text is None:
        raise ValueError("window_start must not be empty")
    if window_end_text is None:
        raise ValueError("window_end must not be empty")

    started = time.perf_counter()
    async with cache._write_guard():
        async with cache._connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            await db.execute(
                """
                INSERT INTO user_history (
                    username,
                    window_start,
                    window_end,
                    requested_at,
                    started_at,
                    finished_at,
                    last_refresh_at,
                    error_text,
                    row_count
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    window_start = VALUES(window_start),
                    window_end = VALUES(window_end),
                    requested_at = VALUES(requested_at),
                    started_at = VALUES(started_at),
                    finished_at = VALUES(finished_at),
                    last_refresh_at = VALUES(last_refresh_at),
                    error_text = VALUES(error_text),
                    row_count = VALUES(row_count)
                """,
                (
                    username_text,
                    window_start_text,
                    window_end_text,
                    requested_at_value,
                    started_at_value,
                    finished_at_value,
                    last_refresh_at_value,
                    error_text_value,
                    row_count,
                ),
            )
            await db.commit()
    cache._warn_slow_write("upsert_user_history", started, row_count=1)


async def delete_user_history(cache, username: str) -> int:
    await cache.initialize()

    username_text = _normalize_text(username)
    if username_text is None:
        return 0

    started = time.perf_counter()
    async with cache._write_guard():
        async with cache._connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            cursor = await db.execute(
                "DELETE FROM user_history WHERE username = %s",
                (username_text,),
            )
            updated = max(0, int(cursor.rowcount))
            await db.commit()
    cache._warn_slow_write("delete_user_history", started, row_count=updated)
    return updated


async def request_user_history(
    cache,
    *,
    username: str,
    window_start: str | None = None,
    window_end: str | None = None,
    force: bool = False,
) -> tuple[UserHistoryRecord, bool]:
    await cache.initialize()

    username_text = _normalize_text(username)
    if username_text is None:
        raise ValueError("username must not be empty")

    if isinstance(window_start, str) and not window_start.strip():
        window_start = None
    if isinstance(window_end, str) and not window_end.strip():
        window_end = None
    if window_start is None or window_end is None:
        default_start, default_end = CREATIONS.default_window()
        window_start = default_start if window_start is None else window_start
        window_end = default_end if window_end is None else window_end

    current = await get_user_history(cache, username_text)
    current_start_epoch = _to_epoch_seconds(current.window_start) if current is not None else None
    current_end_epoch = _to_epoch_seconds(current.window_end) if current is not None else None
    requested_start_epoch = _to_epoch_seconds(window_start)
    requested_end_epoch = _to_epoch_seconds(window_end)

    if (
        not force
        and current is not None
        and current.finished_at is not None
        and current_start_epoch is not None
        and current_end_epoch is not None
        and requested_start_epoch is not None
        and requested_end_epoch is not None
        and current_start_epoch <= requested_start_epoch
        and current_end_epoch >= requested_end_epoch
    ):
        return current, False

    if current is not None:
        if current_start_epoch is not None and requested_start_epoch is not None:
            window_start = window_start if requested_start_epoch < current_start_epoch else current.window_start
        elif current.window_start:
            window_start = current.window_start

        if current_end_epoch is not None and requested_end_epoch is not None:
            window_end = window_end if requested_end_epoch > current_end_epoch else current.window_end
        elif current.window_end:
            window_end = current.window_end

    await upsert_user_history(
        cache,
        username=username_text,
        window_start=window_start,
        window_end=window_end,
        requested_at=datetime.now(UTC),
        started_at=None,
        finished_at=None,
        last_refresh_at=current.last_refresh_at if current is not None else None,
        error_text=None,
        row_count=current.row_count if current is not None else None,
    )

    refreshed = await get_user_history(cache, username_text)
    if refreshed is None:
        raise RuntimeError("Failed to materialize user history request")
    return refreshed, True


async def count_user_history_requests(cache) -> int:
    await cache.initialize()

    async with cache._connect() as db:
        cursor = await db.execute(
            """
            SELECT COUNT(*)
            FROM user_history
            WHERE requested_at IS NOT NULL
              AND (finished_at IS NULL OR requested_at > finished_at)
            """
        )
        row = await cursor.fetchone()

    return int(row[0]) if row and row[0] is not None else 0


async def list_user_history_requests(cache, limit: int | None = None) -> list[UserHistoryRecord]:
    await cache.initialize()

    sql = """
        SELECT username, window_start, window_end, requested_at, started_at, finished_at, last_refresh_at, error_text, row_count
        FROM user_history
        WHERE requested_at IS NOT NULL
          AND (finished_at IS NULL OR requested_at > finished_at)
        ORDER BY requested_at ASC, username ASC
    """
    params: tuple[Any, ...] = ()
    if limit is not None:
        sql += " LIMIT %s"
        params = (int(limit),)

    async with cache._connect() as db:
        cursor = await db.execute(sql, params)
        rows = await cursor.fetchall()

    result: list[UserHistoryRecord] = []
    for row in rows:
        username = _normalize_text(row[0])
        window_start = _normalize_text(row[1])
        window_end = _normalize_text(row[2])
        if username is None or window_start is None or window_end is None:
            continue
        result.append(
            UserHistoryRecord(
                username=username,
                window_start=window_start,
                window_end=window_end,
                requested_at=_to_utc_datetime(row[3]),
                started_at=_to_utc_datetime(row[4]),
                finished_at=_to_utc_datetime(row[5]),
                last_refresh_at=_to_utc_datetime(row[6]),
                error_text=_normalize_text(row[7]),
                row_count=_to_optional_int(row[8]),
            )
        )
    return result
