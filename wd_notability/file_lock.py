from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path


def cache_lock_path(target: str | Path, cache_name: str | None = None) -> Path:
    path = Path(target)
    suffix = f".{cache_name}" if cache_name else ""
    return path.with_name(f"{path.name}{suffix}.lock")


@contextmanager
def acquire_file_lock(target: str | Path, cache_name: str | None = None):
    lock_path = cache_lock_path(target, cache_name=cache_name)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        if os.name == "posix":
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        else:  # pragma: no cover
            raise RuntimeError("File locking is only supported on POSIX platforms")
        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, f"pid={os.getpid()}\n".encode("utf-8"))
        yield lock_path
    finally:
        try:
            if os.name == "posix":
                import fcntl

                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
