from __future__ import annotations

from pathlib import Path

from wd_notability.toolforge_defaults import toolforge_cache_root

LOCALDB_ROOT = Path(toolforge_cache_root())
LOOKUP_CACHE_PATH = LOCALDB_ROOT / "lookup_cache.db"
EVALUATION_CACHE_PATH = LOCALDB_ROOT / "evaluation_cache.sqlite3"
