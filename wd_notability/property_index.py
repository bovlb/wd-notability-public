from __future__ import annotations

from pathlib import Path

from wd_notability.lookup_cache import lookup_cache


class PropertyInstanceIndex:
    def __init__(self, property_index_path: Path | None = None):
        self.property_index_path = property_index_path

    async def property_instances(self, qid: str) -> set[str]:
        return await lookup_cache.property_instances(qid)

    async def property_instances_for(self, qids: list[str] | set[str] | tuple[str, ...]) -> dict[str, set[str]]:
        return await lookup_cache.property_instances_for(qids)


property_index = PropertyInstanceIndex()
