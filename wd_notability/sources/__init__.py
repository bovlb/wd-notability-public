from wd_notability.models import Source
from wd_notability.sources.entity_data import ENTITY_DATA_SOURCE, EntityDataSource
from wd_notability.sources.inlinks import INLINKS_SOURCE
from wd_notability.sources.osm import OSM_SOURCE
from wd_notability.sources.sdc import SDC_SOURCE
from wd_notability.sources.wiki_usage import WIKI_USAGE_SOURCE


SOURCES: list[Source] = [
    ENTITY_DATA_SOURCE,
    SDC_SOURCE,
    INLINKS_SOURCE,
    WIKI_USAGE_SOURCE,
    OSM_SOURCE,
]
SOURCES_BY_NAME: dict[str, Source] = {source.name: source for source in SOURCES}


def get_source(name: str) -> Source:
    try:
        return SOURCES_BY_NAME[name]
    except KeyError as exc:
        raise ValueError(f"Unknown source: {name}") from exc


__all__ = [
    "ENTITY_DATA_SOURCE",
    "EntityDataSource",
    "INLINKS_SOURCE",
    "OSM_SOURCE",
    "SDC_SOURCE",
    "SOURCES",
    "SOURCES_BY_NAME",
    "WIKI_USAGE_SOURCE",
    "get_source",
]
