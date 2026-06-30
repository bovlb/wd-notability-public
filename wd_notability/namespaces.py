from enum import Enum
from typing import Any

from wd_notability.lookup_cache import lookup_cache


class Namespace(Enum):
    SPECIAL = "special"
    MEDIA = "media"
    MAIN = "main"
    TALK = "talk"  # all positive odd IDs
    USER = "user"
    PROJECT = "project"
    FILE = "file"
    MEDIAWIKI = "mediawiki"
    TEMPLATE = "template"
    HELP = "help"
    CATEGORY = "category"
    PORTAL = "portal"
    COMMENTS = "comments"
    CITATIONS = "citations"
    DRAFT = "draft"
    PROPERTY = "property"
    LEXEME = "lexeme"
    ENTITY_SCHEMA = "entity_schema"
    MODULE = "module"
    TRANSLATIONS = "translations"
    EVENT = "event"
    TOPIC = "topic"
    INDEX = "index"
    PAGE = "page"
    OTHER = "other"


ID_TO_NAMESPACE = {
    -2: Namespace.MEDIA,
    -1: Namespace.SPECIAL,
    0: Namespace.MAIN,
    2: Namespace.USER,
    4: Namespace.PROJECT,
    6: Namespace.FILE,
    8: Namespace.MEDIAWIKI,
    10: Namespace.TEMPLATE,
    12: Namespace.HELP,
    14: Namespace.CATEGORY,
    100: Namespace.PORTAL,
    103: Namespace.COMMENTS,
    104: Namespace.INDEX,
    106: Namespace.PAGE,
    114: Namespace.CITATIONS,
    118: Namespace.DRAFT,
    120: Namespace.PROPERTY,
    146: Namespace.LEXEME,
    640: Namespace.ENTITY_SCHEMA,
    828: Namespace.MODULE,
    1198: Namespace.TRANSLATIONS,
    1728: Namespace.EVENT,
    2600: Namespace.TOPIC,
}


def extract_namespace(title: str, prefix_to_id: dict[str, int] | None) -> Namespace:
    if ":" not in title:
        return Namespace.MAIN

    if not prefix_to_id:
        return Namespace.OTHER

    prefix = title.split(":", 1)[0].lower()
    ns_id = prefix_to_id.get(prefix)
    if ns_id is None:
        return Namespace.MAIN # Could be a non-namespace colon

    if ns_id > 0 and ns_id % 2 == 1:
        return Namespace.TALK

    return ID_TO_NAMESPACE.get(ns_id, Namespace.OTHER)


class NamespaceExtractor:
    def get_prefix_to_id(self, site_key: str) -> dict[str, int] | None:
        return lookup_cache.get_prefix_to_id(site_key)

    async def extract(self, sitelink: dict[str, Any]) -> Namespace:
        site_key = sitelink.get("site")
        title = sitelink.get("title")
        if not isinstance(site_key, str) or not isinstance(title, str):
            raise ValueError("Sitelink must contain string 'site' and 'title' fields")

        prefix_to_id = self.get_prefix_to_id(site_key)
        return extract_namespace(title, prefix_to_id)


def load_site_api_urls() -> dict[str, str]:
    return lookup_cache.get_site_api_urls()


namespace_extractor = NamespaceExtractor()
