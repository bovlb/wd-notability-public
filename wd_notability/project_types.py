from enum import Enum

class ProjectType(Enum):
    WIKIPEDIA = "wikipedia"
    WIKIVOYAGE = "wikivoyage"
    WIKISOURCE = "wikisource"
    WIKIQUOTE = "wikiquote"
    WIKINEWS = "wikinews"
    WIKIBOOKS = "wikibooks"
    WIKIDATA = "wikidata"
    WIKISPECIES = "wikispecies"
    WIKIVERSITY = "wikiversity"
    WIKTIONARY = "wiktionary"
    COMMONS = "commons"
    META = "meta"
    MEDIAWIKI = "mediawiki"
    WIKIMANIA = "wikimania"
    OTHER = "other"

SITE_SUFFIX_TO_PROJECT = {
    "wiki": ProjectType.WIKIPEDIA,
    "wikivoyage": ProjectType.WIKIVOYAGE,
    "wikisource": ProjectType.WIKISOURCE,
    "wikiquote": ProjectType.WIKIQUOTE,
    "wikinews": ProjectType.WIKINEWS,
    "wikibooks": ProjectType.WIKIBOOKS,
    "wikidata": ProjectType.WIKIDATA,
    "wikispecies": ProjectType.WIKISPECIES,
    "wikiversity": ProjectType.WIKIVERSITY,
    "wiktionary": ProjectType.WIKTIONARY,
    "commons": ProjectType.COMMONS,
    "meta": ProjectType.META,
    "mediawiki": ProjectType.MEDIAWIKI,
    "wikimania": ProjectType.WIKIMANIA,
}


def detect_project_type(site_key: str) -> ProjectType:
    site_key = site_key.lower()
    for special in ["commons", "meta", "mediawiki", "wikidata", "wikispecies", "wikimania"]:
        if site_key == f"{special}wiki":
            return SITE_SUFFIX_TO_PROJECT[special]
    for suffix, project in SITE_SUFFIX_TO_PROJECT.items():
        if site_key.endswith(suffix):
            return project
    return ProjectType.OTHER


class ProjectTypeExtractor:
    def __init__(self):
        self._cache = {}
    def extract(self, site_key: str) -> ProjectType:
        if site_key in self._cache:
            return self._cache[site_key]
        project_type = detect_project_type(site_key)
        self._cache[site_key] = project_type
        return project_type


# Singleton instance for shared use
project_type_extractor = ProjectTypeExtractor()
