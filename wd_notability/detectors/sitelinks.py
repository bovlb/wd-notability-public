from collections.abc import AsyncGenerator
from typing import Optional

from wd_notability.models import Detector, NotabilityCriterion, NotabilityLevel, SignalResult
from wd_notability.namespaces import Namespace, namespace_extractor
from wd_notability.project_types import ProjectType, project_type_extractor


# N1 policy mapping notes:
# - Valid projects: Wikipedia-family + Commons + selected sister projects.
# - Invalid targets: talk/mediawiki/special/file/translations/user/draft/topic namespaces.
# - Additional exclusions: Portal subpages, TemplateStyles suffixes (.css/.js),
#   module /doc pages, and project-specific namespace restrictions.
# - Special handling: undetermined projects are weak; Commons categories are weak;
#   template subpages are weak unless explicitly excluded.
class SitelinksDetector(Detector):
    # Detector policy constants (good candidates for future override/subclassing).
    VALID_PROJECTS = {
        ProjectType.WIKIPEDIA,
        ProjectType.WIKIVOYAGE,
        ProjectType.WIKISOURCE,
        ProjectType.WIKIQUOTE,
        ProjectType.WIKINEWS,
        ProjectType.WIKIBOOKS,
        ProjectType.WIKIDATA,
        ProjectType.WIKISPECIES,
        ProjectType.WIKIVERSITY,
        ProjectType.COMMONS,
    }

    INVALID_NAMESPACES = {
        Namespace.TALK,
        Namespace.MEDIAWIKI,
        Namespace.SPECIAL,
        Namespace.FILE,
        Namespace.TRANSLATIONS,
        Namespace.USER,
        Namespace.DRAFT,
        Namespace.TOPIC,
    }

    UNDETERMINED_PROJECTS = {
        ProjectType.META,
        ProjectType.MEDIAWIKI,
        ProjectType.WIKIMANIA,
    }

    TEMPLATE_BAD_SUBPAGES = {"doc", "XML", "meta", "sandbox", "testcases", "TemplateData"}

    WIKIDATA_BAD_NAMESPACES = {
        Namespace.MAIN,  # item
        Namespace.PROPERTY,
        Namespace.LEXEME,
        Namespace.ENTITY_SCHEMA,
    }

    WIKTIONARY_BAD_NAMESPACES = {
        Namespace.COMMENTS,
        Namespace.MAIN,  # Main namespace excluded because interlanguage links are automatic.
    }

    SITELINK_LIKE_PROPERTIES = (
        "P373",  # Commons category
    )

    def __init__(self) -> None:
        super().__init__("sitelinks", NotabilityCriterion.N1)

    def get_project_type(self, sitelink: dict) -> ProjectType:
        site_key = sitelink["site"]
        return project_type_extractor.extract(site_key)

    async def get_namespace(self, sitelink: dict) -> Namespace:
        return await namespace_extractor.extract(sitelink)

    def get_subpage(self, sitelink: dict) -> Optional[str]:
        title = sitelink["title"]
        if "/" in title:
            return title.split("/")[-1]
        return None

    def get_suffix(self, sitelink: dict) -> Optional[str]:
        title = sitelink["title"]
        if "." in title:
            return title.split(".")[-1]
        return None

    def entity_get_claims_by_property(self, entity: dict, prop: str) -> list:
        return entity.get("claims", {}).get(prop, [])

    async def test_sitelink(self, sitelink: dict) -> SignalResult:
        site = sitelink["site"]
        title = sitelink["title"]
        url = sitelink.get("url")
        properties = {"site": site, "title": title}
        if isinstance(url, str) and url:
            properties["url"] = url
        project_type = self.get_project_type(sitelink)
        namespace = await self.get_namespace(sitelink)
        subpage = self.get_subpage(sitelink)
        suffix = self.get_suffix(sitelink)

        def make_result(level: NotabilityLevel, key: str, **kwargs) -> SignalResult:
            return self.make_signal(
                level=level,
                key=f"sitelinks_{key}",
                properties={**properties, **kwargs},
            )

        # Policy: "The status of pages in Meta-Wiki, MediaWiki.org, Wikimania
        # and other supported special Wikimedia sites is undetermined."
        if project_type in self.UNDETERMINED_PROJECTS:
            return make_result(NotabilityLevel.WEAK, "undetermined_project")

        # Policy: must be one of the valid projects (Wikipedia-family + Commons + selected sisters).
        if project_type not in self.VALID_PROJECTS:
            return make_result(NotabilityLevel.NONE, "invalid_project")

        # Policy: invalid namespaces include talk, MediaWiki, special, file,
        # translations, user, draft, and topic-like discussion namespaces.
        if namespace in self.INVALID_NAMESPACES:
            return make_result(NotabilityLevel.NONE, "invalid_namespace", namespace=namespace.value)

        # Policy: subpages of Portal namespace are not valid sitelinks.
        if namespace == Namespace.PORTAL and subpage is not None:
            return make_result(NotabilityLevel.NONE, "invalid_portal_subpage")

        # Policy: pages intended for TemplateStyles (e.g. .css/.js) are not valid.
        if suffix in {"css", "js"}:
            return make_result(NotabilityLevel.NONE, "invalid_suffix", suffix=suffix)

        # Policy: template handling has special rules for allowed/forbidden subpages.
        if namespace == Namespace.TEMPLATE:
            if subpage is not None:
                if subpage in self.TEMPLATE_BAD_SUBPAGES:
                    return make_result(NotabilityLevel.NONE, "invalid_template_subpage", subpage=subpage)
                # Policy nuance: template subpages are weaker evidence than plain templates.
                return make_result(NotabilityLevel.WEAK, "template_subpage", subpage=subpage)
            return make_result(NotabilityLevel.STRONG, "template")

        # Policy: module /doc subpages are not valid.
        if namespace == Namespace.MODULE and subpage == "doc":
            return make_result(NotabilityLevel.NONE, "invalid_module_doc")

        # Policy: Commons-only category items are generally restricted; currently treated as weak.
        if namespace == Namespace.CATEGORY and project_type == ProjectType.COMMONS:
            return make_result(NotabilityLevel.WEAK, "commons_category")

        # Policy: on Wikisource, Index/Page namespaces are invalid,
        # and mainspace subpages are treated as weaker/undetermined evidence.
        if project_type == ProjectType.WIKISOURCE:
            if namespace in {Namespace.INDEX, Namespace.PAGE}:
                return make_result(NotabilityLevel.NONE, "invalid_wikisource_namespace", namespace=namespace.value)
            if namespace == Namespace.MAIN and subpage is not None:
                return make_result(NotabilityLevel.WEAK, "wikisource_main_subpage", subpage=subpage)

        # Policy: Wikinews comments namespace pages are not valid.
        if project_type == ProjectType.WIKINEWS and namespace == Namespace.COMMENTS:
            return make_result(NotabilityLevel.NONE, "invalid_wikinews_namespace", namespace=namespace.value)

        # Policy: structured-data namespaces on Wikidata are not valid sitelink targets.
        if project_type == ProjectType.WIKIDATA and namespace in self.WIKIDATA_BAD_NAMESPACES:
            return make_result(NotabilityLevel.NONE, "invalid_wikidata_namespace", namespace=namespace.value)

        # Policy: Wiktionary citation/main namespaces are excluded.
        if project_type == ProjectType.WIKTIONARY and namespace in self.WIKTIONARY_BAD_NAMESPACES:
            return make_result(NotabilityLevel.NONE, "invalid_wiktionary_namespace", namespace=namespace.value)

        # Otherwise this is treated as a valid sitelink.
        return make_result(NotabilityLevel.STRONG, "valid_sitelink")

    async def detect(self, entity: dict) -> AsyncGenerator[SignalResult, None]:
        sitelinks = entity.get("sitelinks", {})
        for sitelink in sitelinks.values():
            yield await self.test_sitelink(sitelink)

        # Policy-adjacent heuristic: some claim properties act like sitelink evidence.
        for prop in self.SITELINK_LIKE_PROPERTIES:
            if self.entity_get_claims_by_property(entity, prop):
                yield self.make_signal(
                    level=NotabilityLevel.WEAK,
                    key="sitelink_like_property",
                    properties={"property": prop, "values": [claim.get("mainsnak", {}).get("datavalue", {}).get("value") for claim in self.entity_get_claims_by_property(entity, prop)]},
                )


SITELINKS_DETECTOR = SitelinksDetector()
