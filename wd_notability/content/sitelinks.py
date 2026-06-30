from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Optional

from wd_notability.models import Detector, NotabilityCriterion, NotabilityLevel, SignalResult
from wd_notability.namespaces import Namespace, namespace_extractor
from wd_notability.project_types import ProjectType, project_type_extractor


class SitelinksDetector(Detector):
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
        ProjectType.WIKTIONARY,
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
        Namespace.MAIN,
        Namespace.PROPERTY,
        Namespace.LEXEME,
        Namespace.ENTITY_SCHEMA,
    }

    WIKTIONARY_BAD_NAMESPACES = {
        Namespace.CITATIONS,
        Namespace.MAIN,
    }

    SITELINK_LIKE_PROPERTIES = ("P373",)

    REDIRECT_BADGES = {
        "Q70894304",
        "Q70893996",
    }

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

    def get_badges(self, sitelink: dict) -> list[str]:
        badges = sitelink.get("badges", [])
        if not isinstance(badges, list):
            return []
        return [badge for badge in badges if isinstance(badge, str)]

    def has_redirect_badge(self, sitelink: dict) -> bool:
        return bool(self.REDIRECT_BADGES.intersection(self.get_badges(sitelink)))

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
            return self.make_signal(level=level, key=f"sitelinks_{key}", properties={**properties, **kwargs})

        if project_type in self.UNDETERMINED_PROJECTS:
            return make_result(NotabilityLevel.WEAK, "undetermined_project")
        if project_type not in self.VALID_PROJECTS:
            return make_result(NotabilityLevel.NONE, "invalid_project")
        if namespace in self.INVALID_NAMESPACES:
            return make_result(NotabilityLevel.NONE, "invalid_namespace", namespace=namespace.value)
        if namespace == Namespace.PORTAL and subpage is not None:
            return make_result(NotabilityLevel.NONE, "invalid_portal_subpage")
        if suffix in {"css", "js"}:
            return make_result(NotabilityLevel.NONE, "invalid_suffix", suffix=suffix)
        if namespace == Namespace.TEMPLATE:
            if subpage is not None:
                if subpage in self.TEMPLATE_BAD_SUBPAGES:
                    return make_result(NotabilityLevel.NONE, "invalid_template_subpage", subpage=subpage)
                return make_result(NotabilityLevel.WEAK, "template_subpage", subpage=subpage)
            return make_result(NotabilityLevel.STRONG, "template")
        if namespace == Namespace.MODULE and subpage == "doc":
            return make_result(NotabilityLevel.NONE, "invalid_module_doc")
        if namespace == Namespace.CATEGORY and project_type == ProjectType.COMMONS:
            return make_result(NotabilityLevel.WEAK, "commons_category")
        if project_type == ProjectType.WIKISOURCE:
            if namespace in {Namespace.INDEX, Namespace.PAGE}:
                return make_result(NotabilityLevel.NONE, "invalid_wikisource_namespace", namespace=namespace.value)
            if namespace == Namespace.MAIN and subpage is not None:
                return make_result(NotabilityLevel.WEAK, "wikisource_main_subpage", subpage=subpage)
        if project_type == ProjectType.WIKINEWS and namespace == Namespace.COMMENTS:
            return make_result(NotabilityLevel.NONE, "invalid_wikinews_namespace", namespace=namespace.value)
        if project_type == ProjectType.WIKIDATA and namespace in self.WIKIDATA_BAD_NAMESPACES:
            return make_result(NotabilityLevel.NONE, "invalid_wikidata_namespace", namespace=namespace.value)
        if project_type == ProjectType.WIKTIONARY and namespace in self.WIKTIONARY_BAD_NAMESPACES:
            return make_result(NotabilityLevel.NONE, "invalid_wiktionary_namespace", namespace=namespace.value)
        if self.has_redirect_badge(sitelink):
            return make_result(NotabilityLevel.WEAK, "redirect_badge", badges=self.get_badges(sitelink))
        return make_result(NotabilityLevel.STRONG, "valid_sitelink")

    async def detect(self, entity: dict) -> AsyncGenerator[SignalResult, None]:
        sitelinks = entity.get("sitelinks", {})
        for sitelink in sitelinks.values():
            try:
                yield await self.test_sitelink(sitelink)
            except Exception as exc:  # noqa: BLE001
                site = sitelink.get("site") if isinstance(sitelink, dict) else None
                title = sitelink.get("title") if isinstance(sitelink, dict) else None
                yield self.make_signal(level=NotabilityLevel.NONE, key="sitelink_error", properties={"site": site, "title": title, "error": str(exc)})

        for prop in self.SITELINK_LIKE_PROPERTIES:
            if self.entity_get_claims_by_property(entity, prop):
                yield self.make_signal(
                    level=NotabilityLevel.WEAK,
                    key="sitelink_like_property",
                    properties={"property": prop, "values": [claim.get("mainsnak", {}).get("datavalue", {}).get("value") for claim in self.entity_get_claims_by_property(entity, prop)]},
                )


SITELINKS_DETECTOR = SitelinksDetector()

