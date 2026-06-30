from collections.abc import AsyncGenerator, Generator

from wd_notability.models import Detector, Entity, NotabilityCriterion, NotabilityLevel, SignalResult
from wd_notability.property_index import property_index


class SourcesDetector(Detector):
    PROPERTIES_THAT_SUGGEST_STRONG_NOTABILITY: set[str] | None = None

    OTHER_STRONG_PROPERTIES = [
        "P973",
        "P1343",
    ]

    WEAK_PROPERTIES = [
        "P856",
    ]

    STRONG_REFERENCE_PROPERTIES = [
        "P854",
        "P1065",
        "P1343",
    ]

    WEAK_REFERENCE_PROPERTIES = [
        "P248",
        "P4656",
    ]

    def __init__(self) -> None:
        super().__init__("sources", NotabilityCriterion.N2b)

    def _all_references(self, entity: Entity) -> Generator[dict, None, None]:
        for prop_claims in entity.get("claims", {}).values():
            for claim in prop_claims:
                for ref in claim.get("references", []):
                    yield ref["snaks"]

    def _has_property(self, entity: Entity, prop: str) -> bool:
        return prop in entity.get("claims", {})

    def _snak_value(self, snak: dict):
        datavalue = snak.get("datavalue", {})
        if isinstance(datavalue, dict):
            return datavalue.get("value")
        return None

    def _reference_values(self, reference: dict, prop: str) -> list[object]:
        snaks = reference.get(prop, [])
        if not isinstance(snaks, list):
            return []
        return [self._snak_value(snak) for snak in snaks if isinstance(snak, dict)]

    def _claim_values(self, entity: Entity, prop: str) -> list[object]:
        claims = entity.get("claims", {}).get(prop, [])
        if not isinstance(claims, list):
            return []
        values: list[object] = []
        for claim in claims:
            if isinstance(claim, dict):
                mainsnak = claim.get("mainsnak", {})
                if isinstance(mainsnak, dict):
                    values.append(self._snak_value(mainsnak))
        return values

    async def _ensure_property_set(self) -> None:
        if self.PROPERTIES_THAT_SUGGEST_STRONG_NOTABILITY is None:
            property_sets = await property_index.property_instances_for(["Q62589316"])
            self.__class__.PROPERTIES_THAT_SUGGEST_STRONG_NOTABILITY = property_sets.get("Q62589316", set())

    async def detect(self, entity: Entity) -> AsyncGenerator[SignalResult, None]:
        await self._ensure_property_set()
        strong_props = self.PROPERTIES_THAT_SUGGEST_STRONG_NOTABILITY or set()

        for reference in self._all_references(entity):
            for prop in self.STRONG_REFERENCE_PROPERTIES:
                if prop in reference:
                    yield self.make_signal(level=NotabilityLevel.STRONG, key="sources_strong_reference_property", properties={"property": prop, "values": self._reference_values(reference, prop)})
                    break
            else:
                for prop in self.WEAK_REFERENCE_PROPERTIES:
                    if prop in reference:
                        yield self.make_signal(level=NotabilityLevel.WEAK, key="sources_weak_reference_property", properties={"property": prop, "values": self._reference_values(reference, prop)})
                        break

        for prop in strong_props:
            if self._has_property(entity, prop):
                yield self.make_signal(level=NotabilityLevel.STRONG, key="sources_strong_identifier", properties={"property": prop, "values": self._claim_values(entity, prop)})

        for prop in self.OTHER_STRONG_PROPERTIES:
            if self._has_property(entity, prop):
                yield self.make_signal(level=NotabilityLevel.STRONG, key="sources_other_strong_property", properties={"property": prop, "values": self._claim_values(entity, prop)})

        for prop in self.WEAK_PROPERTIES:
            if self._has_property(entity, prop):
                yield self.make_signal(level=NotabilityLevel.WEAK, key="sources_other_weak_property", properties={"property": prop, "values": self._claim_values(entity, prop)})


SOURCES_DETECTOR = SourcesDetector()

