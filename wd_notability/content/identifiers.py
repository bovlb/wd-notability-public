from collections.abc import AsyncGenerator, Generator
from time import perf_counter

from wd_notability.models import Detector, NotabilityCriterion, NotabilityLevel, SignalResult
from wd_notability.property_index import property_index


class IdentifiersDetector(Detector):
    ONLINE_ACCOUNTS_PROPERTIES: set[str] | None = None
    AUTHORITY_CONTROL_PROPERTIES: set[str] | None = None

    OTHER_STRONG_IDENTIFIERS = {
        "P217",
        "P1031",
    }

    OTHER_WEAK_IDENTIFIERS = {
        "P281",
        "P625",
        "P856",
        "P963",
        "P1433",
        "P6375",
        "P1957",
        "P996",
        "P953",
        "P1957",
    }

    def __init__(self) -> None:
        super().__init__("identifiers", NotabilityCriterion.N2a)

    def _all_claims(self, entity: dict) -> Generator[tuple[str, dict], None, None]:
        claims = entity.get("claims", {})
        for prop, claim_list in claims.items():
            for claim in claim_list:
                yield (prop, claim)

    def _claim_is_external_identifier(self, claim: dict) -> bool:
        mainsnak = claim.get("mainsnak", {})
        return mainsnak.get("datatype") == "external-id"

    def _claim_value(self, claim: dict):
        mainsnak = claim.get("mainsnak", {})
        datavalue = mainsnak.get("datavalue", {})
        if isinstance(datavalue, dict):
            return datavalue.get("value")
        return None

    async def _ensure_property_sets(self) -> None:
        missing_qids: list[str] = []
        if self.ONLINE_ACCOUNTS_PROPERTIES is None:
            missing_qids.append("Q105388954")
        if self.AUTHORITY_CONTROL_PROPERTIES is None:
            missing_qids.append("Q18614948")

        if missing_qids:
            print(f"IdentifiersDetector: fetching property sets for missing QIDs: {missing_qids}")
            start_time = perf_counter()
            property_sets = await property_index.property_instances_for(missing_qids)
            if self.ONLINE_ACCOUNTS_PROPERTIES is None:
                self.__class__.ONLINE_ACCOUNTS_PROPERTIES = property_sets.get("Q105388954", set())
            if self.AUTHORITY_CONTROL_PROPERTIES is None:
                self.__class__.AUTHORITY_CONTROL_PROPERTIES = property_sets.get("Q18614948", set())
            elapsed = perf_counter() - start_time
            print(f"IdentifiersDetector: fetched property sets for missing QIDs in {elapsed:.2f} seconds")

    async def detect(self, entity: dict) -> AsyncGenerator[SignalResult, None]:
        await self._ensure_property_sets()

        online_accounts = self.ONLINE_ACCOUNTS_PROPERTIES or set()
        authority_control = self.AUTHORITY_CONTROL_PROPERTIES or set()

        for prop, claim in self._all_claims(entity):
            value = self._claim_value(claim)
            if self._claim_is_external_identifier(claim):
                if prop not in online_accounts:
                    yield self.make_signal(level=NotabilityLevel.STRONG, key="identifiers_identifier_not_online_account", properties={"property": prop, "value": value})
                    continue

                yield self.make_signal(level=NotabilityLevel.WEAK, key="identifiers_identifier_online_account", properties={"property": prop, "value": value})
                continue

            if prop in self.OTHER_STRONG_IDENTIFIERS:
                yield self.make_signal(level=NotabilityLevel.STRONG, key="identifiers_not_identifier_strong", properties={"property": prop, "value": value})
                continue

            if prop in self.OTHER_WEAK_IDENTIFIERS:
                yield self.make_signal(level=NotabilityLevel.WEAK, key="identifiers_not_identifier_weak", properties={"property": prop, "value": value})

            if prop in authority_control:
                yield self.make_signal(level=NotabilityLevel.WEAK, key="identifiers_not_identifier_authority_control", properties={"property": prop, "value": value})
                continue


IDENTIFIERS_DETECTOR = IdentifiersDetector()

