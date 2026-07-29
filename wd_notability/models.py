from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Collection
from enum import Enum, IntEnum
from typing import Any, ClassVar, Final, Set
import time

from pydantic import BaseModel, ConfigDict, Field

# Alias QID as string
QID = str
# alias ItemData as dict
ItemData = dict
Entity = dict

class NotabilityLevel(IntEnum):
    NONE = 0
    PARTIAL_WEAK = 1
    PARTIAL_STRONG = 2
    WEAK = 3
    UNKNOWN = 4
    STRONG = 5

    def __str__(self):
        return {
            NotabilityLevel.NONE: "none",
            NotabilityLevel.PARTIAL_WEAK: "partial-weak",
            NotabilityLevel.PARTIAL_STRONG: "partial-strong",
            NotabilityLevel.WEAK: "weak",
            NotabilityLevel.UNKNOWN: "unknown",
            NotabilityLevel.STRONG: "strong",
        }[self]

    @property
    def value_str(self):
        return str(self)
    

class EvaluationReason(IntEnum):
    """
    Encodes the reason why we are asking for the status of a given entity.
    Values are stable persisted IDs, not priorities.
    """
    TEXT = 0 # mentioned in text
    USE = 1 # used in a statement
    INLINK = 2 # links to another entity
    EDIT = 3 # Edit seen in user contributions or recent changes
    CREATE = 4 # Creation seen in user contributions or recent changes
    PAGE = 5 # looking at specific page (or page history, etc.)

    def __str__(self):
        return {
            EvaluationReason.TEXT: "text",
            EvaluationReason.USE: "use",
            EvaluationReason.INLINK: "inlink",
            EvaluationReason.EDIT: "edit",
            EvaluationReason.CREATE: "create",
            EvaluationReason.PAGE: "page",
        }[self]
    
    @classmethod
    def from_str(cls, reason_str: str) -> "EvaluationReason":
        mapping = {
            "text": cls.TEXT,
            "use": cls.USE,
            "inlink": cls.INLINK,
            "edit": cls.EDIT,
            "create": cls.CREATE,
            "page": cls.PAGE,
        }
        if reason_str not in mapping:
            raise ValueError(f"Unknown reason string: {reason_str}")
        return mapping[reason_str]
    
    @classmethod
    def from_int(cls, value: int) -> "EvaluationReason":
        mapping = {
            0: cls.TEXT,
            1: cls.USE,
            2: cls.INLINK,
            3: cls.EDIT,
            4: cls.CREATE,
            5: cls.PAGE,
        }
        if value not in mapping:
            raise ValueError(f"Unknown reason value: {value}")
        return mapping[value]

    @property
    def priority(self) -> int:
        return {
            EvaluationReason.INLINK: 0,
            EvaluationReason.TEXT: 1,
            EvaluationReason.USE: 2,
            EvaluationReason.EDIT: 3,
            EvaluationReason.CREATE: 4,
            EvaluationReason.PAGE: 5,
        }[self]


class NotabilityCriterion(str, Enum):
    N1 = "N1"  # sitelinks
    N2a = "N2a"  # identifier
    N2b = "N2b"  # source
    N3_INLINKS = "N3_inlinks"  # structural need from inlinks
    N3_OSM = "N3_osm"  # structural need from OSM
    N3_WIKISUB = "N3_wikisub"  # structural need from wiki subscriptions
    N3_SDC = "N3_sdc"  # structural need from SDC
    N3 = "N3"  # computed structural need
    N2 = "N2"  # minimum of N2a and N2b
    N12 = "N12"  # maximum of N1 and N2
    N = "N"  # overall notability, maximum of N1, N2, and N3


EXTERNAL_USAGE_LEVELS: Final[dict[NotabilityCriterion, NotabilityLevel]] = {
    NotabilityCriterion.N3_OSM: NotabilityLevel.WEAK,
    NotabilityCriterion.N3_WIKISUB: NotabilityLevel.WEAK,
    NotabilityCriterion.N3_SDC: NotabilityLevel.STRONG,
}


def external_usage_level(criterion: NotabilityCriterion) -> NotabilityLevel:
    return EXTERNAL_USAGE_LEVELS[criterion]


class SignalResult(BaseModel):
    detector: str = ""
    criterion: NotabilityCriterion
    level: NotabilityLevel
    key: str
    properties: dict[str, Any] = Field(default_factory=dict)


SourceContext = Any # how excitingly vague


class Detector(ABC):
    def __init__(self, name: str, criterion: NotabilityCriterion):
        self.name = name
        self.criterion = criterion

    @abstractmethod
    async def detect(self, qid: QID, context: SourceContext) -> AsyncGenerator[SignalResult, None]:
        raise NotImplementedError

    def make_signal(
        self,
        *,
        level: NotabilityLevel,
        key: str,
        properties: dict[str, Any] | None = None,
    ) -> SignalResult:
        return SignalResult(
            detector=self.name,
            criterion=self.criterion,
            level=level,
            key=key,
            properties=properties or {},
        )

    async def run(self, *args, **kwargs) -> AsyncGenerator[SignalResult, None]:
        async for raw_signal in self.detect(*args, **kwargs):
            signal = raw_signal if isinstance(raw_signal, SignalResult) else SignalResult.model_validate(raw_signal)
            yield self.make_signal(
                level=signal.level,
                key=signal.key,
                properties=signal.properties,
            )


class EvaluationResult(BaseModel):
    qid: str
    n1: NotabilityLevel = NotabilityLevel.NONE
    n2a: NotabilityLevel = NotabilityLevel.NONE
    n2b: NotabilityLevel = NotabilityLevel.NONE
    n3_inlinks: NotabilityLevel = NotabilityLevel.NONE
    n3_osm: NotabilityLevel = NotabilityLevel.NONE
    n3_wikisub: NotabilityLevel = NotabilityLevel.NONE
    n3_sdc: NotabilityLevel = NotabilityLevel.NONE
    inlinks_count: int = 0
    signals: list[SignalResult] = Field(default_factory=list)
    errors: dict[str, list[str]] = Field(
        default_factory=lambda: {
            NotabilityCriterion.N1.value: [],
            NotabilityCriterion.N2a.value: [],
            NotabilityCriterion.N2b.value: [],
            NotabilityCriterion.N3_INLINKS.value: [],
            NotabilityCriterion.N3_OSM.value: [],
            NotabilityCriterion.N3_WIKISUB.value: [],
            NotabilityCriterion.N3_SDC.value: [],
        }
    )
    has_claims_count: int = 0
    has_claims_known: bool = False
    has_sitelinks_count: int = 0
    is_redirect: bool = False
    redirect_target: int | None = None
    is_deleted: bool = False
    content_last_revid: int | None = None
    recent_changes_last_revid: int | None = None
    source_urls: list[dict[str, str]] = Field(default_factory=list)
    source_timings: dict[str, float] = Field(default_factory=dict)
    source_contexts: dict[str, Any] = Field(default_factory=dict)

    def add_error(self, detector: Detector, error: Exception) -> None:
        self.errors.setdefault(detector.criterion.value, []).append(
            f"{detector.name}: {error}"
        )
        self.set(detector.criterion, NotabilityLevel.UNKNOWN)

    def set(self, criterion: NotabilityCriterion, level: NotabilityLevel) -> None:
        match criterion:
            case NotabilityCriterion.N1:
                self.n1 = max(self.n1, level)
            case NotabilityCriterion.N2a:
                self.n2a = max(self.n2a, level)
            case NotabilityCriterion.N2b:
                self.n2b = max(self.n2b, level)
            case NotabilityCriterion.N3_INLINKS:
                self.n3_inlinks = max(self.n3_inlinks, level)
            case NotabilityCriterion.N3_OSM:
                self.n3_osm = max(self.n3_osm, level)
            case NotabilityCriterion.N3_WIKISUB:
                self.n3_wikisub = max(self.n3_wikisub, level)
            case NotabilityCriterion.N3_SDC:
                self.n3_sdc = max(self.n3_sdc, level)
            case _:
                raise ValueError(f"Cannot set derived criterion {criterion.value}")

    @property
    def n2(self) -> NotabilityLevel:
        return deduce_n2(self.n2a, self.n2b)

    @property
    def n12(self) -> NotabilityLevel:
        return max(self.n1, self.n2)

    @property
    def n(self) -> NotabilityLevel:
        return max(self.n1, self.n2, self.n3)

    @property
    def n3(self) -> NotabilityLevel:
        return max(self.n3_inlinks, self.n3_osm, self.n3_wikisub, self.n3_sdc)

    @property
    def levels(self) -> dict[NotabilityCriterion, NotabilityLevel]:
        return {
            NotabilityCriterion.N1: self.n1,
            NotabilityCriterion.N2a: self.n2a,
            NotabilityCriterion.N2b: self.n2b,
            NotabilityCriterion.N3_INLINKS: self.n3_inlinks,
            NotabilityCriterion.N3_OSM: self.n3_osm,
            NotabilityCriterion.N3_WIKISUB: self.n3_wikisub,
            NotabilityCriterion.N3_SDC: self.n3_sdc,
            NotabilityCriterion.N3: self.n3,
            NotabilityCriterion.N2: self.n2,
            NotabilityCriterion.N12: self.n12,
            NotabilityCriterion.N: self.n,
        }

    @property
    def levels_str(self) -> dict[str, str]:
        return {k.value: v.value_str for k, v in self.levels.items()}

    @property
    def has_claims(self) -> bool:
        return self.has_claims_count > 0

    @property
    def has_sitelinks(self) -> bool:
        return self.has_sitelinks_count > 0

    @classmethod
    def combine(cls, qid: QID, parts: list["EvaluationResult"]) -> "EvaluationResult":
        result = cls(qid=qid)
        for part in parts:
            result.signals.extend(part.signals)
            result.source_urls.extend(part.source_urls)
            result.source_timings.update(part.source_timings)
            result.source_contexts.update(part.source_contexts)
            result.has_claims_count = max(result.has_claims_count, part.has_claims_count)
            result.has_claims_known = result.has_claims_known or part.has_claims_known
            result.has_sitelinks_count = max(result.has_sitelinks_count, part.has_sitelinks_count)
            result.inlinks_count = max(result.inlinks_count, part.inlinks_count)
            result.is_redirect = result.is_redirect or part.is_redirect
            if result.redirect_target is None and part.redirect_target is not None:
                result.redirect_target = part.redirect_target
            result.is_deleted = result.is_deleted or part.is_deleted
            if part.content_last_revid is not None:
                result.content_last_revid = (
                    part.content_last_revid
                    if result.content_last_revid is None
                    else max(result.content_last_revid, part.content_last_revid)
                )
            if part.recent_changes_last_revid is not None:
                result.recent_changes_last_revid = (
                    part.recent_changes_last_revid
                    if result.recent_changes_last_revid is None
                    else max(result.recent_changes_last_revid, part.recent_changes_last_revid)
                )

            result.set(NotabilityCriterion.N1, part.n1)
            result.set(NotabilityCriterion.N2a, part.n2a)
            result.set(NotabilityCriterion.N2b, part.n2b)
            result.set(NotabilityCriterion.N3_INLINKS, part.n3_inlinks)
            result.set(NotabilityCriterion.N3_OSM, part.n3_osm)
            result.set(NotabilityCriterion.N3_WIKISUB, part.n3_wikisub)
            result.set(NotabilityCriterion.N3_SDC, part.n3_sdc)

            for criterion, errors in part.errors.items():
                result.errors.setdefault(criterion, []).extend(errors)

        return result


def deduce_n2(n2a: NotabilityLevel, n2b: NotabilityLevel) -> NotabilityLevel:
    if NotabilityLevel.UNKNOWN in {n2a, n2b}:
        return NotabilityLevel.UNKNOWN
    if n2a == NotabilityLevel.NONE and n2b == NotabilityLevel.NONE:
        return NotabilityLevel.NONE
    if n2a == NotabilityLevel.NONE:
        if n2b in {NotabilityLevel.PARTIAL_STRONG, NotabilityLevel.STRONG}:
            return NotabilityLevel.PARTIAL_STRONG
        return NotabilityLevel.PARTIAL_WEAK
    if n2b == NotabilityLevel.NONE:
        if n2a in {NotabilityLevel.PARTIAL_STRONG, NotabilityLevel.STRONG}:
            return NotabilityLevel.PARTIAL_STRONG
        return NotabilityLevel.PARTIAL_WEAK
    return min(n2a, n2b)
    

class Source(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    detectors: Set[Detector]
    
    async def get_contexts(self, qids: Collection[QID]) -> dict[QID, SourceContext]:
        """Preparation step to fetch information needed by the detectors.

        Sources should override this to batch their work across many QIDs.
        The default implementation preserves backwards compatibility by
        delegating to ``get_context()`` one QID at a time.
        """
        contexts: dict[QID, SourceContext] = {}
        for qid in qids:
            try:
                contexts[qid] = await self.get_context(qid)
            except Exception as exc:  # noqa: BLE001
                contexts[qid] = exc
        return contexts

    async def get_context(self, qid: QID) -> SourceContext:
        return {"qid": qid}

    def detector_context(self, context: SourceContext) -> SourceContext:
        """
        Convert source context into the value passed to each detector.
        """
        return context

    async def update_result(self, result: EvaluationResult, context: SourceContext) -> None:
        """
        Apply non-signal source metadata to an evaluation result.
        """
        pass

    async def refresh_cache(self, cache: Any, data: Any) -> int:
        """
        Populate the main evaluation cache from source-specific build data.

        Sources that can derive structural criteria from cached usage data can
        override this so builders can refresh the main cache in one batch.
        """
        return 0

    @property
    def criteria(self) -> Set[NotabilityCriterion]:
        """
        The set of criteria that this source can provide signals for, derived from its detectors.
        """
        return {detector.criterion for detector in self.detectors}
    
    async def extra(self, qid: QID, context: SourceContext, result: EvaluationResult) -> None:
        """Extra processing step after all detectors have run. This is called once per source, and can be used to update the cache with additional information derived from the context or the detector results."""
        pass

    async def extra_many(
        self,
        qids: Collection[QID],
        contexts: dict[QID, SourceContext],
        results: dict[QID, EvaluationResult],
    ) -> dict[QID, float]:
        timings: dict[QID, float] = {}
        for qid in qids:
            context = contexts.get(qid)
            result = results.get(qid)
            if context is None or result is None or isinstance(context, Exception):
                continue
            start = time.perf_counter()
            await self.extra(qid, context, result)
            timings[qid] = time.perf_counter() - start
        return timings

    def report_urls(self, qid: QID, context: SourceContext) -> dict[str, str]:
        return {}

    async def _run_context_core(self, qid: QID, context: SourceContext) -> EvaluationResult:
        result = EvaluationResult(qid=qid)
        timings: dict[str, float] = {}

        if isinstance(context, dict):
            context_timings = context.get("_timings")
            if isinstance(context_timings, dict):
                for key, value in context_timings.items():
                    if isinstance(value, (int, float)):
                        timings[key] = float(value)

        urls = self.report_urls(qid, context)
        if urls:
            result.source_urls.append({"source": self.name, **urls})

        start = time.perf_counter()
        await self.update_result(result, context)
        timings["post_process"] = time.perf_counter() - start
        timings["update_result"] = timings["post_process"]

        detector_context = self.detector_context(context)
        start = time.perf_counter()
        for detector in self.detectors:
            detector_start = time.perf_counter()
            try:
                async for signal in detector.run(detector_context):
                    result.signals.append(signal)
                    result.set(signal.criterion, signal.level)
            except Exception as exc:
                result.add_error(detector, exc)
            timings[f"detector_{detector.name}"] = time.perf_counter() - detector_start
        timings["detectors"] = time.perf_counter() - start

        result.source_timings = timings
        return result

    async def run_context(self, qid: QID, context: SourceContext) -> EvaluationResult:
        result = await self._run_context_core(qid, context)
        result.source_contexts[self.name] = context
        extra_timings = await self.extra_many([qid], {qid: context}, {qid: result})
        result.source_timings["extra"] = extra_timings.get(qid, 0.0)
        return result

    async def run(self, qid: QID) -> EvaluationResult:
        start = time.perf_counter()
        contexts = await self.get_contexts([qid])
        elapsed = time.perf_counter() - start
        context = contexts.get(qid)
        if isinstance(context, Exception):
            raise context
        if qid not in contexts:
            raise KeyError(f"Source {self.name} did not return context for {qid}")
        if isinstance(context, dict):
            timings = context.get("_timings")
            if not isinstance(timings, dict):
                timings = {}
                context["_timings"] = timings
            timings.setdefault("get_context", elapsed)
        return await self.run_context(qid, context)
