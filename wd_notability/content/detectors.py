from wd_notability.content.identifiers import IDENTIFIERS_DETECTOR
from wd_notability.content.sitelinks import SITELINKS_DETECTOR
from wd_notability.content.sources import SOURCES_DETECTOR

# Detector set registered with the content source.
CONTENT_DETECTORS = {
    SITELINKS_DETECTOR,
    IDENTIFIERS_DETECTOR,
    SOURCES_DETECTOR,
}

__all__ = [
    "CONTENT_DETECTORS",
    "IDENTIFIERS_DETECTOR",
    "SITELINKS_DETECTOR",
    "SOURCES_DETECTOR",
]
