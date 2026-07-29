# Notability Detectors

For the data flow behind these detectors, including cache refreshes and worker behavior, see [Data flow and caches](data-flow.md).

This page explains the signals used by the notability checker. Each detector looks for one kind of evidence and maps it to the corresponding Wikidata notability criterion.

Signal levels are, from strongest to weakest:

- **Strong**: the detector found evidence that normally satisfies the criterion.
- **Unknown**: the detector has not yet finished checking the evidence.
- **Weak**: the detector found evidence related to the criterion, but not enough by itself to make notability likely.
- **Partial-strong**: one side of N2 is strong and the other side is missing.
- **Partial-weak**: one side of N2 is weak and the other side is missing.
- **None**: the detector found no evidence relating to the criterion.

Each detector maps to one of the direct parts of the notability criteria: N1 sitelinks, N2a identifiers, or N2b sources. `N12` is the intrinsic score derived from N1 and N2. The four N3 subcriteria are extrinsic structural signals: inlinks, OSM, wiki subscribers, and SDC. N3 is the maximum of those four N3 subcriteria.

N2 is derived from N2a and N2b, so it can also become `partial-weak` or `partial-strong` when only one side is present.
* N12 is the higher of N1 and N2 and is used by the Inlinks detector.
* N is the highest of N1, N2, and N3 and represents the overall notability of the item.


## Sitelinks

**Criterion:** N1, sitelinks

The sitelinks detector checks whether the item is linked to from a page on a Wikimedia project where a sitelink is meaningful evidence of notability.

It gives a **strong** N1 signal when the item has a valid sitelink on Wikipedia, Wikivoyage, Wikimedia Commons, Wikisource, Wikiquote, Wikinews, Wikibooks, Wikidata, Wikispecies, Wikiversity, or Wiktionary, as long as the page type is eligible for that project.

It gives a **weak** N1 signal when the sitelink is related to the item but is less conclusive. Examples include sitelinks on Meta, MediaWiki, or Wikimania; Commons category pages; template subpages; Wikisource mainspace subpages; redirect-marked sitelinks; and Commons category statements that act like sitelink evidence, such as [Commons category (P373)](https://www.wikidata.org/wiki/Property:P373).

For template sitelinks, the detector treats `doc`, `XML`, `meta`, `sandbox`, `testcases`, and `TemplateData` subpages as invalid, even though other template subpages may still count as weak evidence.

It gives **no** N1 signal for sitelinks that do not count toward the criterion. This includes unsupported projects, talk pages, user pages, draft pages, file pages, special pages, portal subpages, documentation-only module pages, style/script pages, Wikisource index and page namespaces, Wikinews comments pages, Wikidata item/property/lexeme/entity-schema pages, and Wiktionary mainspace and citation pages.


## Identifiers

**Criterion:** N2a, clearly identifiable conceptual or material entity

The identifiers detector checks whether the item has information that distinguishes it as a specific real, conceptual, or material entity.

It gives a **strong** N2a signal when the item has an external identifier that is not merely an online account identifier, or when it has a strong identifying property such as [inventory number (P217)](https://www.wikidata.org/wiki/Property:P217) or [legal citation of this text (P1031)](https://www.wikidata.org/wiki/Property:P1031).

It gives a **weak** N2a signal when the item has identifying information that is useful but less definitive on its own. In practice, that includes ordinary URL-style claims and the online-account or authority-control property sets. Examples include:
* [online account identifier (Q105388954)](https://www.wikidata.org/wiki/Q105388954)
* [authority control (Q18614948)](https://www.wikidata.org/wiki/Q18614948)
* [coordinate location (P625)](https://www.wikidata.org/wiki/Property:P625)
* [postal code (P281)](https://www.wikidata.org/wiki/Property:P281)
* [official website (P856)](https://www.wikidata.org/wiki/Property:P856)
* [streaming media URL (P963)](https://www.wikidata.org/wiki/Property:P963)
* [street address (P6375)](https://www.wikidata.org/wiki/Property:P6375)
* [published in (P1433)](https://www.wikidata.org/wiki/Property:P1433)
* [Wikisource index page URL (P1957)](https://www.wikidata.org/wiki/Property:P1957)
* [document file on Wikimedia Commons (P996)](https://www.wikidata.org/wiki/Property:P996)
* [work available at URL (P953)](https://www.wikidata.org/wiki/Property:P953)
* [exact match (P2888)](https://www.wikidata.org/wiki/Property:P2888).

It can also give a **weak** N2a signal when a specific combination of properties is present, even if each property by itself would only be weak evidence. For example, an item that has both [EXIF make (P2010)](https://www.wikidata.org/wiki/Property:P2010) and [EXIF model (P2009)](https://www.wikidata.org/wiki/Property:P2009) gets a weak signal from the pair.

If none of these identifying signals are present, the detector does not add support for N2a.

## Sources

**Criterion:** N2b, described by serious and publicly available references

The sources detector checks whether the item is supported by source-like statements or references.

It gives a **strong** N2b signal when a statement reference includes a substantial source indicator, such as:
* [reference URL (P854)](https://www.wikidata.org/wiki/Property:P854)
* [archive URL (P1065)](https://www.wikidata.org/wiki/Property:P1065)
* [described by source (P1343)](https://www.wikidata.org/wiki/Property:P1343)

It also gives a strong signal when the item has a property that directly indicates source coverage, such as:
* [described at URL (P973)](https://www.wikidata.org/wiki/Property:P973)
* [described by source (P1343)](https://www.wikidata.org/wiki/Property:P1343)
* A property in the [collection of properties that suggest notability (Q62589316)](https://www.wikidata.org/wiki/Q62589316)
* A top-level [reference URL (P854)](https://www.wikidata.org/wiki/Property:P854) claim, which the detector also treats as strong even though it is usually better used in a reference

It gives a **weak** N2b signal when the evidence is source-related but not enough by itself to show serious public coverage. Examples include:
* [stated in (P248)](https://www.wikidata.org/wiki/Property:P248)
* [official website (P856)](https://www.wikidata.org/wiki/Property:P856)
* [present in work (P1441)](https://www.wikidata.org/wiki/Property:P1441)
* [Wikimedia import URL (P4656)](https://www.wikidata.org/wiki/Property:P4656)
* [URL (P2699)](https://www.wikidata.org/wiki/Property:P2699)

`P2699` is accepted here for compatibility because it is a common reference mistake, but `P854` is the preferred property for reference URLs.

If the item has no recognized source or reference signals, the detector does not add support for N2b.


## Inlinks

**Criterion:** N3_inlinks, fulfills a structural need

The inlinks detector checks whether other Wikidata items link to the item being evaluated. It then considers the notability of those linking items.

It gives an N3_inlinks signal when another item links to this item and that linking item is itself supported by N1 or N2. The strength of the N3_inlinks signal follows the strength of the linking item's N1-or-N2 result.

This supports N3 because an item can be notable when it is needed to describe other notable items. For example, an item used as a value on several well-supported items may have structural value even if it has little direct coverage.

If some linking items have not yet been evaluated and no strong linking evidence is found, the result is **unknown** until those linked items can be checked.

If an item has no inlinks, then the result is **none**.


## Structured Data on Commons Usage

**Criterion:** N3_sdc, fulfills a structural need

The Structured Data on Commons usage detector checks whether the item is used in structured data statements on Wikimedia Commons media files.

It gives a **strong** N3_sdc signal when at least one Commons media file uses the item in structured data. This indicates that the item helps describe media on Commons and may be needed for structured media metadata.



## OpenStreetMap Usage

**Criterion:** N3_osm, fulfills a structural need

The OpenStreetMap detector checks whether the item is used by OpenStreetMap objects through a `wikidata=QID` tag.

It gives a **weak** N3_osm signal when at least one OpenStreetMap node, way, or relation refers to the item. This indicates external structural use, but it is treated as weaker evidence because OpenStreetMap is not an official Wikimedia Foundation project.



## Wikimedia Subscribers

**Criterion:** N3_wikisub, fulfills a structural need

The Wikisub detector determines if another project relies on a Wikidata item, perhaps by using it in a template.

It gives a **weak** N3_wikisub signal when the item is known to be used by at least one non-Wikidata Wikimedia wiki. This supports N3 because the item is needed by another Wikimedia wiki to display or organize content, but the signal is still coarse and indirect.
