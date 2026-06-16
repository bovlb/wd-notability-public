# Notability Detectors

For the data flow behind these detectors, including cache refreshes and worker behavior, see [Data flow and caches](data-flow.md).

This page explains the signals used by the notability checker. Each detector looks for one kind of evidence and maps it to the corresponding Wikidata notability criterion.

Signal levels mean:

- **Strong**: the detector found evidence that normally satisfies the criterion.
- **Weak**: the detector found evidence related to the criterion, but not enough by itself to make notability likely.
- **Unknown**: the detector could not finish checking the evidence.

Each detector produces evidence related to one of the direct parts of the notability criteria (N1 sitelinks, N2a identifiers, N2b sources, and the four N3 subcriteria: inlinks, OSM, wiki subscribers, and SDC).
These are combined by picking the highest level for each one (or **unknown** if the highest level is lower than **strong** and there is information missing). The overall N3 value is computed from the four N3 subcriteria.
* N2 is satisfied only when both parts of N2 are supported: N2a shows that the item is clearly identifiable, and N2b shows that it is supported by serious public sources.
* N12 is the higher of N1 and N2 and is used by the Inlinks detector.
* N is the highest of N1, N2, and N3 and represents the overall notability of the item.


## Sitelinks

**Criterion:** N1, sitelinks

The sitelinks detector checks whether the item is linked to from a page on a Wikimedia project where a sitelink is meaningful evidence of notability.

It gives a **strong** N1 signal when the item has a valid sitelink on Wikipedia, Wikimedia Commons, Wikidata pages outside the item/property/lexeme/entity-schema namespaces, or an eligible sister project. A valid sitelink is one that points to a project and page type that can reasonably represent the subject.

It gives a **weak** N1 signal when the sitelink is related to the item but is less conclusive. Examples include sitelinks on projects whose status is uncertain for this purpose, Commons category pages, template subpages, Wikisource mainspace subpages, and Commons category statements that act like sitelink evidence, such as [Commons category (P373)](https://www.wikidata.org/wiki/Property:P373).

It gives **no** N1 signal for sitelinks that do not count toward the criterion. This includes unsupported projects, talk pages, user pages, draft pages, file pages, special pages, portal subpages, documentation-only module pages, style/script pages, Wikidata item/property/lexeme/entity-schema pages, and project-specific page types that are excluded from N1.


## Identifiers

**Criterion:** N2a, clearly identifiable conceptual or material entity

The identifiers detector checks whether the item has information that distinguishes it as a specific real, conceptual, or material entity.

It gives a **strong** N2a signal when the item has an external identifier that is not merely an online account identifier, or when it has a strong identifying property such as [inventory number (P217)](https://www.wikidata.org/wiki/Property:P217) or [legal citation of this text (P1031)](https://www.wikidata.org/wiki/Property:P1031).

It gives a **weak** N2a signal when the item has identifying information that is useful but less definitive on its own. Examples include properties in the [online account identifier collection (Q105388954)](https://www.wikidata.org/wiki/Q105388954), properties in the [authority control collection (Q18614948)](https://www.wikidata.org/wiki/Q18614948), [coordinate location (P625)](https://www.wikidata.org/wiki/Property:P625), [postal code (P281)](https://www.wikidata.org/wiki/Property:P281), [official website (P856)](https://www.wikidata.org/wiki/Property:P856), [street address (P6375)](https://www.wikidata.org/wiki/Property:P6375), [published in (P1433)](https://www.wikidata.org/wiki/Property:P1433), [Wikisource index page URL (P1957)](https://www.wikidata.org/wiki/Property:P1957), or [document file on Wikimedia Commons (P996)](https://www.wikidata.org/wiki/Property:P996).

If none of these identifying signals are present, the detector does not add support for N2a.

## Sources

**Criterion:** N2b, described by serious and publicly available references

The sources detector checks whether the item is supported by source-like statements or references.

It gives a **strong** N2b signal when a statement reference includes a substantial source indicator, such as [reference URL (P854)](https://www.wikidata.org/wiki/Property:P854) or [archive URL (P1065)](https://www.wikidata.org/wiki/Property:P1065). It also gives a strong signal when the item has a property that directly indicates source coverage, such as [described at URL (P973)](https://www.wikidata.org/wiki/Property:P973), [described by source (P1343)](https://www.wikidata.org/wiki/Property:P1343), or a property in the [collection of properties that suggest notability (Q62589316)](https://www.wikidata.org/wiki/Q62589316).

It gives a **weak** N2b signal when the evidence is source-related but not enough by itself to show serious public coverage. Examples include [stated in (P248)](https://www.wikidata.org/wiki/Property:P248), [official website (P856)](https://www.wikidata.org/wiki/Property:P856), or [Wikimedia import URL (P4656)](https://www.wikidata.org/wiki/Property:P4656) used as reference evidence.

If the item has no recognized source or reference signals, the detector does not add support for N2b.


## Inlinks

**Criterion:** N3_inlinks, fulfills a structural need

The inlinks detector checks whether other Wikidata items link to the item being evaluated. It then considers the notability of those linking items.

It gives an N3_inlinks signal when another item links to this item and that linking item is itself supported by N1 or N2. The strength of the N3_inlinks signal follows the strength of the linking item's N1-or-N2 result.

This supports N3 because an item can be notable when it is needed to describe other notable items. For example, an item used as a value on several well-supported items may have structural value even if it has little direct coverage.

If some linking items have not yet been evaluated and no strong linking evidence is found, the result may be **unknown** until those linked items can be checked.

If an item has no inlinks, then the result is **none**.


## Structured Data on Commons Usage

**Criterion:** N3_sdc, fulfills a structural need

The Structured Data on Commons usage detector checks whether the item is used in structured data statements on Wikimedia Commons media files. It looks for use in common Commons item-valued statements: [depicts (P180)](https://www.wikidata.org/wiki/Property:P180), [main subject (P921)](https://www.wikidata.org/wiki/Property:P921), [creator (P170)](https://www.wikidata.org/wiki/Property:P170), [collection (P195)](https://www.wikidata.org/wiki/Property:P195), [material used (P186)](https://www.wikidata.org/wiki/Property:P186), [location (P276)](https://www.wikidata.org/wiki/Property:P276), [location of creation (P1071)](https://www.wikidata.org/wiki/Property:P1071), and [digital representation of (P6243)](https://www.wikidata.org/wiki/Property:P6243).

It gives a **strong** N3_sdc signal when at least one Commons media file uses the item in structured data. This indicates that the item helps describe media on Commons and may be needed for structured media metadata.



## OpenStreetMap Usage

**Criterion:** N3_osm, fulfills a structural need

The OpenStreetMap detector checks whether the item is used by OpenStreetMap objects through a `wikidata=QID` tag.

It gives a **weak** N3_osm signal when at least one OpenStreetMap node, way, or relation refers to the item. This indicates external structural use, but it is treated as weaker evidence because OpenStreetMap is not an official Wikimedia Foundation project.



## Wikimedia Subscribers

**Criterion:** N3_wikisub, fulfills a structural need

The Wikisub detector determines if another project relies on a Wikidata item, perhaps by using it in a template.

It gives a **strong** N3_wikisub signal when the item is known to be used by at least one non-Wikidata Wikimedia wiki. This supports N3 because the item is needed by another Wikimedia wiki to display or organize content.
