# Badge

The notability badge is a compact summary of the evaluation state.

The outer ring shows overall notability:

- `none` is red.
- `weak` is orange.
- `strong` is green.
- `unknown` is grey.
- `partial-weak` and `partial-strong` use a split ring. The badge renderer uses the same split styling for the N3 wedge when that field is partial.

The inner regions show the main criteria:

- Left: `N1` sitelinks.
- Center top: `N2a` identifiers.
- Center bottom: `N2b` sources.
- Right: `N3` structural need.

Hovering a badge opens a small hierarchy card with color-coded levels plus the counts and flags behind them.

If an item has no claims, both N2 halves are white.

If the item is a redirect, the purple arrow overlays the badge and the target item is evaluated normally.

If the item is deleted, the badge is replaced by a large red X.

## Segment Map

This diagram labels the five badge regions:

<style>
  .badge-legend {
    margin: 1rem 0 1.5rem;
    padding: .9rem;
    border: 1px solid var(--border);
    border-radius: 16px;
    background: var(--panel, transparent);
  }
  .badge-legend svg {
    width: 100%;
    height: auto;
    display: block;
  }
  .badge-legend .label {
    font: 700 15px ui-sans-serif, system-ui, sans-serif;
    fill: var(--text, #111);
  }
  .badge-legend .connector {
    stroke: var(--border, #999);
    stroke-width: 1.5;
    fill: none;
  }
  .badge-legend .badge-outline {
    fill: none;
    stroke: var(--level-strong, #1b7f2a);
    stroke-width: 1.5;
  }
  .badge-legend .badge-n1 { fill: var(--level-strong, #1b7f2a); stroke: #165d20; stroke-width: .45; }
  .badge-legend .badge-n2a { fill: var(--level-weak, #b26a00); stroke: #8b5200; stroke-width: .45; }
  .badge-legend .badge-n2b,
  .badge-legend .badge-n3 { fill: var(--level-none, #b00020); stroke: #7e0017; stroke-width: .45; }
</style>

<figure class="badge-legend">
  <svg viewBox="50 50 370 100" role="img" aria-label="Labeled notability badge segments">
    <g transform="translate(148 38) scale(3.4)">
      <circle cx="18" cy="18" r="13.66" class="badge-outline" />
      <path class="badge-n1" d="M12.78,28.04 A11.32,11.32 0 0,1 12.78,7.96 Z" />
      <path class="badge-n2a" d="M14.1,7.38 A11.32,11.32 0 0,1 21.9,7.38 L21.9,17.28 L14.1,17.28 Z" />
      <path class="badge-n2b" d="M14.1,28.62 A11.32,11.32 0 0,0 21.9,28.62 L21.9,18.72 L14.1,18.72 Z" />
      <path class="badge-n3" d="M23.22,28.04 A11.32,11.32 0 0,0 23.22,7.96 Z" />
    </g>
    <line class="connector" x1="138" y1="60" x2="170" y2="70" />
    <line class="connector" x1="138" y1="100" x2="180" y2="100" />
    <line class="connector" x1="208" y1="80" x2="280" y2="60" />
    <line class="connector" x1="208" y1="120" x2="280" y2="140" />
    <line class="connector" x1="238" y1="100" x2="280" y2="100" />
    <text x="133" y="63" text-anchor="end" class="label">Overall</text>
    <text x="133" y="103" text-anchor="end" class="label">N1 sitelinks</text>
    <text x="285" y="63" class="label">N2a identifiers</text>
    <text x="285" y="143" class="label">N2b sources</text>
    <text x="285" y="103" class="label">N3 structural need</text>
  </svg>
</figure>

## Examples

The example gallery is rendered from the API so the doc stays aligned with the live badge SVG:

- [Example metadata](api/badge-examples)
- `GET /api/badge-examples/{id}.svg`

<style>
  .badge-gallery {
    display: grid;
    gap: 1rem;
    grid-template-columns: repeat(auto-fit, minmax(11rem, 1fr));
    margin: 1.25rem 0;
  }
  .badge-example {
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: .85rem;
    background: var(--panel, transparent);
  }
  .badge-example figure {
    margin: 0;
    display: grid;
    gap: .5rem;
    justify-items: center;
    text-align: center;
  }
  .badge-example img {
    width: 9rem;
    height: 9rem;
  }
  .badge-example figcaption {
    font-size: .92rem;
  }
  .badge-example .label {
    font-weight: 700;
    display: block;
  }
</style>

<div class="badge-gallery">
  <div class="badge-example">
    <figure>
      <a href="api/badge-examples/strong.svg">
        <img src="api/badge-examples/strong.svg" alt="Strong overall badge example" />
      </a>
      <figcaption><span class="label">Strong</span>All of N1, N2a, N2b, and N3 are strong.</figcaption>
    </figure>
  </div>
  <div class="badge-example">
    <figure>
      <a href="api/badge-examples/n3-partial-weak.svg">
        <img src="api/badge-examples/n3-partial-weak.svg" alt="N3 partial weak badge example" />
      </a>
      <figcaption><span class="label">N3 partial weak</span>N3 is partial-weak, while N1 and N2 are none, so the outer ring is also partial-weak.</figcaption>
    </figure>
  </div>
  <div class="badge-example">
    <figure>
      <a href="api/badge-examples/n3-partial-strong.svg">
        <img src="api/badge-examples/n3-partial-strong.svg" alt="N3 partial strong badge example" />
      </a>
      <figcaption><span class="label">N3 partial strong</span>N3 is partial-strong, while N1 and N2 are none, so the outer ring is also partial-strong.</figcaption>
    </figure>
  </div>
  <div class="badge-example">
    <figure>
      <a href="api/badge-examples/partial-strong.svg">
        <img src="api/badge-examples/partial-strong.svg" alt="Partial strong badge example" />
      </a>
      <figcaption><span class="label">Partial strong</span>N2b is strong, while N1, N2a, and N3 are none, so the overall result is partial-strong.</figcaption>
    </figure>
  </div>
  <div class="badge-example">
    <figure>
      <a href="api/badge-examples/partial-weak.svg">
        <img src="api/badge-examples/partial-weak.svg" alt="Partial weak badge example" />
      </a>
      <figcaption><span class="label">Partial weak</span>N2a is weak, while N1, N2b, and N3 are none, so the overall result is partial-weak</figcaption>
    </figure>
  </div>
  <div class="badge-example">
    <figure>
      <a href="api/badge-examples/weak.svg">
        <img src="api/badge-examples/weak.svg" alt="Weak overall badge example" />
      </a>
      <figcaption><span class="label">Weak</span>The item has weak sitelinks (N1), so the overall state is weak.</figcaption>
    </figure>
  </div>
  <div class="badge-example">
    <figure>
      <a href="api/badge-examples/empty.svg">
        <img src="api/badge-examples/empty.svg" alt="Empty badge example" />
      </a>
      <figcaption><span class="label">Empty</span>When there are no claims, N2a and N2b are none, but are shown as empty.</figcaption>
    </figure>
  </div>
  <div class="badge-example">
    <figure>
      <a href="api/badge-examples/unknown.svg">
        <img src="api/badge-examples/unknown.svg" alt="UNKNOWN / PENDING badge example" />
      </a>
      <figcaption><span class="label">UNKNOWN / PENDING</span>Nothing has been evaluated yet.</figcaption>
    </figure>
  </div>
  <div class="badge-example">
    <figure>
      <a href="api/badge-examples/n3-unknown.svg">
        <img src="api/badge-examples/n3-unknown.svg" alt="N3 UNKNOWN / PENDING badge example" />
      </a>
      <figcaption><span class="label">N3 UNKNOWN / PENDING</span>N1 and N2 are weak, but N3 has not been evaluated yet, so the overall result is UNKNOWN / PENDING.</figcaption>
    </figure>
  </div>
  <div class="badge-example">
    <figure>
      <a href="api/badge-examples/redirect.svg">
        <img src="api/badge-examples/redirect.svg" alt="Redirect badge example" />
      </a>
      <figcaption><span class="label">Redirect</span>Redirects have additional purple arrows; N1 and N2 are from redirect target; N3 is from the original item.</figcaption>
    </figure>
  </div>
  <div class="badge-example">
    <figure>
      <a href="api/badge-examples/deleted.svg">
        <img src="api/badge-examples/deleted.svg" alt="Deleted badge example" />
      </a>
      <figcaption><span class="label">Deleted</span>The badge is replaced by a red X.</figcaption>
    </figure>
  </div>
</div>
