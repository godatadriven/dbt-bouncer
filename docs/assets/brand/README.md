# dbt-bouncer Brand Assets

Brand assets for dbt-bouncer — use these for talks, blog posts, social media, and documentation.

## Colour Palette

| Colour | Hex | Usage |
|---|---|---|
| Brand orange | `#ff694a` | Primary accent, project name highlight, CTA elements |
| Dark background | `#1a1a2e` | Dark card/page backgrounds |
| Logo circle fill | `#130912` | Circle background in the logo |
| Light text | `#fafafa` | Text and icon paths within the logo |
| Dark text | `#1a1a2e` | Body text on light backgrounds |
| Muted text (light) | `#555566` | Secondary text on light backgrounds (URLs, captions) |
| Muted text (dark) | `#aaaacc` | Secondary text on dark backgrounds |
| White | `#ffffff` | Light card/page backgrounds |

## Logo

| Asset | Path | Notes |
|---|---|---|
| Full logo (SVG) | [`logo.svg`](../logo.svg) | Original — includes bouncer character, "dbt-bouncer" text, and tagline |
| Icon-only logo (SVG) | [`logo-icon-only.svg`](logo-icon-only.svg) | Bouncer character in circle, no text — for small contexts |
| Full logo (PNG) | `logo-{512,256,128}.png` | Pre-rendered at standard sizes |
| Icon-only logo (PNG) | `logo-icon-only-{512,256,128}.png` | Pre-rendered at standard sizes |

## Social Cards

Six standalone social cards (3 text variants x 2 colour schemes), each in SVG + PNG.

| Variant | Light | Dark |
|---|---|---|
| Name only | `social-card-light-name.{svg,png}` | `social-card-dark-name.{svg,png}` |
| Name + tagline | `social-card-light-tagline.{svg,png}` | `social-card-dark-tagline.{svg,png}` |
| Name + tagline + URL | `social-card-light-full.{svg,png}` | `social-card-dark-full.{svg,png}` |

All cards are **1200x630px** (standard Open Graph / LinkedIn / Twitter size).

## Colour Palette Swatch

| Asset | Path | Notes |
|---|---|---|
| Colour palette (SVG) | `colour-palette.svg` | Visual swatch of all brand colours — for slides, docs, quick reference |
| Colour palette (PNG) | `colour-palette.png` | Pre-rendered at 1200x320px |

## Usage Guidelines

- Do not alter, distort, or recolour the logo.
- Maintain clear space around the logo — do not crowd it with other elements.
- Use the brand colour `#ff694a` for accents when referencing dbt-bouncer.
- Prefer the light variant for light backgrounds and the dark variant for dark backgrounds.
- When sharing on social media, use the PNG versions for maximum compatibility.
- Use the icon-only logo for favicons, avatars, and any context smaller than ~200px.

## Regenerating Assets

Scripts in [`scripts/`](scripts/) can regenerate all assets if the logo or branding changes. See [`scripts/README.md`](scripts/README.md) for details.

## Docs Site

See the [Brand page](https://godatadriven.github.io/dbt-bouncer/brand/) on the docs site for previews.
