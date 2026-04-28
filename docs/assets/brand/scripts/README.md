# Brand Asset Generation Scripts

Scripts to regenerate brand assets when the logo or branding changes.

## Workflow

1. **Regenerate SVGs** (embeds logo as base64 data URI):

   ```bash
   python generate_svgs.py
   ```

2. **Regenerate PNGs** (uses Playwright for pixel-perfect rendering):

   ```bash
   python render_pngs.py
   ```

   This script:
   - Starts local HTTP servers to serve SVG and HTML assets
   - Generates HTML templates via `generate_card_html.py`
   - Renders each page at the correct viewport dimensions using headless Chromium

## Prerequisites

- Python 3.11+
- Playwright: `pip install playwright && playwright install chromium`

## Why Playwright?

The logo SVG uses deeply nested SVG elements with complex coordinate transforms
(Inkscape-generated). This rules out two common approaches:

- **ImageMagick** (`convert`): mispositions elements in nested SVG structures —
  the bouncer character renders offset from its correct position.
- **Chrome CLI** (`google-chrome --headless --screenshot`): the `--window-size`
  flag does not map exactly to the CSS viewport at small sizes, causing logo PNGs
  to be clipped at the bottom.

Playwright's `setViewportSize` gives exact pixel control over the rendering
viewport, producing results identical to what a browser displays.
