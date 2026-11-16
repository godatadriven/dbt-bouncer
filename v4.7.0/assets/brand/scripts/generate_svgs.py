"""Generate dbt-bouncer social card SVGs.

Produces 6 SVG social cards (3 text variants x 2 colour schemes) in the
parent brand directory. The original logo is embedded as a base64 data URI
with a background-colour rect to match each card theme.

Usage:
    python generate_svgs.py

Requirements:
    - Python 3.11+
    - No external dependencies
"""

import base64
import re
from pathlib import Path

from _constants import (
    ACCENT_BAR_HEIGHT,
    HEIGHT,
    LOGO_SIZE,
    TAGLINE,
    THEMES,
    URL_TEXT,
    VARIANTS,
    WIDTH,
    get_layout,
)

SCRIPT_DIR = Path(__file__).resolve().parent
BRAND_DIR = SCRIPT_DIR.parent
LOGO_PATH = BRAND_DIR.parent / "logo.svg"


def prepare_logo_b64(bg_colour: str) -> str:
    """Read logo SVG, add background rect, return base64."""
    svg = LOGO_PATH.read_text()
    svg = re.sub(
        r"(<svg\b[^>]*>)(\s*)",
        rf'\1\2<rect width="100%" height="100%" fill="{bg_colour}" />\2',
        svg,
        count=1,
    )
    return base64.b64encode(svg.encode()).decode()


def build_svg(theme_name: str, variant_name: str, logo_b64: str) -> str:
    """Build a social card SVG."""
    theme = THEMES[theme_name]
    variant = VARIANTS[variant_name]
    layout = get_layout(variant_name)
    logo_x = (WIDTH - LOGO_SIZE) // 2

    text_elements = []
    text_elements.append(
        f'  <text x="{WIDTH // 2}" y="{layout["name_y"]}" '
        f'font-family="Arial, Helvetica, sans-serif" font-size="52" font-weight="bold" '
        f'fill="{theme["accent"]}" text-anchor="middle">dbt-bouncer</text>'
    )
    if "tagline" in variant["lines"]:
        text_elements.append(
            f'  <text x="{WIDTH // 2}" y="{layout["tagline_y"]}" '
            f'font-family="Arial, Helvetica, sans-serif" font-size="26" font-weight="normal" '
            f'fill="{theme["text"]}" text-anchor="middle">{TAGLINE}</text>'
        )
    if "url" in variant["lines"]:
        text_elements.append(
            f'  <text x="{WIDTH // 2}" y="{layout["url_y"]}" '
            f'font-family="Arial, Helvetica, sans-serif" font-size="22" font-weight="normal" '
            f'fill="{theme["muted"]}" text-anchor="middle">{URL_TEXT}</text>'
        )

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink"
     width="{WIDTH}" height="{HEIGHT}" viewBox="0 0 {WIDTH} {HEIGHT}">

  <!-- Background -->
  <rect width="{WIDTH}" height="{HEIGHT}" fill="{theme["bg"]}" />

  <!-- Accent bar -->
  <rect y="{HEIGHT - ACCENT_BAR_HEIGHT}" width="{WIDTH}" height="{ACCENT_BAR_HEIGHT}" fill="{theme["accent"]}" />

  <!-- Logo (docs/assets/logo.svg, background colour matched) -->
  <image x="{logo_x}" y="{layout["logo_y"]}" width="{LOGO_SIZE}" height="{LOGO_SIZE}"
         href="data:image/svg+xml;base64,{logo_b64}" />

  <!-- Text -->
{chr(10).join(text_elements)}

</svg>"""


def main() -> None:
    for theme_name in THEMES:
        theme = THEMES[theme_name]
        logo_b64 = prepare_logo_b64(theme["bg"])
        for variant_name in VARIANTS:
            svg = build_svg(theme_name, variant_name, logo_b64)
            filename = f"social-card-{theme_name}-{variant_name}.svg"
            (BRAND_DIR / filename).write_text(svg)
            print(f"Created {filename}")


if __name__ == "__main__":
    main()
