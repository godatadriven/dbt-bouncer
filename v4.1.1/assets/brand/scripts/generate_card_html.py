"""Generate HTML pages used by render_pngs.py for Playwright rendering.

These HTML pages reference the logo SVG via a local HTTP server and produce
pixel-perfect renders when screenshotted by a headless browser.

Usage:
    python generate_card_html.py [ASSETS_PORT]

Called automatically by render_pngs.py, which passes the dynamically
assigned assets server port. Defaults to 8766 when run standalone.
"""

import sys
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
HTML_DIR = SCRIPT_DIR / "html"


def build_card_html(theme_name: str, variant_name: str, assets_port: int) -> str:
    theme = THEMES[theme_name]
    variant = VARIANTS[variant_name]
    layout = get_layout(variant_name)
    logo_x = (WIDTH - LOGO_SIZE) // 2

    text_html = []
    text_html.append(
        f'<div style="position:absolute;left:0;right:0;top:{layout["name_y"] - 40}px;'
        f"text-align:center;font-family:Arial,Helvetica,sans-serif;font-size:52px;"
        f'font-weight:bold;color:{theme["accent"]}">dbt-bouncer</div>'
    )
    if "tagline" in variant["lines"]:
        text_html.append(
            f'<div style="position:absolute;left:0;right:0;top:{layout["tagline_y"] - 20}px;'
            f"text-align:center;font-family:Arial,Helvetica,sans-serif;font-size:26px;"
            f'color:{theme["text"]}">{TAGLINE}</div>'
        )
    if "url" in variant["lines"]:
        text_html.append(
            f'<div style="position:absolute;left:0;right:0;top:{layout["url_y"] - 16}px;'
            f"text-align:center;font-family:Arial,Helvetica,sans-serif;font-size:22px;"
            f'color:{theme["muted"]}">{URL_TEXT}</div>'
        )

    return f"""<!DOCTYPE html>
<html>
<head>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ width: {WIDTH}px; height: {HEIGHT}px; overflow: hidden; }}
  .card {{ position: relative; width: {WIDTH}px; height: {HEIGHT}px; background: {theme["bg"]}; }}
  .accent {{ position: absolute; bottom: 0; left: 0; right: 0; height: {ACCENT_BAR_HEIGHT}px; background: {theme["accent"]}; }}
  .logo {{ position: absolute; left: {logo_x}px; top: {layout["logo_y"]}px; width: {LOGO_SIZE}px; height: {LOGO_SIZE}px; }}
</style>
</head>
<body>
  <div class="card">
    <img class="logo" src="http://127.0.0.1:{assets_port}/logo.svg" />
    {"".join(text_html)}
    <div class="accent"></div>
  </div>
</body>
</html>"""


def build_logo_html(logo_src: str, size: int) -> str:
    return f"""<!DOCTYPE html>
<html>
<head><style>*{{margin:0;padding:0;}}body{{width:{size}px;height:{size}px;overflow:hidden;background:transparent;}}</style></head>
<body><img src="{logo_src}" width="{size}" height="{size}" /></body>
</html>"""


def main() -> None:
    assets_port = int(sys.argv[1]) if len(sys.argv) > 1 else 8766

    HTML_DIR.mkdir(exist_ok=True)

    # Social card HTML pages
    for theme_name in THEMES:
        for variant_name in VARIANTS:
            html = build_card_html(theme_name, variant_name, assets_port)
            filename = f"social-card-{theme_name}-{variant_name}.html"
            (HTML_DIR / filename).write_text(html)
            print(f"Created {filename}")

    # Logo PNG HTML pages
    base_url = f"http://127.0.0.1:{assets_port}"
    logos = [
        ("logo", f"{base_url}/logo.svg"),
        ("logo-icon-only", f"{base_url}/brand/logo-icon-only.svg"),
    ]
    sizes = [512, 256, 128]
    for name, src in logos:
        for size in sizes:
            html = build_logo_html(src, size)
            filename = f"{name}-{size}.html"
            (HTML_DIR / filename).write_text(html)
            print(f"Created {filename}")


if __name__ == "__main__":
    main()
