"""Generate HTML pages used by render_pngs.sh for Playwright/Chrome rendering.

These HTML pages reference the logo SVG via a local HTTP server (port 8766)
and produce pixel-perfect renders when screenshotted by a headless browser.

Usage:
    python generate_card_html.py

Called automatically by render_pngs.sh.
"""

from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
HTML_DIR = SCRIPT_DIR / "html"

TAGLINE = "Configure and enforce conventions for your dbt project"
URL_TEXT = "godatadriven.github.io/dbt-bouncer"

THEMES = {
    "light": {
        "bg": "#ffffff",
        "text": "#1a1a2e",
        "muted": "#555566",
        "accent": "#ff694a",
    },
    "dark": {
        "bg": "#1a1a2e",
        "text": "#ffffff",
        "muted": "#aaaacc",
        "accent": "#ff694a",
    },
}

VARIANTS = {
    "name": {"lines": []},
    "tagline": {"lines": ["tagline"]},
    "full": {"lines": ["tagline", "url"]},
}

WIDTH, HEIGHT = 1200, 630
LOGO_SIZE = 220
ACCENT_BAR_HEIGHT = 6


def get_layout(variant_name: str) -> dict:
    lines = VARIANTS[variant_name]["lines"]
    if not lines:
        logo_y = 120
        name_y = logo_y + LOGO_SIZE + 65
    elif len(lines) == 1:
        logo_y = 90
        name_y = logo_y + LOGO_SIZE + 60
    else:
        logo_y = 70
        name_y = logo_y + LOGO_SIZE + 55
    return {
        "logo_y": logo_y,
        "name_y": name_y,
        "tagline_y": name_y + 50,
        "url_y": name_y + 95,
    }


def build_card_html(theme_name: str, variant_name: str) -> str:
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
    <img class="logo" src="http://localhost:8766/logo.svg" />
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
    HTML_DIR.mkdir(exist_ok=True)

    # Social card HTML pages
    for theme_name in THEMES:
        for variant_name in VARIANTS:
            html = build_card_html(theme_name, variant_name)
            filename = f"social-card-{theme_name}-{variant_name}.html"
            (HTML_DIR / filename).write_text(html)
            print(f"Created {filename}")

    # Logo PNG HTML pages
    logos = [
        ("logo", "http://localhost:8766/logo.svg"),
        ("logo-icon-only", "http://localhost:8766/brand/logo-icon-only.svg"),
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
