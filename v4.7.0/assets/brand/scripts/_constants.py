"""Shared constants for brand asset generation scripts."""

TAGLINE = "Configure and enforce conventions for your dbt project"
URL_TEXT = "godatadriven.github.io/dbt-bouncer"

THEMES: dict[str, dict[str, str]] = {
    "dark": {
        "accent": "#ff694a",
        "bg": "#1a1a2e",
        "muted": "#aaaacc",
        "text": "#ffffff",
    },
    "light": {
        "accent": "#ff694a",
        "bg": "#ffffff",
        "muted": "#555566",
        "text": "#1a1a2e",
    },
}

VARIANTS: dict[str, dict[str, list[str]]] = {
    "full": {"lines": ["tagline", "url"]},
    "name": {"lines": []},
    "tagline": {"lines": ["tagline"]},
}

WIDTH, HEIGHT = 1200, 630
LOGO_SIZE = 220
ACCENT_BAR_HEIGHT = 6


def get_layout(variant_name: str) -> dict[str, int]:
    """Calculate vertical positions for a given variant."""
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
