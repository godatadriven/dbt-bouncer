"""Render social card PNGs and logo PNGs using Playwright.

Uses Playwright (headless Chromium) for pixel-perfect browser-based rendering.
This replaces the previous Chrome CLI approach, which clipped logo PNGs at small
viewport sizes due to ``--window-size`` not mapping exactly to the CSS viewport.

Prerequisites:
    pip install playwright
    playwright install chromium

Usage:
    cd docs/assets/brand/scripts
    python render_pngs.py
"""

import http.server
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

from playwright.sync_api import sync_playwright

SCRIPT_DIR = Path(__file__).resolve().parent
BRAND_DIR = SCRIPT_DIR.parent
ASSETS_DIR = BRAND_DIR.parent  # docs/assets/


def _make_handler(directory: Path) -> type:
    """Return a silent ``SimpleHTTPRequestHandler`` rooted at *directory*."""

    class _Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(directory), **kwargs)

        def log_message(self, format, *args):  # noqa: A002
            pass

    return _Handler


def _start_server(directory: Path) -> http.server.HTTPServer:
    """Start a background HTTP server on a free port, return the server."""
    server = http.server.HTTPServer(("127.0.0.1", 0), _make_handler(directory))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def _wait_for_server(port: int, *, retries: int = 20, delay: float = 0.25) -> None:
    """Block until the HTTP server on *port* is accepting connections."""
    for _ in range(retries):
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=1)
            return
        except OSError:
            time.sleep(delay)
    msg = f"Server on port {port} did not become ready"
    raise RuntimeError(msg)


def main() -> None:
    html_dir = SCRIPT_DIR / "html"
    html_dir.mkdir(exist_ok=True)

    # Start asset server first so we know the port for HTML generation
    print("Starting HTTP servers...")
    assets_server = _start_server(ASSETS_DIR)
    assets_port = assets_server.server_address[1]
    _wait_for_server(assets_port)

    # Generate HTML templates with the actual assets port
    subprocess.run(
        [
            sys.executable,
            str(SCRIPT_DIR / "generate_card_html.py"),
            str(assets_port),
        ],
        check=True,
    )

    # Start HTML server
    html_server = _start_server(html_dir)
    html_port = html_server.server_address[1]
    _wait_for_server(html_port)
    print(f"  Assets server on port {assets_port}, HTML server on port {html_port}")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch()

            # Render social card PNGs (1200x630)
            print("Rendering social card PNGs...")
            for html_file in sorted(html_dir.glob("social-card-*.html")):
                name = html_file.stem
                page = browser.new_page(viewport={"width": 1200, "height": 630})
                page.goto(f"http://127.0.0.1:{html_port}/{html_file.name}")
                page.screenshot(path=str(BRAND_DIR / f"{name}.png"))
                page.close()
                print(f"  Created {name}.png")

            # Render logo PNGs (square, size extracted from filename)
            print("Rendering logo PNGs...")
            for html_file in sorted(html_dir.glob("logo-*.html")):
                name = html_file.stem
                size = int(name.rsplit("-", 1)[-1])
                page = browser.new_page(viewport={"width": size, "height": size})
                page.goto(f"http://127.0.0.1:{html_port}/{html_file.name}")
                page.screenshot(path=str(BRAND_DIR / f"{name}.png"))
                page.close()
                print(f"  Created {name}.png")

            browser.close()
    finally:
        assets_server.shutdown()
        html_server.shutdown()

    print(f"Done. All PNGs written to {BRAND_DIR}")


if __name__ == "__main__":
    main()
