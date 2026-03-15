"""Command-line entrypoint for the local Premise UI service."""

from __future__ import annotations

import argparse
import threading
import time
from urllib import error, request
import webbrowser

BROWSER_OPEN_TIMEOUT_SECONDS = 30.0
BROWSER_OPEN_POLL_INTERVAL_SECONDS = 0.25


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Launch the local Premise UI.")
    parser.add_argument("--host", default="127.0.0.1", help="Bind address.")
    parser.add_argument("--port", default=8765, type=int, help="Bind port.")
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Do not open a browser window automatically.",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable code reload for local UI development.",
    )
    return parser


def _open_browser(url: str) -> None:
    webbrowser.open(url, new=2)


def _browser_url(host: str, port: int) -> str:
    browser_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host
    return f"http://{browser_host}:{port}/"


def _wait_for_http_ready(
    url: str,
    *,
    timeout_seconds: float = BROWSER_OPEN_TIMEOUT_SECONDS,
    poll_interval_seconds: float = BROWSER_OPEN_POLL_INTERVAL_SECONDS,
) -> bool:
    deadline = time.monotonic() + timeout_seconds
    health_url = f"{url.rstrip('/')}/api/health"

    while time.monotonic() < deadline:
        try:
            with request.urlopen(health_url, timeout=1.0) as response:
                if 200 <= response.status < 300:
                    return True
        except (error.URLError, OSError):
            pass
        time.sleep(poll_interval_seconds)

    return False


def _open_browser_when_ready(url: str) -> None:
    if _wait_for_http_ready(url):
        _open_browser(url)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        import uvicorn
    except ImportError as exc:
        raise SystemExit(
            "Premise UI dependencies are not available in this environment. "
            "Reinstall or upgrade `premise` so the bundled UI runtime is installed."
        ) from exc

    url = _browser_url(args.host, args.port)
    if not args.no_browser:
        threading.Thread(
            target=_open_browser_when_ready,
            args=(url,),
            daemon=True,
            name="premise-ui-browser-opener",
        ).start()

    uvicorn.run(
        "premise_ui.api.app:create_app",
        factory=True,
        host=args.host,
        port=args.port,
        reload=args.reload,
    )
    return 0
