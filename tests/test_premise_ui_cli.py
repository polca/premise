from types import SimpleNamespace

from premise_ui import cli


def test_browser_url_uses_loopback_for_wildcard_host():
    assert cli._browser_url("0.0.0.0", 8765) == "http://127.0.0.1:8765/"
    assert cli._browser_url("::", 8765) == "http://127.0.0.1:8765/"
    assert cli._browser_url("127.0.0.1", 8765) == "http://127.0.0.1:8765/"


def test_wait_for_http_ready_retries_until_success(monkeypatch):
    calls = {"count": 0}

    class _Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def _urlopen(url, timeout):
        calls["count"] += 1
        if calls["count"] < 3:
            raise OSError("not ready")
        assert url == "http://127.0.0.1:8765/api/health"
        assert timeout == 1.0
        return _Response()

    monkeypatch.setattr(cli.request, "urlopen", _urlopen)
    monkeypatch.setattr(cli.time, "sleep", lambda seconds: None)

    assert cli._wait_for_http_ready(
        "http://127.0.0.1:8765/",
        timeout_seconds=1.0,
        poll_interval_seconds=0.01,
    )
    assert calls["count"] == 3


def test_open_browser_when_ready_skips_browser_if_server_never_comes_up(monkeypatch):
    opened = []

    monkeypatch.setattr(cli, "_wait_for_http_ready", lambda url: False)
    monkeypatch.setattr(cli, "_open_browser", lambda url: opened.append(url))

    cli._open_browser_when_ready("http://127.0.0.1:8765/")

    assert opened == []


def test_main_starts_background_browser_waiter(monkeypatch):
    thread_calls = {}
    uvicorn_calls = {}

    class _Thread:
        def __init__(self, *, target, args, daemon, name):
            thread_calls["target"] = target
            thread_calls["args"] = args
            thread_calls["daemon"] = daemon
            thread_calls["name"] = name

        def start(self):
            thread_calls["started"] = True

    monkeypatch.setattr(cli.threading, "Thread", _Thread)
    monkeypatch.setitem(
        __import__("sys").modules,
        "uvicorn",
        SimpleNamespace(
            run=lambda *args, **kwargs: uvicorn_calls.update(
                {"args": args, "kwargs": kwargs}
            )
        ),
    )

    assert cli.main([]) == 0
    assert thread_calls["target"] is cli._open_browser_when_ready
    assert thread_calls["args"] == ("http://127.0.0.1:8765/",)
    assert thread_calls["daemon"] is True
    assert thread_calls["started"] is True
    assert uvicorn_calls["kwargs"]["host"] == "127.0.0.1"
    assert uvicorn_calls["kwargs"]["port"] == 8765
