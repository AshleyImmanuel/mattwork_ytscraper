import asyncio
import importlib
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from starlette.requests import Request
from starlette.responses import Response


@pytest.fixture()
def main_module(monkeypatch):
    monkeypatch.setenv("ENABLE_API_DOCS", "0")

    import main

    importlib.reload(main)
    main.MAX_EXTRACT_BODY_BYTES = 32
    main.jobs.clear()
    main._rate_limit_hits.clear()
    main._rate_limit_last_seen.clear()
    main._last_rate_limit_cleanup_at = 0.0
    yield main
    main.jobs.clear()
    main._rate_limit_hits.clear()
    main._rate_limit_last_seen.clear()
    main._last_rate_limit_cleanup_at = 0.0


@pytest.fixture()
def client(main_module):
    return TestClient(main_module.app)


def _job_state(logs=None, status="running"):
    now = datetime.now(timezone.utc).isoformat()
    return {
        "status": status,
        "progress": 12,
        "total": 100,
        "logs": list(logs or []),
        "startedAt": now,
        "finishedAt": None,
        "filePath": None,
        "emailsFound": 3,
        "videosSearched": 7,
        "error": None,
    }


def test_status_honors_log_offset_and_reports_next_offset(client, main_module):
    main_module.jobs["job123"] = _job_state(
        logs=["log-0", "log-1", "log-2", "log-3"],
    )

    response = client.get("/api/status/job123?logOffset=2")

    assert response.status_code == 200
    payload = response.json()
    assert payload["logs"] == ["log-2", "log-3"]
    assert payload["nextLogOffset"] == 4
    assert payload["status"] == "running"
    assert payload["emailsFound"] == 3
    assert payload["videosSearched"] == 7


def test_status_missing_job_returns_404(client):
    response = client.get("/api/status/does-not-exist")

    assert response.status_code == 404
    assert response.json() == {"error": "Job not found"}


def test_status_rate_limit_shape_is_json_and_sets_retry_after(client, main_module):
    main_module.jobs["jobrate"] = _job_state(logs=["one"])
    for rule in main_module.RATE_LIMIT_RULES:
        if rule["key"] == "status":
            rule["limit"] = 1
            break

    first = client.get("/api/status/jobrate")
    second = client.get("/api/status/jobrate")

    assert first.status_code == 200
    assert second.status_code == 429
    assert second.json() == {"error": "Rate limit exceeded. Please retry shortly."}
    assert second.headers["retry-after"].isdigit()


@pytest.mark.asyncio
async def test_extract_rejects_oversized_payload_by_content_length(main_module):
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/api/extract",
        "raw_path": b"/api/extract",
        "query_string": b"",
        "headers": [(b"content-length", b"64")],
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
    }

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    request = Request(scope, receive)
    called = False

    async def call_next(_request):
        nonlocal called
        called = True
        raise AssertionError("call_next should not run for oversized extract payloads")

    response = await main_module.rate_limit_middleware(request, call_next)

    assert not called
    assert response.status_code == 413
    assert response.body == b'{"error":"Request payload too large."}'


@pytest.mark.asyncio
async def test_extract_rejects_oversized_payload_when_streamed_body_exceeds_limit(main_module):
    chunks = iter(
        [
            {"type": "http.request", "body": b'{"keyword":"', "more_body": True},
            {"type": "http.request", "body": b"abcdefghijklmnopqrstuvwxyz", "more_body": True},
            {"type": "http.request", "body": b'"}', "more_body": False},
        ]
    )
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/api/extract",
        "raw_path": b"/api/extract",
        "query_string": b"",
        "headers": [(b"content-length", b"16"), (b"content-type", b"application/json")],
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
    }

    async def receive():
        return next(chunks)

    request = Request(scope, receive)

    async def call_next(streaming_request):
        await streaming_request.body()
        return Response(status_code=204)

    response = await main_module.rate_limit_middleware(request, call_next)

    assert response.status_code == 413
    assert response.body == b'{"error":"Request payload too large."}'


def test_cleanup_jobs_overflow_keeps_running_jobs(main_module):
    main_module.MAX_STORED_JOBS = 2
    main_module.JOB_RETENTION_SECONDS = 10**9

    main_module.jobs["running-a"] = _job_state(status="running")
    main_module.jobs["running-b"] = _job_state(status="running")
    main_module.jobs["running-c"] = _job_state(status="running")
    completed = _job_state(status="completed")
    completed["finishedAt"] = datetime.now(timezone.utc).isoformat()
    main_module.jobs["completed-a"] = completed

    main_module._cleanup_jobs()

    assert set(main_module.jobs) == {"running-a", "running-b", "running-c"}
    assert all(job["status"] == "running" for job in main_module.jobs.values())


def test_client_ip_ignores_untrusted_forwarded_headers(main_module):
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/api/status/job123",
        "raw_path": b"/api/status/job123",
        "query_string": b"",
        "headers": [
            (b"x-forwarded-for", b"198.51.100.7"),
            (b"x-real-ip", b"198.51.100.8"),
        ],
        "client": ("203.0.113.9", 4567),
        "server": ("testserver", 80),
    }

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    request = Request(scope, receive)

    assert main_module._client_ip(request) == "203.0.113.9"
