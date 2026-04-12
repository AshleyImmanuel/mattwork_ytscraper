"""
YT LeadMiner - FastAPI Server
Serves the frontend, handles extraction jobs, and provides file downloads.
"""
import os
import sys
import asyncio

# Windows: Force ProactorEventLoop to support Playwright subprocesses inside FastAPI
# This must be set as early as possible.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import uuid
import traceback
from collections import deque
from datetime import datetime, timezone
from ipaddress import ip_address
from time import monotonic
from typing import Literal

# Force UTF-8 encoding for Windows console
if sys.platform == "win32":
    import io
    if isinstance(sys.stdout, io.TextIOWrapper):
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    if isinstance(sys.stderr, io.TextIOWrapper):
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')

from fastapi import FastAPI, BackgroundTasks, Request, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Field

from services.youtube import search_videos, get_video_details, get_channel_details, filter_results
from services.scraper import extract_emails
from services.excel import generate_excel
from googleapiclient.errors import HttpError


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, minimum: int | None = None, maximum: int | None = None) -> int:
    raw = os.getenv(name)
    value = default
    if raw is not None:
        try:
            value = int(raw.strip())
        except (TypeError, ValueError):
            value = default
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def _env_csv(name: str, default_csv: str) -> list[str]:
    raw = os.getenv(name, default_csv)
    return [item.strip() for item in raw.split(",") if item.strip()]


ENABLE_API_DOCS = _env_flag("ENABLE_API_DOCS", default=False)
TRUST_PROXY_HEADERS = _env_flag("TRUST_PROXY_HEADERS", default=False)
TRUSTED_PROXY_IPS = {
    value.strip()
    for value in os.getenv("TRUSTED_PROXY_IPS", "").split(",")
    if value.strip()
}
MAX_EXTRACT_BODY_BYTES = _env_int("MAX_EXTRACT_BODY_BYTES", 10000, minimum=1000)
RATE_LIMIT_CLEANUP_INTERVAL_SECONDS = _env_int("RATE_LIMIT_CLEANUP_INTERVAL_SECONDS", 30, minimum=5)
RATE_LIMIT_MAX_KEYS = _env_int("RATE_LIMIT_MAX_KEYS", 5000, minimum=100)
MAX_CONCURRENT_JOBS = _env_int("MAX_CONCURRENT_JOBS", 2, minimum=1)
JOB_RETENTION_SECONDS = _env_int("JOB_RETENTION_SECONDS", 21600, minimum=300)
MAX_STORED_JOBS = _env_int("MAX_STORED_JOBS", 200, minimum=20)
MAX_JOB_LOG_LINES = _env_int("MAX_JOB_LOG_LINES", 400, minimum=50)
MAX_KEYWORDS_PER_JOB = _env_int("MAX_KEYWORDS_PER_JOB", 10, minimum=1, maximum=50)
MAX_API_FETCHES = _env_int("MAX_API_FETCHES", 100, minimum=1, maximum=500)
MAX_STALE_BATCHES = _env_int("MAX_STALE_BATCHES", 8, minimum=1, maximum=100)
MIN_MATCH_TARGET_ABSOLUTE = _env_int("MIN_MATCH_TARGET_ABSOLUTE", 20, minimum=1)
MIN_MATCH_TARGET_DIVISOR = _env_int("MIN_MATCH_TARGET_DIVISOR", 10, minimum=1)
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = _env_int("APP_PORT", 8000, minimum=1, maximum=65535)
RATE_LIMIT_EXTRACT_PER_MIN = _env_int("RATE_LIMIT_EXTRACT_PER_MIN", 6, minimum=1)
RATE_LIMIT_STATUS_PER_MIN = _env_int("RATE_LIMIT_STATUS_PER_MIN", 240, minimum=10)
RATE_LIMIT_DOWNLOAD_PER_MIN = _env_int("RATE_LIMIT_DOWNLOAD_PER_MIN", 30, minimum=1)
BOTH_REGION_SEQUENCE = [r.upper() for r in _env_csv("BOTH_REGION_SEQUENCE", "US,UK")]
if not BOTH_REGION_SEQUENCE:
    BOTH_REGION_SEQUENCE = ["US", "UK"]

app = FastAPI(
    title="YT LeadMiner",
    version="2.1",
    docs_url="/docs" if ENABLE_API_DOCS else None,
    redoc_url="/redoc" if ENABLE_API_DOCS else None,
    openapi_url="/openapi.json" if ENABLE_API_DOCS else None,
)

# ---- Rate limiting ----
# In-memory, per-IP limits to reduce endpoint spam. Suitable for single-instance deployment.
RATE_LIMIT_RULES = [
    {"key": "extract", "path_prefix": "/api/extract", "limit": RATE_LIMIT_EXTRACT_PER_MIN, "window_seconds": 60},
    {"key": "status", "path_prefix": "/api/status/", "limit": RATE_LIMIT_STATUS_PER_MIN, "window_seconds": 60},
    {"key": "download", "path_prefix": "/api/download/", "limit": RATE_LIMIT_DOWNLOAD_PER_MIN, "window_seconds": 60},
]
_rate_limit_hits: dict[tuple[str, str], deque[float]] = {}
_rate_limit_last_seen: dict[tuple[str, str], float] = {}
_last_rate_limit_cleanup_at = 0.0
_rate_limit_lock = asyncio.Lock()


class _RequestBodyTooLarge(Exception):
    """Raised when an incoming extract request exceeds the configured body size."""


def _safe_ip(raw: str | None) -> str | None:
    if not raw:
        return None
    candidate = raw.strip()
    try:
        return str(ip_address(candidate))
    except ValueError:
        return None


def _is_trusted_proxy(client_host: str | None) -> bool:
    if not TRUST_PROXY_HEADERS:
        return False
    parsed = _safe_ip(client_host)
    if not parsed:
        return False
    if not TRUSTED_PROXY_IPS:
        return True
    return parsed in TRUSTED_PROXY_IPS


def _client_ip(request: Request) -> str:
    client_host = request.client.host if request.client else None
    forwarded = request.headers.get("x-forwarded-for")
    real_ip_header = request.headers.get("x-real-ip")

    if _is_trusted_proxy(client_host):
        if forwarded:
            forwarded_ip = _safe_ip(forwarded.split(",")[0])
            if forwarded_ip:
                return forwarded_ip
        real_ip = _safe_ip(real_ip_header)
        if real_ip:
            return real_ip

    parsed_client = _safe_ip(client_host)
    if parsed_client:
        return parsed_client
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def _match_rate_limit_rule(path: str):
    for rule in RATE_LIMIT_RULES:
        prefix = rule["path_prefix"]
        if path == prefix or path.startswith(prefix):
            return rule
    return None


def _apply_security_headers(response: Response, path: str) -> Response:
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    response.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
    response.headers.setdefault("Cross-Origin-Opener-Policy", "same-origin")
    response.headers.setdefault("Cross-Origin-Resource-Policy", "same-origin")
    response.headers.setdefault("Content-Security-Policy", "default-src 'self'; base-uri 'self'; frame-ancestors 'none'; object-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src 'self' https://fonts.gstatic.com; img-src 'self' data: https:; connect-src 'self'")
    if path.startswith("/api/"):
        response.headers.setdefault("Cache-Control", "no-store")
        response.headers.setdefault("Pragma", "no-cache")
    return response


def _request_payload_too_large(path: str) -> Response:
    response = JSONResponse(
        status_code=413,
        content={"error": "Request payload too large."},
    )
    return _apply_security_headers(response, path)


def _enforce_request_body_limit(request: Request, limit_bytes: int) -> None:
    original_receive = request._receive
    received_bytes = 0

    async def limited_receive():
        nonlocal received_bytes
        message = await original_receive()
        if message["type"] != "http.request":
            return message

        received_bytes += len(message.get("body", b""))
        if received_bytes > limit_bytes:
            raise _RequestBodyTooLarge
        return message

    request._receive = limited_receive


def _prune_rate_limit_state(now: float):
    global _last_rate_limit_cleanup_at
    if (now - _last_rate_limit_cleanup_at) < RATE_LIMIT_CLEANUP_INTERVAL_SECONDS:
        return
    _last_rate_limit_cleanup_at = now

    max_window = max(int(rule["window_seconds"]) for rule in RATE_LIMIT_RULES)
    stale_cutoff = now - max_window

    for key, hits in list(_rate_limit_hits.items()):
        while hits and hits[0] <= stale_cutoff:
            hits.popleft()
        if not hits:
            _rate_limit_hits.pop(key, None)
            _rate_limit_last_seen.pop(key, None)

    if len(_rate_limit_hits) > RATE_LIMIT_MAX_KEYS:
        overflow = len(_rate_limit_hits) - RATE_LIMIT_MAX_KEYS
        oldest_keys = sorted(
            _rate_limit_last_seen.items(),
            key=lambda item: item[1],
        )[:overflow]
        for old_key, _ in oldest_keys:
            _rate_limit_hits.pop(old_key, None)
            _rate_limit_last_seen.pop(old_key, None)


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    path = request.url.path

    if path == "/api/extract" and request.method == "POST":
        content_length = request.headers.get("content-length")
        if content_length and content_length.isdigit() and int(content_length) > MAX_EXTRACT_BODY_BYTES:
            return _request_payload_too_large(path)
        _enforce_request_body_limit(request, MAX_EXTRACT_BODY_BYTES)

    if request.method == "OPTIONS":
        response = await call_next(request)
        return _apply_security_headers(response, path)

    rule = _match_rate_limit_rule(path)
    if not rule:
        response = await call_next(request)
        return _apply_security_headers(response, path)

    now = monotonic()
    client_ip = _client_ip(request)
    key = (client_ip, rule["key"])
    limit = int(rule["limit"])
    window_seconds = int(rule["window_seconds"])

    retry_after: int | None = None

    async with _rate_limit_lock:
        _prune_rate_limit_state(now)

        if key not in _rate_limit_hits and len(_rate_limit_hits) >= RATE_LIMIT_MAX_KEYS:
            retry_after = 5
        else:
            hits = _rate_limit_hits.setdefault(key, deque())
            _rate_limit_last_seen[key] = now
            window_start = now - window_seconds

            while hits and hits[0] <= window_start:
                hits.popleft()

            if len(hits) >= limit:
                oldest = hits[0]
                retry_after = max(1, int(window_seconds - (now - oldest)) + 1)
            else:
                hits.append(now)
                _rate_limit_last_seen[key] = now

            if not hits:
                _rate_limit_hits.pop(key, None)
                _rate_limit_last_seen.pop(key, None)

    if retry_after is not None:
        response = JSONResponse(
            status_code=429,
            content={"error": "Rate limit exceeded. Please retry shortly."},
            headers={"Retry-After": str(retry_after)},
        )
        return _apply_security_headers(response, path)

    try:
        response = await call_next(request)
    except _RequestBodyTooLarge:
        return _request_payload_too_large(path)
    return _apply_security_headers(response, path)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Log detailed 422 errors to console for debugging."""
    print(f"\n[422] Validation Error at {request.url.path}")
    print(f"Details: {exc.errors()}")
    print(f"Body: {await request.body()}\n")
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors(), "error": "Validation failed. Check your inputs."},
    )

# ---- Serve frontend ----
FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "frontend")
app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")


@app.get("/")
async def serve_index():
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))


# ---- Job State ----
jobs: dict[str, dict] = {}


class ExtractionRequest(BaseModel):
    keyword: str = Field(min_length=1, max_length=1000)
    minViews: int = Field(default=0, ge=0)
    maxViews: int = Field(default=0, ge=0)  # 0 = no upper limit
    minSubs: int = Field(default=0, ge=0)
    maxSubs: int = Field(default=0, ge=0)   # 0 = no upper limit
    region: Literal["Both", "US", "UK"] = "Both"
    dateFilter: Literal["Today", "This Week", "Last Month", "This Year"] = "This Year"
    videoType: Literal["All", "Shorts", "Long"] = "All"
    searchPoolSize: int = Field(default=500, ge=50, le=5000)


def _cleanup_jobs():
    now = datetime.now(timezone.utc)
    stale_ids = []

    for job_id, job in jobs.items():
        if job.get("status") not in {"completed", "failed"}:
            continue
        timestamp = job.get("finishedAt") or job.get("startedAt")
        try:
            ts = datetime.fromisoformat(timestamp)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        except Exception:
            continue
        if (now - ts).total_seconds() > JOB_RETENTION_SECONDS:
            stale_ids.append(job_id)

    for job_id in stale_ids:
        jobs.pop(job_id, None)

    if len(jobs) <= MAX_STORED_JOBS:
        return

    overflow = len(jobs) - MAX_STORED_JOBS
    evictable_jobs = [
        (job_id, job)
        for job_id, job in jobs.items()
        if job.get("status") in {"completed", "failed"}
    ]
    ordered_jobs = sorted(
        evictable_jobs,
        key=lambda item: item[1].get("finishedAt") or item[1].get("startedAt") or "",
    )
    for old_job_id, _ in ordered_jobs[:overflow]:
        jobs.pop(old_job_id, None)


# ---- API Endpoints ----

@app.post("/api/extract")
async def start_extraction(req: ExtractionRequest, background_tasks: BackgroundTasks):
    """Start an extraction job in the background."""
    _cleanup_jobs()

    running_jobs = sum(1 for item in jobs.values() if item.get("status") == "running")
    if running_jobs >= MAX_CONCURRENT_JOBS:
        return JSONResponse(
            status_code=429,
            content={"error": "Too many running extraction jobs. Please try again shortly."},
        )

    job_id = str(uuid.uuid4())[:8]
    jobs[job_id] = {
        "status": "running",
        "progress": 0,
        "total": 0,
        "logs": ["Job created - starting extraction pipeline..."],
        "startedAt": datetime.now(timezone.utc).isoformat(),
        "finishedAt": None,
        "filePath": None,
        "emailsFound": 0,
        "videosSearched": 0,
        "error": None,
    }

    background_tasks.add_task(run_extraction, job_id, req)
    return {"jobId": job_id, "status": "running"}


@app.get("/api/status/{job_id}")
async def job_status(job_id: str, logOffset: int = Query(default=0, ge=0)):
    """Poll the current status of a running job."""
    _cleanup_jobs()
    job = jobs.get(job_id)
    if not job:
        return JSONResponse(status_code=404, content={"error": "Job not found"})

    all_logs = job["logs"]
    logs = all_logs[logOffset:] if logOffset else all_logs
    return {
        "status": job["status"],
        "progress": job["progress"],
        "total": job["total"],
        "logs": logs,
        "nextLogOffset": len(all_logs),
        "emailsFound": job["emailsFound"],
        "videosSearched": job["videosSearched"],
        "error": job["error"],
    }


@app.get("/api/download/{job_id}")
async def download_file(job_id: str):
    """Download the generated Excel file for a completed job."""
    _cleanup_jobs()
    job = jobs.get(job_id)
    if not job:
        return JSONResponse(status_code=404, content={"error": "Job not found"})
    if job["status"] != "completed":
        return JSONResponse(status_code=400, content={"error": "Job not completed yet"})
    if not job["filePath"] or not os.path.exists(job["filePath"]):
        return JSONResponse(status_code=404, content={"error": "File not found"})

    filepath = job["filePath"]
    filename = os.path.basename(filepath)
    return FileResponse(
        path=filepath,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=filename,
        headers={
            "Access-Control-Expose-Headers": "Content-Disposition",
        },
    )


# ---- Background Extraction Pipeline ----

def run_extraction(job_id: str, req: ExtractionRequest):
    """Entry point for the background task, running in a dedicated thread and event loop."""
    import asyncio
    import sys
    
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_do_run_extraction(job_id, req))
    finally:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception:
            pass
        loop.close()


async def _do_run_extraction(job_id: str, req: ExtractionRequest):
    """Full extraction pipeline: Search -> Filter -> Scrape Emails -> Export Excel."""
    job = jobs[job_id]

    try:
        # Step 1-4: Loop to fetch exactly up to Search Pool Size
        keywords = [k.strip() for k in req.keyword.split(",") if k.strip()]
        if not keywords:
            keywords = ["Keyword"]
        if len(keywords) > MAX_KEYWORDS_PER_JOB:
            _log(
                job,
                f"Keyword count exceeded limit; using first {MAX_KEYWORDS_PER_JOB} values.",
            )
            keywords = keywords[:MAX_KEYWORDS_PER_JOB]

        search_regions = BOTH_REGION_SEQUENCE if req.region == "Both" else [req.region]
        search_slots = [(keyword, region) for keyword in keywords for region in search_regions]

        results = []
        page_tokens = {slot: None for slot in search_slots}
        exhausted_slots = set()
        seen_channel_ids = set()
        videos_searched = 0
        max_api_fetches = MAX_API_FETCHES  # Safeguard for API quota
        fetches = 0
        slot_idx = 0
        stale_batches = 0
        max_stale_batches = MAX_STALE_BATCHES
        min_match_target_for_early_stop = max(
            MIN_MATCH_TARGET_ABSOLUTE,
            req.searchPoolSize // MIN_MATCH_TARGET_DIVISOR,
        )

        while videos_searched < req.searchPoolSize and fetches < max_api_fetches:
            active_slots = [slot for slot in search_slots if slot not in exhausted_slots]
            if not active_slots:
                _log(job, "All keyword-region combinations exhausted.")
                break

            current_kw, current_region = active_slots[slot_idx % len(active_slots)]
            slot_idx += 1
            fetches += 1

            _log(
                job,
                f"[Batch {fetches}] Searching '{current_kw}' in {current_region}... "
                f"(Scanned {videos_searched}/{req.searchPoolSize})",
            )

            batch_videos, new_token = search_videos(
                current_kw,
                current_region,
                req.dateFilter,
                max_results=50,
                page_token=page_tokens[(current_kw, current_region)],
                video_type=req.videoType,
            )

            if not batch_videos or not new_token:
                exhausted_slots.add((current_kw, current_region))

            if not batch_videos:
                continue

            page_tokens[(current_kw, current_region)] = new_token

            # Trim the batch so we never overshoot the pool size
            remaining = req.searchPoolSize - videos_searched
            if remaining <= 0:
                _log(job, f"Search pool reached limit ({req.searchPoolSize}). Stopping.")
                break

            if len(batch_videos) > remaining:
                _log(job, f"Trimming last batch from {len(batch_videos)} to {remaining} items.")
                batch_videos = batch_videos[:remaining]

            videos_searched += len(batch_videos)
            job["videosSearched"] = videos_searched

            candidate_videos = [v for v in batch_videos if v["channelId"] not in seen_channel_ids]
            if not candidate_videos:
                stale_batches += 1
                _log(job, "Batch had only already-selected channels; skipping detail lookups.")
                if stale_batches >= max_stale_batches and len(results) >= min_match_target_for_early_stop:
                    _log(
                        job,
                        "Stopping early to protect API quota after repeated low-yield batches.",
                    )
                    break
                continue

            video_ids = [v["videoId"] for v in candidate_videos]
            video_details = get_video_details(video_ids)

            channel_ids = list(set(v["channelId"] for v in candidate_videos))
            channel_details = get_channel_details(channel_ids)

            max_views = req.maxViews if req.maxViews > 0 else None
            max_subs = req.maxSubs if req.maxSubs > 0 else None

            batch_results = filter_results(
                candidate_videos,
                video_details,
                channel_details,
                req.minViews,
                max_views,
                req.minSubs,
                max_subs,
                req.region,
                video_type=req.videoType,
            )

            new_unique = 0
            for br in batch_results:
                channel_id = br.get("channelId")
                if channel_id and channel_id not in seen_channel_ids:
                    seen_channel_ids.add(channel_id)
                    results.append(br)
                    new_unique += 1
                elif not channel_id:
                    results.append(br)
                    new_unique += 1

            if new_unique == 0:
                stale_batches += 1
            else:
                stale_batches = 0

            _log(
                job,
                f"Found {len(batch_results)} matches in this batch. "
                f"Added {new_unique} new channels. Total matches so far: {len(results)}",
            )

            if stale_batches >= max_stale_batches and len(results) >= min_match_target_for_early_stop:
                _log(
                    job,
                    "Stopping early to protect API quota after repeated low-yield batches.",
                )
                break
        _log(job, f"Search pipeline finished. Total videos scanned: {videos_searched}/{req.searchPoolSize}. Found {len(results)} potential matches.")

        if not results:
            _log(job, "No channels matched your filter criteria.")
            filepath = generate_excel([], req.keyword)
            job["filePath"] = filepath
            job["status"] = "completed"
            job["finishedAt"] = datetime.now(timezone.utc).isoformat()
            return

        # Step 5: Scrape emails
        job["total"] = len(results)
        _log(job, f"Launching Playwright browser - scraping emails from {len(results)} channels...")

        def on_progress(current, total, name, email):
            job["progress"] = current
            status = f"found: {email}" if email else "no public email"
            _log(job, f"  [{current}/{total}] {name} - {status}")
            if email:
                job["emailsFound"] += 1

        def on_log(message: str):
            _log(job, f"  [scraper] {message}")

        # Run the massive concurrent Async Playwright pipeline
        results = await extract_emails(results, on_progress, on_log)
        _log(job, f"Email extraction complete - {job['emailsFound']} emails found.")

        # Step 5b: Final Deduplication (Ensure unique channels)
        unique_results = {}
        for r in results:
            cid = r.get("channelId")
            if cid and cid not in unique_results:
                unique_results[cid] = r
            elif not cid:
                unique_results[f"no-id-{uuid.uuid4()}"] = r
        results = list(unique_results.values())

        # Step 6: Export to Excel
        _log(job, "Generating Excel file...")
        filepath = generate_excel(results, req.keyword)
        job["filePath"] = filepath
        _log(job, f"[OK] Export complete: {os.path.basename(filepath)}")

        job["status"] = "completed"
        job["finishedAt"] = datetime.now(timezone.utc).isoformat()

    except HttpError as e:
        job["status"] = "failed"
        job["finishedAt"] = datetime.now(timezone.utc).isoformat()
        
        # Check for quota error
        import json
        try:
            err_data = json.loads(e.content.decode("utf-8"))
            reason = err_data.get("error", {}).get("errors", [{}])[0].get("reason")
        except Exception:
            reason = None

        if reason == "quotaExceeded":
            job["error"] = "YouTube API Quota Exceeded. Please wait for reset or use a different key."
            _log(job, "[ERR] YouTube API quota exceeded (10,000 unit limit reached).")
        else:
            job["error"] = f"YouTube API Error: {type(e).__name__}"
            _log(job, f"[ERR] YouTube API HttpError: {e}")

    except Exception as e:
        print("\n" + "="*50)
        print(f"CRITICAL ERROR in job {job_id}")
        traceback.print_exc()
        print("="*50 + "\n")
        
        job["status"] = "failed"
        job["error"] = "Extraction failed due to an internal error."
        job["finishedAt"] = datetime.now(timezone.utc).isoformat()
        _log(job, f"[ERR] Error: {type(e).__name__}: {e}")


def _log(job: dict, message: str):
    """Append a timestamped log entry to the job."""
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    job["logs"].append(f"[{ts}] {message}")
    if len(job["logs"]) > MAX_JOB_LOG_LINES:
        del job["logs"][: len(job["logs"]) - MAX_JOB_LOG_LINES]
    try:
        print(f"[job] {message}")
    except UnicodeEncodeError:
        # Fallback for environments that still can't handle UTF-8
        print(f"[job] {message.encode('ascii', errors='replace').decode('ascii')}")


# ---- Run ----
if __name__ == "__main__":
    import uvicorn

    print(f"INFO: Starting server on {APP_HOST}:{APP_PORT} (policy: {type(asyncio.get_event_loop_policy()).__name__})")
    uvicorn.run(
        "main:app",
        host=APP_HOST,
        port=APP_PORT,
        reload=_env_flag("UVICORN_RELOAD", default=False),
        loop="asyncio"
    )
