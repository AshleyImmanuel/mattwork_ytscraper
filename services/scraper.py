"""
Email Scraper - Extracts public emails from YouTube channel About pages
using Playwright routed through ScraperAPI proxy.

Implements retry logic, request throttling, and external link scraping
as specified in the PRD.
"""
import re
import os
import socket
import threading
from urllib.parse import urlparse
from ipaddress import ip_address
from time import monotonic
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import asyncio
from config import SCRAPER_API_KEY

# Regex to find email addresses
EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
)

# Common junk emails to ignore
BLACKLIST = {
    "noreply@youtube.com", "support@google.com", "press@youtube.com",
    "example@example.com", "name@example.com", "email@example.com",
    "copyright@youtube.com", "legal@google.com", "abuse@youtube.com",
}

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


MAX_RETRIES = _env_int("SCRAPER_MAX_RETRIES", 2, minimum=1, maximum=6)
RETRY_DELAY_MS = _env_int("SCRAPER_RETRY_DELAY_MS", 2000, minimum=250, maximum=60000)
THROTTLE_MS = _env_int("SCRAPER_THROTTLE_MS", 0, minimum=0, maximum=15000)
ABOUT_TIMEOUT_MS = _env_int("SCRAPER_ABOUT_TIMEOUT_MS", 20000, minimum=5000, maximum=120000)
CHANNEL_TIMEOUT_MS = _env_int("SCRAPER_CHANNEL_TIMEOUT_MS", 15000, minimum=5000, maximum=120000)
EXTERNAL_TIMEOUT_MS = _env_int("SCRAPER_EXTERNAL_TIMEOUT_MS", 10000, minimum=3000, maximum=60000)
ABOUT_POST_LOAD_WAIT_MS = _env_int("SCRAPER_ABOUT_POST_LOAD_WAIT_MS", 2000, minimum=0, maximum=15000)
CONSENT_CLICK_TIMEOUT_MS = _env_int("SCRAPER_CONSENT_CLICK_TIMEOUT_MS", 3000, minimum=500, maximum=20000)
CONSENT_POST_CLICK_WAIT_MS = _env_int("SCRAPER_CONSENT_POST_CLICK_WAIT_MS", 2000, minimum=0, maximum=15000)
VIEW_EMAIL_CLICK_TIMEOUT_MS = _env_int("SCRAPER_VIEW_EMAIL_CLICK_TIMEOUT_MS", 3000, minimum=500, maximum=20000)
VIEW_EMAIL_POST_CLICK_WAIT_MS = _env_int("SCRAPER_VIEW_EMAIL_POST_CLICK_WAIT_MS", 2000, minimum=0, maximum=15000)
CHANNEL_POST_LOAD_WAIT_MS = _env_int("SCRAPER_CHANNEL_POST_LOAD_WAIT_MS", 1500, minimum=0, maximum=15000)
EXTERNAL_POST_LOAD_WAIT_MS = _env_int("SCRAPER_EXTERNAL_POST_LOAD_WAIT_MS", 1000, minimum=0, maximum=15000)
SCRAPER_CONCURRENCY = _env_int("SCRAPER_CONCURRENCY", 5, minimum=1, maximum=20)
SCRAPER_USER_AGENT = os.getenv(
    "SCRAPER_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
).strip()
SCRAPER_PROXY_SCHEME = os.getenv("SCRAPER_PROXY_SCHEME", "http").strip() or "http"
SCRAPER_PROXY_HOST = os.getenv("SCRAPER_PROXY_HOST", "proxy-server.scraperapi.com").strip() or "proxy-server.scraperapi.com"
SCRAPER_PROXY_PORT = _env_int("SCRAPER_PROXY_PORT", 8001, minimum=1, maximum=65535)
SCRAPER_PROXY_USERNAME = os.getenv("SCRAPER_PROXY_USERNAME", "scraperapi").strip() or "scraperapi"
DNS_RESOLVE_TIMEOUT_MS = _env_int("SCRAPER_DNS_RESOLVE_TIMEOUT_MS", 750, minimum=100, maximum=5000)
DNS_CACHE_TTL_SECONDS = _env_int("SCRAPER_DNS_CACHE_TTL_SECONDS", 300, minimum=10, maximum=3600)
DNS_FAILURE_CACHE_TTL_SECONDS = _env_int("SCRAPER_DNS_FAILURE_CACHE_TTL_SECONDS", 30, minimum=1, maximum=600)

_DNS_SAFETY_CACHE: dict[str, tuple[float, bool]] = {}
_DNS_SAFETY_CACHE_LOCK = threading.Lock()
_DNS_RESOLVER = ThreadPoolExecutor(max_workers=4, thread_name_prefix="scraper-dns")


def _format_exception(exc: Exception, max_len: int = 180) -> str:
    raw = str(exc).strip()
    if not raw:
        return type(exc).__name__
    if len(raw) > max_len:
        raw = raw[: max_len - 3] + "..."
    return f"{type(exc).__name__}: {raw}"


def _is_safe_external_url(url: str) -> bool:
    """Allow only public http(s) URLs for external-link scraping."""
    try:
        parsed = urlparse((url or "").strip())
    except Exception:
        return False

    if parsed.scheme not in {"http", "https"}:
        return False

    host = (parsed.hostname or "").strip().lower().rstrip(".")
    if not host:
        return False

    if host == "localhost" or host.endswith(".local") or host.endswith(".internal") or host.endswith(".lan"):
        return False

    try:
        addr = ip_address(host)
        return addr.is_global
    except ValueError:
        return _is_public_hostname(host)


def _is_public_hostname(host: str) -> bool:
    now = monotonic()
    with _DNS_SAFETY_CACHE_LOCK:
        cached = _DNS_SAFETY_CACHE.get(host)
        if cached and cached[0] > now:
            return cached[1]
        if cached:
            _DNS_SAFETY_CACHE.pop(host, None)

    try:
        future = _DNS_RESOLVER.submit(_resolve_host_addresses, host)
        addresses = future.result(timeout=DNS_RESOLVE_TIMEOUT_MS / 1000)
        is_safe = bool(addresses) and all(ip_address(addr).is_global for addr in addresses)
        ttl_seconds = DNS_CACHE_TTL_SECONDS if is_safe else DNS_FAILURE_CACHE_TTL_SECONDS
    except (FutureTimeoutError, OSError, ValueError):
        is_safe = False
        ttl_seconds = DNS_FAILURE_CACHE_TTL_SECONDS
        if "future" in locals():
            future.cancel()

    with _DNS_SAFETY_CACHE_LOCK:
        _DNS_SAFETY_CACHE[host] = (now + ttl_seconds, is_safe)
    return is_safe


def _resolve_host_addresses(host: str) -> set[str]:
    infos = socket.getaddrinfo(
        host,
        None,
        family=socket.AF_UNSPEC,
        type=socket.SOCK_STREAM,
    )
    return {
        sockaddr[0]
        for _, _, _, _, sockaddr in infos
        if sockaddr and sockaddr[0]
    }


def _scraper_api_proxy_url() -> str:
    """Build the ScraperAPI proxy connection string for Playwright."""
    return (
        f"{SCRAPER_PROXY_SCHEME}://{SCRAPER_PROXY_USERNAME}:{SCRAPER_API_KEY}"
        f"@{SCRAPER_PROXY_HOST}:{SCRAPER_PROXY_PORT}"
    )


def _extract_emails_from_text(text: str) -> list[str]:
    """Find all valid emails in a block of text, filtering blacklisted ones."""
    found = EMAIL_REGEX.findall(text)
    return [e for e in found if e.lower() not in BLACKLIST]


async def _try_extract_from_about(page, channel_url: str, on_log=None) -> str | None:
    """
    Navigate to a YouTube channel's About page and extract the first
    valid public email from the page content.
    """
    about_url = channel_url.rstrip("/") + "/about"

    await page.goto(about_url, wait_until="domcontentloaded", timeout=ABOUT_TIMEOUT_MS)
    await page.wait_for_timeout(ABOUT_POST_LOAD_WAIT_MS)  # let dynamic content load

    if "consent." in page.url.lower():
        if on_log: on_log(f"CAPTCHA/Consent wall detected for {channel_url}. Attempting to bypass...")
        try:
            btn = page.locator('button:has-text("Accept all"), button:has-text("Agree")')
            if await btn.count() > 0:
                await btn.first.click(timeout=CONSENT_CLICK_TIMEOUT_MS)
                await page.wait_for_timeout(CONSENT_POST_CLICK_WAIT_MS)
        except Exception:
            pass

    page_html = await page.content().lower()
    if "recaptcha" in page_html or "unusual traffic" in page_html:
        if on_log: on_log(f"WARNING: Google reCAPTCHA blocked access for {channel_url}.")
        # Yield to let it try fallback external links, but about page is definitely dead.

    # Strategy 1: Find email in visible page text
    page_text = await page.inner_text("body")
    valid = _extract_emails_from_text(page_text)
    if valid:
        return valid[0]

    # Strategy 2: Check the full page source / meta tags / links
    html = await page.content()
    valid = _extract_emails_from_text(html)
    if valid:
        return valid[0]

    # Strategy 3: Look for a "View email address" button
    try:
        btn = page.locator('button:has-text("View email")')
        if await btn.count() > 0:
            await btn.first.click(timeout=VIEW_EMAIL_CLICK_TIMEOUT_MS)
            await page.wait_for_timeout(VIEW_EMAIL_POST_CLICK_WAIT_MS)
            page_text = await page.inner_text("body")
            valid = _extract_emails_from_text(page_text)
            if valid:
                return valid[0]
    except Exception:
        pass

    return None


async def _try_extract_from_links(page, channel_url: str) -> str | None:
    """
    Navigate to the channel page and follow external links (website, social)
    to find emails on linked pages - as required by the PRD.
    """
    await page.goto(channel_url, wait_until="domcontentloaded", timeout=CHANNEL_TIMEOUT_MS)
    await page.wait_for_timeout(CHANNEL_POST_LOAD_WAIT_MS)

    # Gather all external links from the channel page
    links = await page.eval_on_selector_all(
        'a[href*="redirect"]',
        "els => els.map(el => el.href)"
    )
    # Also check direct external links
    all_links = await page.eval_on_selector_all(
        'a[href^="http"]',
        "els => els.map(el => el.href)"
    )
    links.extend(all_links)

    # Filter to only external and safe links
    external = []
    seen_links = set()
    for link in links:
        lower = link.lower()
        is_external_candidate = ("youtube.com" not in lower and "google.com" not in lower) or ("redirect" in lower)
        if not is_external_candidate:
            continue
        if link in seen_links:
            continue
        if not _is_safe_external_url(link):
            continue
        seen_links.add(link)
        external.append(link)

    # Visit up to 3 external links to look for emails
    for ext_url in external[:3]:
        try:
            await page.goto(ext_url, wait_until="domcontentloaded", timeout=EXTERNAL_TIMEOUT_MS)
            await page.wait_for_timeout(EXTERNAL_POST_LOAD_WAIT_MS)
            text = await page.inner_text("body")
            valid = _extract_emails_from_text(text)
            if valid:
                return valid[0]
        except Exception:
            continue

    return None


async def _extract_email_from_channel(page, channel_url: str, on_log=None) -> str | None:
    """
    Full email extraction pipeline for a single channel with retry logic.
    Tries About page first, then follows external links if no email found.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        had_error = False

        # Try About page first
        try:
            email = await _try_extract_from_about(page, channel_url, on_log)
            if email:
                return email
        except PlaywrightTimeoutError as exc:
            had_error = True
            if on_log:
                on_log(
                    f"Attempt {attempt}/{MAX_RETRIES} about-page timeout for {channel_url}: "
                    f"{_format_exception(exc)}"
                )
        except Exception as exc:
            had_error = True
            if on_log:
                on_log(
                    f"Attempt {attempt}/{MAX_RETRIES} about-page error for {channel_url}: "
                    f"{_format_exception(exc)}"
                )

        # Fallback: check external links from the channel page
        try:
            email = await _try_extract_from_links(page, channel_url)
            if email:
                return email
        except PlaywrightTimeoutError as exc:
            had_error = True
            if on_log:
                on_log(
                    f"Attempt {attempt}/{MAX_RETRIES} links timeout for {channel_url}: "
                    f"{_format_exception(exc)}"
                )
        except Exception as exc:
            had_error = True
            if on_log:
                on_log(
                    f"Attempt {attempt}/{MAX_RETRIES} links error for {channel_url}: "
                    f"{_format_exception(exc)}"
                )

        # No email and no hard errors means this channel likely has no public contact email.
        if not had_error:
            return None

        if attempt < MAX_RETRIES:
            await asyncio.sleep(RETRY_DELAY_MS / 1000)
        else:
            if on_log:
                on_log(f"All retries exhausted for {channel_url}. Skipping.")
            return None


async def extract_emails(results: list[dict], on_progress=None, on_log=None) -> list[dict]:
    total = len(results)
    pending_rows: list[tuple[int, dict]] = []

    for idx, row in enumerate(results):
        channel_name = row["channelName"]
        existing_email = str(row.get("EMAIL", "")).strip()
        if existing_email and existing_email.lower() != "nil":
            if on_progress:
                on_progress(idx + 1, total, channel_name, existing_email)
            continue

        desc = row.get("channelDescription", "")
        fast_check = _extract_emails_from_text(desc) if desc else []
        if fast_check:
            row["EMAIL"] = fast_check[0]
            if on_progress:
                on_progress(idx + 1, total, channel_name, fast_check[0])
            continue

        pending_rows.append((idx, row))

    if not pending_rows:
        if on_log:
            on_log("No browser scraping required; all emails resolved from metadata.")
        return results

    proxy_url = _scraper_api_proxy_url()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            proxy={"server": proxy_url} if SCRAPER_API_KEY else None,
        )

        sem = asyncio.Semaphore(SCRAPER_CONCURRENCY)
        context = await browser.new_context(
            user_agent=SCRAPER_USER_AGENT,
            viewport={"width": 1280, "height": 720},
            ignore_https_errors=True,
        )

        async def _route_non_critical(route):
            # Reduce bandwidth and page-load pressure through proxy.
            if route.request.resource_type in {"image", "media", "font"}:
                await route.abort()
                return
            await route.continue_()

        await context.route("**/*", _route_non_critical)

        async def process_channel(idx: int, row: dict):
            channel_url = row["channelUrl"]
            channel_name = row["channelName"]

            async with sem:
                if on_log:
                    on_log(f"Testing browser extraction for: {channel_name}...")
                page = await context.new_page()
                try:
                    if THROTTLE_MS > 0:
                        await page.wait_for_timeout(THROTTLE_MS)
                    email = await _extract_email_from_channel(page, channel_url, on_log)
                    row["EMAIL"] = email or "nil"
                    if on_progress:
                        on_progress(idx + 1, total, channel_name, email)
                finally:
                    await page.close()

        tasks = [process_channel(idx, row) for idx, row in pending_rows]
        task_results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, task_result in enumerate(task_results):
            if not isinstance(task_result, Exception):
                continue
            failed_idx, failed_row = pending_rows[i]
            failed_name = failed_row.get("channelName", "unknown-channel")
            failed_row["EMAIL"] = "nil"
            if on_log:
                on_log(f"Channel scrape failed for {failed_name}: {type(task_result).__name__}")
            if on_progress:
                on_progress(failed_idx + 1, total, failed_name, None)

        await context.close()
        await browser.close()

    return results
