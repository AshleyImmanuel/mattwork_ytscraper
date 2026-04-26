"""
YouTube Web Crawler — Refactored for better maintainability.
"""

import asyncio
import requests
from urllib.parse import quote_plus

from core.config import SCRAPER_API_KEY, USE_LOCAL_BROWSER, BROWSER_TIMEOUT_MS
from services.utils.browser_manager import BrowserManager, PLAYWRIGHT_AVAILABLE
from services.crawler.parsers import parse_yt_initial_data
from services.crawler.extractors import extract_videos_from_data, extract_videos_from_continuation
from services.crawler.dom_extractor import extract_videos_from_dom

try:
    from playwright_recaptcha import recaptchav2
except ImportError:
    recaptchav2 = None

INNERTUBE_API_KEY = "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8"
INNERTUBE_CONTEXT = {
    "client": {
        "clientName": "WEB",
        "clientVersion": "2.20250101.00.00",
        "hl": "en",
    }
}

_SP_FILTERS = {
    ("Today", "All"):       "EgQIAhAB",
    ("This Week", "All"):   "EgQIAxAB",
    ("Last Month", "All"):  "EgQIBBAB",
    ("This Year", "All"):   "EgQIBRAB",
    ("Today", "Long"):       "EgYIAhABGAI=",
    ("This Week", "Long"):   "EgYIAxABGAI=",
    ("Last Month", "Long"):  "EgYIBBABGAI=",
    ("This Year", "Long"):   "EgYIBRABGAI=",
    ("Today", "Shorts"):     "EgYIAhABGAQ=",
    ("This Week", "Shorts"): "EgYIAxABGAQ=",
    ("Last Month", "Shorts"):"EgYIBBABGAQ=",
    ("This Year", "Shorts"): "EgYIBRABGAQ=",
}

_SP_DATE_ONLY = {
    "Today":      "EgQIAhAB",
    "This Week":  "EgQIAxAB",
    "Last Month": "EgQIBBAB",
    "This Year":  "EgQIBRAB",
}

def _get_sp_filter(date_filter: str, video_type: str) -> str:
    key = (date_filter, video_type)
    return _SP_FILTERS.get(key, _SP_DATE_ONLY.get(date_filter, "EgQIBRAB"))

def _get_gl_code(region: str) -> str:
    mapping = {"US": "US", "UK": "GB", "GB": "GB", "Both": "US"}
    return mapping.get(region, "US")

def _scraper_api_fetch(url: str, region: str = "US", timeout: int = 60) -> str | None:
    if not SCRAPER_API_KEY:
        return None
    country_map = {"US": "us", "UK": "gb", "GB": "gb", "Both": "us"}
    country_code = country_map.get(region, "us")
    api_url = (
        f"http://api.scraperapi.com"
        f"?api_key={SCRAPER_API_KEY}"
        f"&url={quote_plus(url)}"
        f"&render=false"
        f"&country_code={country_code}"
    )
    try:
        resp = requests.get(api_url, timeout=timeout)
        return resp.text if resp.status_code == 200 else None
    except Exception:
        return None

async def crawl_youtube_search_async(
    keyword: str,
    region: str,
    date_filter: str,
    video_type: str = "All",
    continuation_token: str | None = None,
    on_log=None,
    page=None,
) -> tuple[list[dict], str | None]:
    if USE_LOCAL_BROWSER and PLAYWRIGHT_AVAILABLE:
        return await _crawl_with_local_browser(
            keyword, region, date_filter, video_type, on_log, page=page
        )
    else:
        return await asyncio.to_thread(
            crawl_youtube_search,
            keyword, region, date_filter, video_type, continuation_token, on_log
        )

def crawl_youtube_search(
    keyword: str,
    region: str,
    date_filter: str,
    video_type: str = "All",
    continuation_token: str | None = None,
    on_log=None,
) -> tuple[list[dict], str | None]:
    if continuation_token:
        return _fetch_continuation_page(continuation_token, region, on_log)
    else:
        return _fetch_first_page(keyword, region, date_filter, video_type, on_log)

def _fetch_first_page(keyword, region, date_filter, video_type, on_log=None):
    gl = _get_gl_code(region)
    sp = _get_sp_filter(date_filter, video_type)
    search_url = f"https://www.youtube.com/results?search_query={quote_plus(keyword)}&sp={sp}&gl={gl}&hl=en&persist_gl=1"
    if on_log: on_log(f"Crawling YouTube search: '{keyword}' (region={region})")
    html = _scraper_api_fetch(search_url, region=region)
    if not html: return [], None
    yt_data = parse_yt_initial_data(html)
    if not yt_data: return [], None
    return extract_videos_from_data(yt_data)

def _fetch_continuation_page(continuation_token, region, on_log=None):
    gl = _get_gl_code(region)
    url = f"https://www.youtube.com/youtubei/v1/search?key={INNERTUBE_API_KEY}"
    body = {"context": {"client": {**INNERTUBE_CONTEXT["client"], "gl": gl}}, "continuation": continuation_token}
    headers = {"Content-Type": "application/json", "User-Agent": "Mozilla/5.0", "Origin": "https://www.youtube.com", "Referer": "https://www.youtube.com/"}
    try:
        resp = requests.post(url, json=body, headers=headers, timeout=20)
        if resp.status_code != 200: return [], None
        return extract_videos_from_continuation(resp.json())
    except Exception: return [], None

async def _crawl_with_local_browser(keyword, region, date_filter, video_type, on_log=None, page=None):
    gl = _get_gl_code(region)
    sp = _get_sp_filter(date_filter, video_type)
    search_url = f"https://www.youtube.com/results?search_query={quote_plus(keyword)}&sp={sp}&gl={gl}&hl=en&persist_gl=1"
    temp_context = None
    if not page:
        temp_context, page = await BrowserManager.get_page(region=region)
        if not page: return [], None
    try:
        await page.goto(search_url, wait_until="commit", timeout=BROWSER_TIMEOUT_MS)
        await page.wait_for_selector('ytd-video-renderer, #captcha-container, .g-recaptcha, #video-title', timeout=30000)
        if recaptchav2:
            try:
                if await page.locator('iframe[src*="recaptcha/api2/anchor"]').first.is_visible(timeout=3000):
                    async with recaptchav2.AsyncSolver(page) as solver: await solver.solve_recaptcha(wait=True)
            except: pass
        
        last_height = 0
        for _ in range(15):
            await page.evaluate("window.scrollTo(0, document.documentElement.scrollHeight)")
            await asyncio.sleep(2.5)
            new_height = await page.evaluate("document.documentElement.scrollHeight")
            if new_height == last_height: break
            last_height = new_height

        yt_data = await page.evaluate("() => typeof ytInitialData !== 'undefined' ? ytInitialData : null")
        if not yt_data:
            yt_data = parse_yt_initial_data(await page.content())
        
        videos, _ = extract_videos_from_data(yt_data) if yt_data else ([], None)
        dom_videos = await extract_videos_from_dom(page)
        seen_ids = {v["videoId"] for v in videos}
        for dv in dom_videos:
            if dv["videoId"] not in seen_ids:
                videos.append(dv)
                seen_ids.add(dv["videoId"])
        return videos, None
    except Exception as e:
        if on_log: on_log(f"Error: {e}")
        return [], None
    finally:
        if temp_context: await temp_context.close()
