"""
YouTube Web Crawler — Apify-style page-by-page search crawling.

Instead of using the YouTube Data API for search (100 quota units per call),
this module crawls YouTube's actual search result pages via ScraperAPI,
parses the embedded `ytInitialData` JSON, and extracts video/channel data.

This reduces API quota consumption by ~90% while using ScraperAPI credits
the user already pays for.
"""

import re
import json
import time
import requests
from urllib.parse import quote_plus

from core.config import SCRAPER_API_KEY

# YouTube's public InnerTube API key (embedded in the frontend, not secret)
INNERTUBE_API_KEY = "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8"

# InnerTube client context for API requests
INNERTUBE_CONTEXT = {
    "client": {
        "clientName": "WEB",
        "clientVersion": "2.20250101.00.00",
        "hl": "en",
    }
}

# --- Search filter codes (sp parameter) ---
# These are base64-encoded protobuf values that YouTube uses for search filters.
# Each combines: content type (video) + date range + duration.

# Date-only filters (type=video implied)
_SP_FILTERS = {
    # (date_filter, video_type) -> sp code
    ("All Time", "All"):    "EgIQAQ%3D%3D",
    ("Today", "All"):       "EgQIAhAB",
    ("This Week", "All"):   "EgQIAxAB",
    ("Last Month", "All"):  "EgQIBBAB",
    ("This Year", "All"):   "EgQIBRAB",

# Date + Long videos (>20 min)
    ("All Time", "Long"):    "EgQQARgC",
    ("Today", "Long"):       "EgYIAhABGAI%3D",
    ("This Week", "Long"):   "EgYIAxABGAI%3D",
    ("Last Month", "Long"):  "EgYIBBABGAI%3D",
    ("This Year", "Long"):   "EgYIBRABGAI%3D",

    # Date + Short videos (<4 min)
    ("All Time", "Shorts"):  "EgQQARgE",
    ("Today", "Shorts"):     "EgYIAhABGAQ%3D",
    ("This Week", "Shorts"): "EgYIAxABGAQ%3D",
    ("Last Month", "Shorts"):"EgYIBBABGAQ%3D",
    ("This Year", "Shorts"): "EgYIBRABGAQ%3D",
}

# Fallback: date-only if video_type isn't recognized
_SP_DATE_ONLY = {
    "All Time":   "EgIQAQ%3D%3D",
    "Today":      "EgQIAhAB",
    "This Week":  "EgQIAxAB",
    "Last Month": "EgQIBBAB",
    "This Year":  "EgQIBRAB",
}


def _get_sp_filter(date_filter: str, video_type: str) -> str:
    """Map UI date/video type to YouTube's sp search parameter."""
    key = (date_filter, video_type)
    if key in _SP_FILTERS:
        return _SP_FILTERS[key]
    return _SP_DATE_ONLY.get(date_filter, "EgQIBRAB")


def _get_gl_code(region: str) -> str:
    """Map UI region to YouTube's gl (geolocation) parameter."""
    mapping = {"US": "US", "UK": "GB", "GB": "GB", "Both": "US"}
    return mapping.get(region, "US")


def _scraper_api_fetch(url: str, timeout: int = 35) -> str | None:
    """Fetch a URL through ScraperAPI proxy. Returns HTML text or None."""
    if not SCRAPER_API_KEY:
        return None

    api_url = (
        f"http://api.scraperapi.com"
        f"?api_key={SCRAPER_API_KEY}"
        f"&url={quote_plus(url)}"
        f"&render=false"
        f"&country_code=us"
    )
    try:
        resp = requests.get(api_url, timeout=timeout)
        if resp.status_code == 200:
            return resp.text
        return None
    except Exception:
        return None


def _parse_yt_initial_data(html: str) -> dict | None:
    """Extract the ytInitialData JSON object from YouTube page HTML."""
    # YouTube embeds this as: var ytInitialData = {...};
    patterns = [
        r'var\s+ytInitialData\s*=\s*(\{.*?\})\s*;',
        r'window\["ytInitialData"\]\s*=\s*(\{.*?\})\s*;',
    ]
    for pattern in patterns:
        match = re.search(pattern, html, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                continue

    # Fallback: try to find the JSON between script tags
    # Sometimes it's in a <script nonce="...">var ytInitialData = ...
    script_pattern = r'<script[^>]*>var\s+ytInitialData\s*=\s*(\{.*?\})\s*;</script>'
    match = re.search(script_pattern, html, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass

    return None


def _parse_view_count(text: str) -> int:
    """Convert '1.2M views', '543K views', '1,234 views' to integer."""
    if not text:
        return 0
    text = text.strip().upper().replace(",", "").replace(" VIEWS", "").replace(" VIEW", "")
    text = text.replace("NO", "0")

    multipliers = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}
    for suffix, mult in multipliers.items():
        if text.endswith(suffix):
            try:
                return int(float(text[:-1]) * mult)
            except ValueError:
                return 0
    try:
        return int(text)
    except ValueError:
        return 0


def _parse_duration_text(text: str) -> int:
    """Convert '12:34' or '1:02:34' to total seconds."""
    if not text:
        return 0
    parts = text.strip().split(":")
    try:
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        elif len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        elif len(parts) == 1:
            return int(parts[0])
    except ValueError:
        pass
    return 0


def _format_duration(seconds: int) -> str:
    """Format seconds as h:mm:ss or m:ss."""
    if seconds <= 0:
        return "0:00"
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h > 0:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


def _safe_text(obj: dict | None, key: str = "simpleText") -> str:
    """Safely extract text from YouTube's various text formats."""
    if not obj:
        return ""
    # simpleText format
    if key in obj:
        return obj[key]
    # runs format (array of text segments)
    if "runs" in obj:
        return "".join(run.get("text", "") for run in obj["runs"])
    return ""


def _extract_videos_from_data(data: dict) -> tuple[list[dict], str | None]:
    """
    Parse video/channel info from ytInitialData JSON.
    Returns (list_of_videos, continuation_token_or_None).
    """
    videos = []
    continuation_token = None

    # Navigate to search results
    contents = (
        data
        .get("contents", {})
        .get("twoColumnSearchResultsRenderer", {})
        .get("primaryContents", {})
        .get("sectionListRenderer", {})
        .get("contents", [])
    )

    for section in contents:
        # Look for continuation token
        cont_renderer = section.get("continuationItemRenderer")
        if cont_renderer:
            continuation_token = (
                cont_renderer
                .get("continuationEndpoint", {})
                .get("continuationCommand", {})
                .get("token")
            )
            continue

        # Look for video results
        item_section = section.get("itemSectionRenderer", {})
        for item in item_section.get("contents", []):
            video = item.get("videoRenderer")
            if not video:
                continue

            video_id = video.get("videoId", "")
            if not video_id:
                continue

            # Title
            title = _safe_text(video.get("title"))

            # Channel info
            channel_name = ""
            channel_id = ""
            owner = video.get("longBylineText") or video.get("ownerText") or video.get("shortBylineText")
            if owner and "runs" in owner:
                runs = owner["runs"]
                if runs:
                    channel_name = runs[0].get("text", "")
                    nav = runs[0].get("navigationEndpoint", {})
                    channel_id = nav.get("browseEndpoint", {}).get("browseId", "")

            # View count
            view_text = _safe_text(video.get("viewCountText"))
            view_count = _parse_view_count(view_text)

            # Duration
            duration_text = _safe_text(video.get("lengthText"))
            duration_seconds = _parse_duration_text(duration_text)

            # Published date (relative, e.g. "2 months ago")
            published_text = _safe_text(video.get("publishedTimeText"))

            # Description snippet
            desc_snippet = _safe_text(video.get("detailedMetadataSnippets", [{}])[0].get("snippetText") if video.get("detailedMetadataSnippets") else video.get("descriptionSnippet"))

            # Skip live streams
            badges = video.get("badges", [])
            is_live = any(
                b.get("metadataBadgeRenderer", {}).get("style") == "BADGE_STYLE_TYPE_LIVE_NOW"
                for b in badges
            )
            if is_live:
                continue

            videos.append({
                "videoId": video_id,
                "title": title,
                "channelId": channel_id,
                "channelTitle": channel_name,
                "viewCount": view_count,
                "duration": _format_duration(duration_seconds),
                "duration_seconds": duration_seconds,
                "publishedText": published_text,
                "description": desc_snippet,
            })

    return videos, continuation_token


def _extract_videos_from_continuation(data: dict) -> tuple[list[dict], str | None]:
    """
    Parse video/channel info from InnerTube continuation response.
    The structure is slightly different from the initial page.
    """
    videos = []
    continuation_token = None

    actions = data.get("onResponseReceivedCommands", [])
    for action in actions:
        items = (
            action
            .get("appendContinuationItemsAction", {})
            .get("continuationItems", [])
        )
        for item in items:
            # Continuation token for next page
            cont_renderer = item.get("continuationItemRenderer")
            if cont_renderer:
                continuation_token = (
                    cont_renderer
                    .get("continuationEndpoint", {})
                    .get("continuationCommand", {})
                    .get("token")
                )
                continue

            # Video results
            video = item.get("videoRenderer")
            if not video:
                # Could also be in an itemSectionRenderer
                section = item.get("itemSectionRenderer", {})
                for sub_item in section.get("contents", []):
                    v = sub_item.get("videoRenderer")
                    if v:
                        _process_video_renderer(v, videos)
                continue

            _process_video_renderer(video, videos)

    return videos, continuation_token


def _process_video_renderer(video: dict, videos: list):
    """Extract a single video from a videoRenderer and append to list."""
    video_id = video.get("videoId", "")
    if not video_id:
        return

    title = _safe_text(video.get("title"))

    channel_name = ""
    channel_id = ""
    owner = video.get("longBylineText") or video.get("ownerText") or video.get("shortBylineText")
    if owner and "runs" in owner:
        runs = owner["runs"]
        if runs:
            channel_name = runs[0].get("text", "")
            nav = runs[0].get("navigationEndpoint", {})
            channel_id = nav.get("browseEndpoint", {}).get("browseId", "")

    view_text = _safe_text(video.get("viewCountText"))
    view_count = _parse_view_count(view_text)

    duration_text = _safe_text(video.get("lengthText"))
    duration_seconds = _parse_duration_text(duration_text)

    published_text = _safe_text(video.get("publishedTimeText"))
    desc_snippet = _safe_text(video.get("detailedMetadataSnippets", [{}])[0].get("snippetText") if video.get("detailedMetadataSnippets") else video.get("descriptionSnippet"))

    # Skip live streams
    badges = video.get("badges", [])
    is_live = any(
        b.get("metadataBadgeRenderer", {}).get("style") == "BADGE_STYLE_TYPE_LIVE_NOW"
        for b in badges
    )
    if is_live:
        return

    videos.append({
        "videoId": video_id,
        "title": title,
        "channelId": channel_id,
        "channelTitle": channel_name,
        "viewCount": view_count,
        "duration": _format_duration(duration_seconds),
        "duration_seconds": duration_seconds,
        "publishedText": published_text,
        "description": desc_snippet,
    })


def crawl_youtube_search(
    keyword: str,
    region: str,
    date_filter: str,
    video_type: str = "All",
    continuation_token: str | None = None,
    on_log=None,
) -> tuple[list[dict], str | None]:
    """
    Crawl a page of YouTube search results.

    First call: Fetches the YouTube search page HTML via ScraperAPI, parses ytInitialData.
    Subsequent calls: Uses the continuation token with YouTube's InnerTube API.

    Returns: (list_of_video_dicts, next_continuation_token_or_None)
    """
    if continuation_token:
        # --- Subsequent pages: Use InnerTube API directly ---
        return _fetch_continuation_page(continuation_token, region, on_log)
    else:
        # --- First page: Crawl the HTML search results page ---
        return _fetch_first_page(keyword, region, date_filter, video_type, on_log)


def _fetch_first_page(
    keyword: str,
    region: str,
    date_filter: str,
    video_type: str,
    on_log=None,
) -> tuple[list[dict], str | None]:
    """Fetch the first page of YouTube search results via HTML crawling."""
    gl = _get_gl_code(region)
    sp = _get_sp_filter(date_filter, video_type)

    search_url = (
        f"https://www.youtube.com/results"
        f"?search_query={quote_plus(keyword)}"
        f"&sp={sp}"
        f"&gl={gl}"
        f"&hl=en"
        f"&persist_gl=1"
    )

    if on_log:
        on_log(f"Crawling YouTube search: '{keyword}' (region={region}, filter={date_filter}, type={video_type})")

    html = _scraper_api_fetch(search_url)
    if not html:
        if on_log:
            on_log("Failed to fetch YouTube search page via ScraperAPI.")
        return [], None

    yt_data = _parse_yt_initial_data(html)
    if not yt_data:
        if on_log:
            on_log("Failed to parse ytInitialData from YouTube HTML. Page structure may have changed.")
        return [], None

    videos, cont_token = _extract_videos_from_data(yt_data)

    if on_log:
        on_log(f"Parsed {len(videos)} videos from search page. Continuation: {'yes' if cont_token else 'no'}")

    return videos, cont_token


def _fetch_continuation_page(
    continuation_token: str,
    region: str,
    on_log=None,
) -> tuple[list[dict], str | None]:
    """Fetch subsequent pages using YouTube's InnerTube search API."""
    gl = _get_gl_code(region)

    url = f"https://www.youtube.com/youtubei/v1/search?key={INNERTUBE_API_KEY}"

    body = {
        "context": {
            "client": {
                **INNERTUBE_CONTEXT["client"],
                "gl": gl,
            }
        },
        "continuation": continuation_token,
    }

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Origin": "https://www.youtube.com",
        "Referer": "https://www.youtube.com/",
    }

    try:
        resp = requests.post(url, json=body, headers=headers, timeout=20)
        if resp.status_code != 200:
            if on_log:
                on_log(f"InnerTube API returned HTTP {resp.status_code} for continuation page.")
            return [], None

        data = resp.json()
        videos, cont_token = _extract_videos_from_continuation(data)

        if on_log:
            on_log(f"Parsed {len(videos)} videos from continuation page. More: {'yes' if cont_token else 'no'}")

        return videos, cont_token

    except Exception as e:
        if on_log:
            on_log(f"Error fetching continuation page: {str(e)[:80]}")
        return [], None
