"""
YouTube Data API v3 — Search & Filter Service
"""
import re
import os
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from config import YOUTUBE_API_KEY


def _env_csv(name: str, default_csv: str) -> list[str]:
    raw = os.getenv(name, default_csv)
    return [item.strip().upper() for item in raw.split(",") if item.strip()]


def _env_region_map(name: str, default_map: dict[str, str]) -> dict[str, str]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default_map
    parsed: dict[str, str] = {}
    for pair in raw.split(","):
        if ":" not in pair:
            continue
        key, value = pair.split(":", 1)
        key = key.strip().upper()
        value = value.strip().upper()
        if key and value:
            parsed[key] = value
    return parsed or default_map


YOUTUBE_DEFAULT_REGION = os.getenv("YOUTUBE_DEFAULT_REGION", "US").strip().upper() or "US"
YOUTUBE_RELEVANCE_LANGUAGE = os.getenv("YOUTUBE_RELEVANCE_LANGUAGE", "en").strip() or "en"
REGION_MAP = _env_region_map("YOUTUBE_REGION_MAP", {"US": "US", "UK": "GB", "GB": "GB"})
ALLOWED_COUNTRIES_BY_REGION = {
    "Both": _env_csv("YOUTUBE_ALLOWED_COUNTRIES_BOTH", "US,GB"),
    "US": _env_csv("YOUTUBE_ALLOWED_COUNTRIES_US", "US"),
    "UK": _env_csv("YOUTUBE_ALLOWED_COUNTRIES_UK", "GB"),
}

EXCLUSION_KEYWORDS = _env_csv(
    "YOUTUBE_EXCLUSION_KEYWORDS", 
    "montage,edit,amv,gmv,phonk,tribute,status,alight motion,capcut,velocity,lyrics,ffx"
)


def _build_client():
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY)


def _normalize_region_code(region: str) -> str:
    """Map UI region values to YouTube API-compatible region codes."""
    return REGION_MAP.get((region or "").upper(), YOUTUBE_DEFAULT_REGION)


def _date_filter_to_rfc3339(date_filter: str) -> str:
    """Convert a human-friendly date filter to an RFC 3339 timestamp."""
    now = datetime.utcnow()
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_of_week = start_of_day - timedelta(days=start_of_day.weekday())
    if start_of_day.month == 1:
        start_of_last_month = start_of_day.replace(year=start_of_day.year - 1, month=12, day=1)
    else:
        start_of_last_month = start_of_day.replace(month=start_of_day.month - 1, day=1)

    mapping = {
        "Today": start_of_day,
        "This Week": start_of_week,
        "Last Month": start_of_last_month,
        "This Year": start_of_day.replace(month=1, day=1),
    }
    if date_filter not in mapping:
        raise ValueError(f"Unsupported date filter: {date_filter!r}")
    dt = mapping[date_filter]
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_duration(iso_duration: str) -> str:
    """Convert ISO 8601 duration (PT12M34S) to human readable (12:34)."""
    match = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso_duration or "")
    if not match:
        return "0:00"
    h, m, s = (int(x) if x else 0 for x in match.groups())
    if h > 0:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


def _parse_duration_seconds(iso_duration: str) -> int:
    """Convert ISO 8601 duration to total seconds."""
    match = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso_duration or "")
    if not match:
        return 0
    h, m, s = (int(x) if x else 0 for x in match.groups())
    return h * 3600 + m * 60 + s


def search_videos(keyword: str, region: str, date_filter: str, max_results: int = 50, page_token: str = None, video_type: str = "All"):
    """
    Search YouTube for videos matching the keyword.
    Returns (list_of_videos, next_page_token).

    video_type: "All", "Shorts", or "Long".
    Uses YouTube's videoDuration param as a coarse pre-filter.
    """
    client = _build_client()
    published_after = _date_filter_to_rfc3339(date_filter)

    reg = _normalize_region_code(region)

    # Pre-filter via the YouTube API's videoDuration param
    # "short" = <4 min (superset of Shorts), "long" = >20 min, "medium" = 4-20 min
    # For Shorts we use "short" as pre-filter then post-filter to ≤60s later.
    # For "Long" we skip the API param and just post-filter (to include 1-20 min videos too).
    search_params = dict(
        part="snippet",
        q=keyword,
        type="video",
        regionCode=reg,
        relevanceLanguage=YOUTUBE_RELEVANCE_LANGUAGE,
        publishedAfter=published_after,
        maxResults=min(50, max_results),
        pageToken=page_token,
        order="relevance",
    )
    if video_type == "Shorts":
        search_params["videoDuration"] = "short"  # <4 min, refined later to ≤60s

    request = client.search().list(**search_params)
    response = request.execute()
    all_video_ids = []

    for item in response.get("items", []):
        # Always exclude live streams and upcoming broadcasts
        if item.get("snippet", {}).get("liveBroadcastContent") in ("live", "upcoming"):
            continue

        all_video_ids.append({
            "videoId": item["id"]["videoId"],
            "title": item["snippet"]["title"],
            "description": item["snippet"].get("description", ""),
            "channelId": item["snippet"]["channelId"],
            "channelTitle": item["snippet"]["channelTitle"],
            "publishedAt": item["snippet"]["publishedAt"][:10],
            "region": "UK" if reg == "GB" else reg,
        })

    next_page_token = response.get("nextPageToken")
    return all_video_ids, next_page_token


def get_video_details(video_ids: list[str]):
    """
    Fetch video statistics (viewCount, likeCount, duration) for a batch of video IDs.
    Returns a dict keyed by video ID.
    Also includes duration_seconds for type filtering and isLive to catch streams.
    """
    client = _build_client()
    details = {}

    # API allows up to 50 IDs per call
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i : i + 50]
        request = client.videos().list(
            part="statistics,contentDetails,liveStreamingDetails",
            id=",".join(batch),
        )
        response = request.execute()

        for item in response.get("items", []):
            vid = item["id"]
            stats = item.get("statistics", {})
            raw_duration = item["contentDetails"].get("duration", "")
            # A video with liveStreamingDetails is or was a live stream
            is_live = "liveStreamingDetails" in item
            details[vid] = {
                "viewCount": int(stats.get("viewCount", 0)),
                "likes": int(stats.get("likeCount", 0)),
                "duration": _parse_duration(raw_duration),
                "duration_seconds": _parse_duration_seconds(raw_duration),
                "isLive": is_live,
            }

    return details


def get_channel_details(channel_ids: list[str]):
    """
    Fetch channel statistics (subscriberCount, channel URL, description) for a batch of channel IDs.
    Returns a dict keyed by channel ID.
    """
    client = _build_client()
    details = {}

    for i in range(0, len(channel_ids), 50):
        batch = channel_ids[i : i + 50]
        request = client.channels().list(
            part="statistics,snippet",
            id=",".join(batch),
        )
        response = request.execute()

        for item in response.get("items", []):
            cid = item["id"]
            stats = item.get("statistics", {})
            snippet = item.get("snippet", {})
            details[cid] = {
                "subscriberCount": int(stats.get("subscriberCount", 0)),
                "channelUrl": f"https://www.youtube.com/channel/{cid}",
                "description": snippet.get("description", ""),
                "country": snippet.get("country", ""), 
            }

    return details


def filter_results(
    videos: list[dict],
    video_details: dict,
    channel_details: dict,
    min_views: int,
    max_views: int,
    min_subs: int,
    max_subs: int,
    region_req: str,
    video_type: str = "All",
) -> list[dict]:
    """
    Merge video + channel data and apply view/subscriber range filters.
    Also filters by video type (All / Shorts / Long) and excludes live streams.
    Returns a list of enriched, filtered result dicts.
    """
    results = []
    seen_channels = set()  # deduplicate by channel

    for v in videos:
        vid = v["videoId"]
        cid = v["channelId"]

        if cid in seen_channels:
            continue

        vd = video_details.get(vid)
        cd = channel_details.get(cid)
        if not vd or not cd:
            continue

        # ── Exclude live streams (current, past, or scheduled) ──
        if vd.get("isLive"):
            continue

        # ── Video type filter ──
        dur_s = vd.get("duration_seconds", 0)
        if video_type == "Shorts" and dur_s > 60:
            continue
        if video_type == "Long" and dur_s <= 60:
            continue

        # ── Exclusion Keywords Filter (Edits, Montages, etc.) ──
        # Uses whole-word matching where possible to avoid false positives (like 'meditation' for 'edit')
        title_upper = v["title"].upper()
        desc_upper = v["description"].upper()
        found_exclusion = False
        for kw in EXCLUSION_KEYWORDS:
            # For keywords with spaces or special chars, use simple 'in' check
            # For simple words, attempt to use a word-boundary-like check for safety
            if " " in kw:
                if kw in title_upper or kw in desc_upper:
                    found_exclusion = True
                    break
            else:
                # Basic whole-word check using boundary chars
                pattern = rf"\b{re.escape(kw)}\b"
                if re.search(pattern, title_upper) or re.search(pattern, desc_upper):
                    found_exclusion = True
                    break
        
        if found_exclusion:
            # Skip videos (and thus their channels) that match exclusion keywords
            continue

        views = vd["viewCount"]
        subs = cd["subscriberCount"]

        if views < min_views or (max_views and views > max_views):
            continue
        if subs < min_subs or (max_subs and subs > max_subs):
            continue

        # STRICT COUNTRY ENFORCEMENT ("no more no less")
        allowed = ALLOWED_COUNTRIES_BY_REGION.get(region_req, ALLOWED_COUNTRIES_BY_REGION["US"])
        if cd["country"] and cd["country"] not in allowed:
            # Drop channels that explictly state they are from another country (e.g., IN)
            continue

        seen_channels.add(cid)
        results.append({
            "title": v["title"],
            "id": vid,
            "channelId": cid,
            "viewCount": views,
            "date": v["publishedAt"],
            "likes": vd["likes"],
            "duration": vd["duration"],
            "url": f"https://www.youtube.com/watch?v={vid}",
            "channelName": v["channelTitle"],
            "channelUrl": cd["channelUrl"],
            "numberOfSubscribers": subs,
            "Country": "UK" if cd.get("country") == "GB" else (cd.get("country") or v["region"]),
            "channelDescription": cd.get("description", ""),
            # email will be filled in by the scraper
            "EMAIL": "nil",
        })

    return results
