import re
from services.youtube import is_strictly_rejected
from core.config import (
    YOUTUBE_EXCLUSION_KEYWORDS as EXCLUSION_KEYWORDS,
    YOUTUBE_PRIORITY_KEYWORDS as PRIORITY_KEYWORDS,
    YOUTUBE_CHANNEL_EXCLUSION_KEYWORDS as CHANNEL_EXCLUSION_KEYWORDS,
    YOUTUBE_AUTHORITY_KEYWORDS as AUTHORITY_KEYWORDS,
    YOUTUBE_AUTHORITY_MIN_DURATION as AUTHORITY_MIN_DUR,
    YOUTUBE_LONG_MIN_DURATION as LONG_MIN_DUR,
)

def pre_filter_crawled_video(
    v: dict,
    min_views: int,
    max_views: int | None,
    video_type: str,
    search_keyword: str,
) -> str | None:
    """Lightweight pre-filter using data from the web crawl (no API calls)."""
    title = v.get("title", "")
    channel_name = v.get("channelTitle", "")
    desc = v.get("description", "")
    views = v.get("viewCount", 0)
    dur_s = v.get("duration_seconds", 0)

    full_text = f"{title} {desc} {channel_name}".upper()
    channel_name_up = channel_name.upper()

    if is_strictly_rejected(title, desc, channel_name):
        return "language"

    if video_type == "Long":
        is_authority = any(kw in search_keyword.upper() for kw in AUTHORITY_KEYWORDS)
        min_dur = AUTHORITY_MIN_DUR if is_authority else LONG_MIN_DUR
        if dur_s > 0 and dur_s < min_dur:
            return "duration"
    elif video_type == "Shorts" and dur_s > 60:
        return "duration"

    if views > 0:
        if views < min_views or (max_views and views > max_views):
            return "viewCount"

    kw_upper = search_keyword.upper()
    user_kws = [word for word in kw_upper.split() if len(word) > 3]
    is_priority = any(x in full_text for x in PRIORITY_KEYWORDS) or (
        any(ukw in full_text for ukw in user_kws) if user_kws else (kw_upper in full_text)
    )

    if video_type == "Long" and not is_priority:
        if any(ckw in channel_name_up for ckw in CHANNEL_EXCLUSION_KEYWORDS):
            return "channelExclusion"

    if "SHORTS" in full_text and video_type == "Long" and not is_priority:
        return "exclusionKeyword"

    if not is_priority:
        for kw in EXCLUSION_KEYWORDS:
            kw_up = kw.upper()
            if len(kw_up) <= 4:
                if re.search(rf"\b{re.escape(kw_up)}\b", full_text):
                    return "exclusionKeyword"
            elif kw_up in full_text:
                if kw_up == "EDIT" and "CREDIT" in full_text and "EDIT" not in full_text.replace("CREDIT", ""):
                    continue
                return "exclusionKeyword"
    return None
