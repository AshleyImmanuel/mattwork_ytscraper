import os
from dotenv import load_dotenv

# Load .env file at the root
# Since this file is in core/, we need to look up one level
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

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

def _env_csv(name: str, default_csv: str, convert_to_upper: bool = False) -> list[str]:
    raw = os.getenv(name, default_csv)
    items = [item.strip() for item in raw.split(",") if item.strip()]
    if convert_to_upper:
        return [item.upper() for item in items]
    return items

def _env_csv_set(name: str, default_csv: str) -> set[str]:
    raw = os.getenv(name, default_csv)
    return {item.strip().lower() for item in raw.split(",") if item.strip()}

# ---- App Settings ----
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = _env_int("APP_PORT", 8000, minimum=1, maximum=65535)
ENABLE_API_DOCS = _env_flag("ENABLE_API_DOCS", default=False)
TRUST_PROXY_HEADERS = _env_flag("TRUST_PROXY_HEADERS", default=False)
TRUSTED_PROXY_IPS = {
    value.strip()
    for value in os.getenv("TRUSTED_PROXY_IPS", "").split(",")
    if value.strip()
}

# ---- YouTube API Settings ----
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")

YOUTUBE_DEFAULT_REGION = os.getenv("YOUTUBE_DEFAULT_REGION", "US").strip().upper() or "US"
YOUTUBE_RELEVANCE_LANGUAGE = os.getenv("YOUTUBE_RELEVANCE_LANGUAGE", "en").strip() or "en"

# Exclusion Keywords (For filtering edits, montages, gaming, music, AI, tutorials, and aesthetic junk)
YOUTUBE_EXCLUSION_KEYWORDS = _env_csv(
    "YOUTUBE_EXCLUSION_KEYWORDS", 
    "montage,edit,fanedit,tribute,status,alight motion,capcut,velocity,lyrics,ffx,"
    "gaming,gameplay,playthrough,walkthrough,"
    "music video,official video,song,mv,remix,mix,ncs,non-copyright,audio,vocal,instrumental,"
    "ai,stable diffusion,midjourney,dalle,"
    "tutorial,how to,course,lesson,"
    "aesthetic,vibe,mood,slowed,reverb,lofi,nightcore,relaxing,bass boosted,"
    "tiktok,reels,shorts,compilation,reaction,instagram,"
    "socks,feet,cosplay,best of,fan page,fan account,re-edit,repurposed,clips,moments,highlights",
    convert_to_upper=True
)

# Unbypassable language bugs (These are checked unconditionally)
YOUTUBE_STRICT_EXCLUSIONS = _env_csv(
    "YOUTUBE_STRICT_EXCLUSIONS",
    "hindi,urdu,telugu,tamil,dub,sub,aur",
    convert_to_upper=True
)

# Channel-specific exclusions (to avoid fan-run clip channels or repurposers)
YOUTUBE_CHANNEL_EXCLUSION_KEYWORDS = _env_csv(
    "YOUTUBE_CHANNEL_EXCLUSION_KEYWORDS",
    "CLIPS,HIGHLIGHTS,MOMENTS,THE VAULT,VAULT,BEST OF,FAN PAGE,FAN ACCOUNT,RE-EDIT,REPURPOSED,SHORTS",
    convert_to_upper=True
)

# Priority Keywords (bypass strict exclusions if matched)
YOUTUBE_PRIORITY_KEYWORDS = _env_csv(
    "YOUTUBE_PRIORITY_KEYWORDS",
    "OFFICIAL,CHANNEL,BUSINESS,CONTACT,MASTERCLASS,DOCUMENTARY",
    convert_to_upper=True
)

# Authority Keywords (for triggering stricter duration requirements)
YOUTUBE_AUTHORITY_KEYWORDS = _env_csv(
    "YOUTUBE_AUTHORITY_KEYWORDS",
    "DOCUMENTARY,MASTERCLASS,LECTURE,CONFERENCE,KEYNOTE",
    convert_to_upper=True
)

# Duration Thresholds (Seconds)
YOUTUBE_AUTHORITY_MIN_DURATION = _env_int("YOUTUBE_AUTHORITY_MIN_DURATION", 600, minimum=0)
YOUTUBE_LONG_MIN_DURATION = _env_int("YOUTUBE_LONG_MIN_DURATION", 60, minimum=0)

# Region/Country Mapping
# Format: KEY:VALUE,KEY:VALUE
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

YOUTUBE_REGION_MAP = _env_region_map("YOUTUBE_REGION_MAP", {"US": "US", "UK": "GB", "GB": "GB"})

ALLOWED_COUNTRIES_BOTH = _env_csv("YOUTUBE_ALLOWED_COUNTRIES_BOTH", "US,GB", convert_to_upper=True)
ALLOWED_COUNTRIES_US = _env_csv("YOUTUBE_ALLOWED_COUNTRIES_US", "US", convert_to_upper=True)
ALLOWED_COUNTRIES_UK = _env_csv("YOUTUBE_ALLOWED_COUNTRIES_UK", "GB", convert_to_upper=True)

# ---- Scraper Settings ----
SCRAPER_API_KEY = os.getenv("SCRAPER_API_KEY", "")
SCRAPER_EMAIL_BLACKLIST = _env_csv_set(
    "SCRAPER_EMAIL_BLACKLIST",
    "noreply@youtube.com,support@google.com,press@youtube.com,example@example.com,name@example.com,email@example.com,copyright@youtube.com,legal@google.com,abuse@youtube.com"
)

# Email Extraction Utility Settings
SCRAPER_PROSE_TLDS = _env_csv_set(
    "SCRAPER_PROSE_TLDS",
    "this,that,some,every,now,here,there,all,the,with,from,for,only,well,more,like,just,very"
)
SCRAPER_JUNK_INDICATORS = _env_csv(
    "SCRAPER_JUNK_INDICATORS",
    "window.,loc@ion,p@reon,patreon.com,substack.com,instagram.com,facebook.com,twitter.com,linkedin.com,tiktok.com,href=,location.,javascript:,onclick,onload,.location,.href,ion.href,script,document.,@media,@font-face,@charset,@import,"
    ".png,.jpg,.jpeg,.webp,.gif,.svg,.bmp,.ico,.png?,.jpg?,.jpeg?,.webp?,.gif?,"
    "assets/,/static/,/images/,/thumbs/,/thumbnails/,/v/,/c/,/u/,?v=,=http,http:,https:",
    convert_to_upper=False
)

# ---- Timeouts & Wait Times ----
SCRAPER_THROTTLE_MS = _env_int("SCRAPER_THROTTLE_MS", 0, minimum=0, maximum=15000)

# ---- Extraction Job Constraints ----
MAX_EXTRACT_BODY_BYTES = _env_int("MAX_EXTRACT_BODY_BYTES", 10000, minimum=1000)
MAX_CONCURRENT_JOBS = _env_int("MAX_CONCURRENT_JOBS", 2, minimum=1)
JOB_RETENTION_SECONDS = _env_int("JOB_RETENTION_SECONDS", 21600, minimum=300)
MAX_STORED_JOBS = _env_int("MAX_STORED_JOBS", 200, minimum=20)
MAX_JOB_LOG_LINES = _env_int("MAX_JOB_LOG_LINES", 400, minimum=50)
MAX_KEYWORDS_PER_JOB = _env_int("MAX_KEYWORDS_PER_JOB", 10, minimum=1, maximum=50)
MAX_API_FETCHES = _env_int("MAX_API_FETCHES", 100, minimum=1, maximum=500)
MAX_STALE_BATCHES = _env_int("MAX_STALE_BATCHES", 8, minimum=1, maximum=100)
MIN_MATCH_TARGET_ABSOLUTE = _env_int("MIN_MATCH_TARGET_ABSOLUTE", 20, minimum=1)
MIN_MATCH_TARGET_DIVISOR = _env_int("MIN_MATCH_TARGET_DIVISOR", 10, minimum=1)

# NEW: Deep Scan Settings
DEEP_SCAN_LIMIT = _env_int("DEEP_SCAN_LIMIT", 15, minimum=1, maximum=50)
RECURSIVE_EXTERNAL_SCAN = _env_flag("RECURSIVE_EXTERNAL_SCAN", default=True)
FAST_CHECK_VIDEO_COUNT = _env_int("FAST_CHECK_VIDEO_COUNT", 10, minimum=1, maximum=50)

# ---- Google Dork Discovery ----
# Uses ScraperAPI credits to search Google for YouTube channels mentioning emails.
GOOGLE_DISCOVERY_ENABLED = _env_flag("GOOGLE_DISCOVERY_ENABLED", default=False)
GOOGLE_DISCOVERY_MAX_PAGES = _env_int("GOOGLE_DISCOVERY_MAX_PAGES", 3, minimum=1, maximum=10)
GOOGLE_DISCOVERY_QUERIES = _env_csv(
    "GOOGLE_DISCOVERY_QUERIES",
    'site:youtube.com "{keyword}" "email" OR "contact" OR "business inquiries",'
    'site:youtube.com/@* "{keyword}" email',
    convert_to_upper=False
)

# New: Direct Handle Dorking for missing emails
DIRECT_DORKING_ENABLED = _env_flag("DIRECT_DORKING_ENABLED", default=True)
DIRECT_DORKING_QUERIES = _env_csv(
    "DIRECT_DORKING_QUERIES",
    'site:twitter.com OR site:instagram.com OR site:facebook.com "{name}" email,'
    '"{name}" "email" OR "contact" OR "inquiries"',
    convert_to_upper=False
)

# ---- Rate Limiting Settings ----
RATE_LIMIT_CLEANUP_INTERVAL_SECONDS = _env_int("RATE_LIMIT_CLEANUP_INTERVAL_SECONDS", 30, minimum=5)
RATE_LIMIT_MAX_KEYS = _env_int("RATE_LIMIT_MAX_KEYS", 5000, minimum=100)
RATE_LIMIT_EXTRACT_PER_MIN = _env_int("RATE_LIMIT_EXTRACT_PER_MIN", 6, minimum=1)
RATE_LIMIT_STATUS_PER_MIN = _env_int("RATE_LIMIT_STATUS_PER_MIN", 240, minimum=10)
RATE_LIMIT_DOWNLOAD_PER_MIN = _env_int("RATE_LIMIT_DOWNLOAD_PER_MIN", 30, minimum=1)

# ---- Region Logic ----
BOTH_REGION_SEQUENCE = _env_csv("BOTH_REGION_SEQUENCE", "US,UK", convert_to_upper=True)

# ---- Output Settings ----
EXCEL_OUTPUT_COLUMNS = _env_csv(
    "EXCEL_OUTPUT_COLUMNS",
    "title,id,viewCount,likes,duration,date,channelName,url,channelUrl,numberOfSubscribers,EMAIL,Country"
)
