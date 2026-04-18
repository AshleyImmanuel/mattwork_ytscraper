import re
import html
import urllib.parse
from core.config import (
    SCRAPER_EMAIL_BLACKLIST as BLACKLIST,
    SCRAPER_PROSE_TLDS as PROSE_TLDS,
    SCRAPER_JUNK_INDICATORS as JUNK_INDICATORS
)

# Regex to find standard email addresses with junk prevention
# Group 1: Optional JS Keyword Prefix (used to filter out code snippets)
# Group 2: The actual email address (captured only if group 1 is NOT a standalone code keyword)
EMAIL_REGEX = re.compile(
    r"((?:\b(?:return|const|let|var|function|window|location|document|href)\b\s*)?)"+
    r"([a-zA-Z0-9.\-_%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})",
    re.IGNORECASE
)

# Regex to find mailto: links specifically
MAILTO_REGEX = re.compile(
    r"mailto:([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})",
    re.IGNORECASE
)

# Patterns for obfuscation cleaning
OBFUSCATION_PATTERNS = [
    # 1. Bracketed/Explicit Delimiters (SAFE): [at], (at), {at}, <at>, [a], (a)
    # Consume surrounding spaces to facilitate regex matching later
    (re.compile(r"\s*[\[\(\<\{\-\_\*]\s*(?:at|@|a)\s*[\]\)\>\}\-\_\*]\s*", re.IGNORECASE), "@"),
    
    # 2. Bracketed Dots: [dot], (dot), {dot}, <dot>, [d], (d)
    (re.compile(r"\s*[\[\(\<\{\-\_\*]\s*(?:dot|\.|d)\s*[\]\)\>\}\-\_\*]\s*", re.IGNORECASE), "."),
    
    # 3. Separated by special characters (e.g. -at-, _at_, *at*)
    (re.compile(r"\s*[_\-\*]\s*at\s*[_\-\*]\s*", re.IGNORECASE), "@"),
    (re.compile(r"\s*[_\-\*]\s*dot\s*[_\-\*]\s*", re.IGNORECASE), "."),
    
    # 4. Explicit space-separated (Riskier, but supported in specific cases like " AT ")
    (re.compile(r"\s+(?:AT|[-–—]at[-–—])\s+"), "@"),
    (re.compile(r"\s+(?:DOT|[-–—]dot[-–—])\s+"), "."),
    
    # 5. URL Encoding (SAFE)
    (re.compile(r"%40", re.IGNORECASE), "@"),
    (re.compile(r"%2e", re.IGNORECASE), "."),
]

# Common "anti-spam" or "jitter" substrings that people insert into emails
JITTER_PATTERNS = [
    (re.compile(r"[\(\[\{]*\s*(?:nospam|remove-me|no-spam|delete-this|noreply)\s*[\)\]\}]*", re.IGNORECASE), ""),
    (re.compile(r"\.(?:spam|junk|nospam)\.", re.IGNORECASE), "."),
    (re.compile(r"[\.\-\_](?:nospam|remove-me|no-spam|delete-this|noreply)", re.IGNORECASE), ""),
    (re.compile(r"(?:nospam|remove-me|no-spam|delete-this|noreply)[\.\-\_]", re.IGNORECASE), ""),
]

# Patterns for obfuscation cleaning

def format_exception(exc: Exception, max_len: int = 180) -> str:
    """Format an exception to a concise string for logging."""
    raw = str(exc).strip()
    if not raw:
        return type(exc).__name__
    if len(raw) > max_len:
        raw = raw[: max_len - 3] + "..."
    return f"{type(exc).__name__}: {raw}"


def clean_text_for_extraction(text: str) -> str:
    """Normalize obfuscated email patterns and remove jitter in text."""
    if not text:
        return ""
    
    # 1. Unescape HTML and URL encoding
    try:
        text = html.unescape(text)
        text = urllib.parse.unquote(text)
    except Exception:
        pass
        
    cleaned = text
    
    # 2. Apply obfuscation replacements (@ and .)
    for pattern, replacement in OBFUSCATION_PATTERNS:
        cleaned = pattern.sub(replacement, cleaned)
        
    # 3. Remove known jitter substrings
    for jitter, replacement in JITTER_PATTERNS:
        cleaned = jitter.sub(replacement, cleaned)
        
    return cleaned


def extract_emails_from_text(text: str) -> list[str]:
    """Find all valid emails in a block of text, including obfuscated ones."""
    if not text:
        return []
    
    # 1. Pre-cleaning for encoded chars
    try:
        text = urllib.parse.unquote(text)
    except Exception: pass

    # 2. Extract candidates from various versions
    candidates = []
    
    # Helper to filter by junk group
    def add_valid(matches):
        for prefix, email in matches:
            if not prefix:
                candidates.append(email)

    # A. Direct Extraction
    add_valid(EMAIL_REGEX.findall(text))
    candidates.extend(MAILTO_REGEX.findall(text))
    
    # B. Normalized Extraction
    cleaned_text = clean_text_for_extraction(text)
    add_valid(EMAIL_REGEX.findall(cleaned_text))
    
    # 3. Deduplicate and filter
    unique = []
    seen = set()
    
    # Characters to strip from the boundaries of candidates
    STRIP_CHARS = ".,!?;:()[]{}<>\"' \n\t"

    for e in candidates:
        e_low = e.lower().strip()
        
        # Robust recursive stripping (e.g. [user@example.com] -> user@example.com)
        last_val = None
        while e_low != last_val:
            last_val = e_low
            e_low = e_low.strip(STRIP_CHARS)
        
        # Validation: basic structure
        if "@" not in e_low or e_low.count("@") > 1:
            continue
        parts = e_low.split("@")
        if len(parts) != 2 or "." not in parts[1] or parts[1].startswith("."):
            continue
        
        # New restriction: If the email contains characters that strongly suggest a URL path or junk
        if any(c in e_low for c in ["/", "\\", "?", "=", "&", "%", "$", "#"]):
            continue

        # Prose Filter: Check if the TLD is a common English word
        domain_parts = parts[1].split(".")
        tld = domain_parts[-1]
        
        # Reject if the TLD is a known media extension
        MEDIA_EXTS = {"png", "jpg", "jpeg", "webp", "gif", "svg", "bmp", "ico", "mp4", "mp3", "pdf", "exe"}
        if tld in MEDIA_EXTS:
            continue

        if tld in PROSE_TLDS:
            continue
            
        # Junk filter: checks if the email ITSELF contains junk words
        # We check this twice: once on the raw e_low and once on a more normalized version
        # to ensure strings like "image-png" or ".png" both trigger it.
        if any(junk in e_low for junk in JUNK_INDICATORS):
            continue

        # Blacklist check
        if e_low in BLACKLIST:
            continue

        # Sanity: too many dots or too long
        if len(e_low) > 60 or e_low.count(".") > 4 or ".." in e_low:
            continue

        # Reject if domain is just a number or too short
        if domain_parts[-2].replace("-", "").isdigit() or len(domain_parts[-2]) < 2:
            continue

        if e_low not in seen:
            unique.append(e_low)
            seen.add(e_low)

    # Secondary filtering: If we have 'user@example.com' and 'usernospam@example.com',
    # we should keep the clean one.
    final_unique = []
    for email in unique:
        # If this email contains jitter words and we already have a cleaner version, skip it
        jitter_words = ["nospam", "remove-me", "no-spam", "delete-this", "noreply", "spam", "junk"]
        has_jitter = any(word in email for word in jitter_words)
        
        if has_jitter:
            cleaner_version = email
            for word in jitter_words:
                cleaner_version = cleaner_version.replace(word, "").replace("..", ".").replace("--", "-").strip(".-")
            
            if cleaner_version in seen and cleaner_version != email:
                continue # Skip the jittery one
        
        final_unique.append(email)
            
    return final_unique


def extract_urls_from_text(text: str) -> list[str]:
    """Find specific high-value external links (Linktree, etc.) in text."""
    if not text:
        return []

    # Regex for general URLs
    URL_REGEX = re.compile(
        r'https?://(?:www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b(?:[-a-zA-Z0-9()@:%_\+.~#?&//=]*)',
        re.IGNORECASE
    )
    
    found_urls = URL_REGEX.findall(text)
    
    # Domains to prioritize (contact-heavy sites)
    PRIORITY_DOMAINS = [
        "linktr.ee", "beacons.ai", "beacons.page", "campsite.bio", 
        "bio.link", "solo.to", "shor.by", "flow.page", "tap.bio",
        "instagram.com", "facebook.com", "twitter.com", "x.com",
        "shopify.com", "myshopify.com"
    ]
    
    # Domains to ignore (junk/redirects)
    IGNORE_DOMAINS = [
        "youtube.com", "youtu.be", "google.com", "googlevideo.com",
        "ytimg.com", "gstatic.com", "doubleclick.net", "facebook.com/tr"
    ]

    filtered = []
    seen = set()
    
    for url in found_urls:
        url_low = url.lower().strip().rstrip("/.,!?;:")
        
        # 1. Skip if specifically ignored
        if any(d in url_low for d in IGNORE_DOMAINS):
            continue
            
        # 2. Extract domain for analysis
        parsed = urllib.parse.urlparse(url_low)
        domain = parsed.netloc.replace("www.", "")
        
        # 3. Prioritize or check if it looks like a personal site
        is_priority = any(p in domain for p in PRIORITY_DOMAINS)
        is_personal = len(domain.split(".")) >= 2 and not is_priority
        
        if (is_priority or is_personal) and url_low not in seen:
            # Re-rank priority to the front
            if is_priority:
                filtered.insert(0, url_low)
            else:
                filtered.append(url_low)
            seen.add(url_low)
            
    return filtered[:10]  # Cap at 10 links for performance
