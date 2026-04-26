import re
import json

def parse_yt_initial_data(html: str) -> dict | None:
    """Extract the ytInitialData JSON object from YouTube page HTML."""
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

    script_pattern = r'<script[^>]*>var\s+ytInitialData\s*=\s*(\{.*?\})\s*;</script>'
    match = re.search(script_pattern, html, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass
    return None

def parse_view_count(text: str) -> int:
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

def parse_duration_text(text: str) -> int:
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

def format_duration(seconds: int) -> str:
    """Format seconds as h:mm:ss or m:ss."""
    if seconds <= 0:
        return "0:00"
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h > 0:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"

def safe_text(obj: dict | None, key: str = "simpleText") -> str:
    """Safely extract text from YouTube's various text formats."""
    if not obj:
        return ""
    if key in obj:
        return obj[key]
    if "runs" in obj:
        return "".join(run.get("text", "") for run in obj["runs"])
    return ""
