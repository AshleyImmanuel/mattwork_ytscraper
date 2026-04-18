
import asyncio
import requests
from urllib.parse import quote_plus
from core.config import SCRAPER_API_KEY
from services.utils.extraction import extract_emails_from_text

def _scraper_api_url(target_url: str, render: bool = True) -> str:
    """Wrap a URL with ScraperAPI proxy. Use JS rendering for social sites."""
    return (
        f"http://api.scraperapi.com"
        f"?api_key={SCRAPER_API_KEY}"
        f"&url={quote_plus(target_url)}"
        f"&render={'true' if render else 'false'}"
        f"&antibot=true"
        f"&premium=true"
    )

async def scrape_external_url(url: str, on_log=None) -> list[str]:
    """Fetch an external URL via ScraperAPI and extract emails."""
    if not SCRAPER_API_KEY:
        return []

    # Social sites like Linktree definitely need JS rendering
    needs_render = any(domain in url.lower() for domain in ["linktr.ee", "beacons", "instagram", "facebook"])
    api_url = _scraper_api_url(url, render=needs_render)

    if on_log: on_log(f"  [external] Scraping: {url} (render={needs_render})")

    try:
        # Run the synchronous request in a thread to keep the loop free
        resp = await asyncio.to_thread(requests.get, api_url, timeout=45)
        if resp.status_code == 200:
            found = extract_emails_from_text(resp.text)
            if found:
                if on_log: on_log(f"  [external] SUCCESS: Found {len(found)} email(s) on {url}")
                return found
        else:
            if on_log: on_log(f"  [external] FAILED: HTTP {resp.status_code} on {url}")
    except Exception as e:
        if on_log: on_log(f"  [external] ERROR on {url}: {str(e)[:50]}")
    
    return []

async def scrape_multiple_urls(urls: list[str], on_log=None) -> list[str]:
    """Process a list of URLs and return the first valid email found."""
    for url in urls:
        emails = await scrape_external_url(url, on_log)
        if emails:
            return emails
    return []
