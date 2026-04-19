
import asyncio
import requests
from urllib.parse import quote_plus
from core.config import (
    SCRAPER_API_KEY, 
    USE_LOCAL_BROWSER, 
    BROWSER_HEADLESS, 
    BROWSER_TIMEOUT_MS
)
from services.utils.extraction import extract_emails_from_text

# Use playwright if enabled
try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

class BrowserManager:
    """Manages a persistent Playwright browser instance."""
    _instance = None
    _browser = None
    _playwright = None

    @classmethod
    async def get_browser(cls):
        if not PLAYWRIGHT_AVAILABLE:
            return None
        if cls._browser is None:
            cls._playwright = await async_playwright().start()
            cls._browser = await cls._playwright.chromium.launch(
                headless=BROWSER_HEADLESS
            )
        return cls._browser

    @classmethod
    async def close(cls):
        if cls._browser:
            await cls._browser.close()
            cls._browser = None
        if cls._playwright:
            await cls._playwright.stop()
            cls._playwright = None

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
    """Fetch an external URL and extract emails (via local browser or ScraperAPI)."""
    
    if USE_LOCAL_BROWSER and PLAYWRIGHT_AVAILABLE:
        return await _scrape_with_local_browser(url, on_log)
    
    # Fallback to ScraperAPI
    if not SCRAPER_API_KEY:
        if on_log: on_log(f"  [external] Skipping {url}: No API key or local browser available.")
        return []

    needs_render = any(domain in url.lower() for domain in ["linktr.ee", "beacons", "instagram", "facebook"])
    api_url = _scraper_api_url(url, render=needs_render)

    if on_log: on_log(f"  [external] Scraping (ScraperAPI): {url} (render={needs_render})")

    try:
        resp = await asyncio.to_thread(requests.get, api_url, timeout=45)
        if resp.status_code == 200:
            found = extract_emails_from_text(resp.text)
            if found:
                if on_log: on_log(f"  [external] SUCCESS: Found {len(found)} email(s) on {url}")
                return found
        else:
            if on_log: on_log(f"  [external] FAILED: HTTP {resp.status_code} on {url}")
    except Exception as e:
        if on_log: on_log(f"  [external] ERROR on {url} (ScraperAPI): {str(e)[:50]}")
    
    return []

async def _scrape_with_local_browser(url: str, on_log=None) -> list[str]:
    """Use local Playwright browser to scrape a URL."""
    if on_log: on_log(f"  [external] Scraping (Local Browser): {url}")
    
    browser = await BrowserManager.get_browser()
    if not browser:
        return []

    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    page = await context.new_page()
    
    try:
        # Navigate and wait for some content or a timeout
        await page.goto(url, wait_until="networkidle", timeout=BROWSER_TIMEOUT_MS)
        
        # Some social sites need a bit more time for JS to fire
        await asyncio.sleep(2)
        
        content = await page.content()
        found = extract_emails_from_text(content)
        
        if found:
            if on_log: on_log(f"  [external] SUCCESS: Found {len(found)} email(s) on {url}")
            return found
            
    except Exception as e:
        if on_log: on_log(f"  [external] ERROR on {url} (Local): {str(e)[:50]}")
    finally:
        # We close the page/context but keep the browser instance alive per user preference
        await context.close()
        
    return []

async def scrape_multiple_urls(urls: list[str], on_log=None) -> list[str]:
    """Process a list of URLs and return the first valid email found."""
    for url in urls:
        # Filter out junk URLs before scraping
        if any(junk in url.lower() for junk in ["youtube.com", "google.com", "twitter.com", "facebook.com/sharer"]):
            continue
            
        emails = await scrape_external_url(url, on_log)
        if emails:
            return emails
    return []
