"""
Email Scraper - Extracts public emails from YouTube channels
using lightweight HTTP requests without headless browsers.
"""
import sys
import asyncio
import os
import re
import traceback
import requests

from core.config import FAST_CHECK_VIDEO_COUNT
from services.utils.extraction import extract_emails_from_text
from services.youtube import get_recent_videos
from services.extraction.lightweight_strategy import try_extract_lightweight


async def extract_emails(results: list[dict], on_progress=None, on_log=None) -> list[dict]:
    """
    Main extraction pipeline:
    1. YT Descriptions (Last N videos via API)
    2. Lightweight External Links (Requests/BS4)
    """
    total = len(results)

    if on_log: on_log(f"Starting concurrent lightweight extraction for {total} candidates...")
    
    # Allow 20 channels to be processed concurrently, supercharging the speed
    sem = asyncio.Semaphore(20)

    async def process_channel(idx, row):
        async with sem:
            channel_name = row["channelName"]
            channel_id = row["channelId"]
            
            # --- TIER 1: YouTube API (Search Snippets + Full Desc) ---
            full_context = f"{row.get('channelDescription','')} {row.get('videoDescription','')}"
            fast_check = extract_emails_from_text(full_context)
            if fast_check:
                row["EMAIL"] = fast_check[0]
                if on_progress: on_progress(idx + 1, total, channel_name, fast_check[0])
                return

            # --- TIER 2: YouTube API (Recent Video Descriptions) ---
            if FAST_CHECK_VIDEO_COUNT > 0:
                try:
                    recent_vids = await asyncio.to_thread(get_recent_videos, channel_id, FAST_CHECK_VIDEO_COUNT)
                    all_vids_text = "".join([f" {vid['title']} {vid['description']}" for vid in recent_vids])
                    
                    v_emails = extract_emails_from_text(all_vids_text)
                    if v_emails:
                        row["EMAIL"] = v_emails[0]
                        if on_log: on_log(f"  [api] SUCCESS: Found in video descriptions for {channel_name}")
                        if on_progress: on_progress(idx + 1, total, channel_name, v_emails[0])
                        return
                except Exception:
                    pass

            # --- TIER 3: Lightweight External Links (Requests) ---
            try:
                if on_log: on_log(f"Analyzing {channel_name}...")
                channel_about_url = row["channelUrl"] + "/about"
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"
                }
                r = await asyncio.to_thread(requests.get, channel_about_url, headers=headers, timeout=8)
                
                # Extract emails natively from HTML just in case
                html_emails = extract_emails_from_text(r.text)
                if html_emails:
                    row["EMAIL"] = html_emails[0]
                    if on_progress: on_progress(idx + 1, total, channel_name, html_emails[0])
                    return

                # Fallback regex to find generic urls inside ytInitialData
                links = set(re.findall(r'https?://[^\s\"\'\>\\]+', r.text))

                found_email = None
                ignored_domains = [
                    "youtube.com", "youtu.be", "google.com", "gstatic.com", "schema.org", "xml", "w3.org",
                    "instagram.com", "facebook.com", "twitter.com", "x.com", "tiktok.com", "linkedin.com",
                    "apple.com", "spotify.com", "amazon.com"
                ]

                for l in links:
                    if any(x in l.lower() for x in ignored_domains): continue
                    
                    # Fix urlencoded redirects
                    if "q=" in l:
                        from urllib.parse import urlparse, parse_qs
                        try: 
                            parsed_q = parse_qs(urlparse(l).query).get("q", [l])[0]
                            if parsed_q.startswith("http"): l = parsed_q
                        except: pass
                    
                    # Deep scan this link
                    found_email = await asyncio.to_thread(try_extract_lightweight, l, on_log, 1)
                    if found_email: break
                    
                if found_email:
                    row["EMAIL"] = found_email
                    if on_progress: on_progress(idx + 1, total, channel_name, found_email)
                else:
                    row["EMAIL"] = "nil"
                    if on_progress: on_progress(idx + 1, total, channel_name, None)

            except Exception as e:
                row["EMAIL"] = "nil"
                if on_progress: on_progress(idx + 1, total, channel_name, None)

    tasks = [process_channel(idx, row) for idx, row in enumerate(results)]
    await asyncio.gather(*tasks)

    return results
