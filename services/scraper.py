"""
Email Scraper - Extracts public emails from YouTube channels
using restricted metadata (descriptions) via YouTube API.
"""
import sys
import asyncio
import os
import traceback

from core.config import FAST_CHECK_VIDEO_COUNT
from services.utils.extraction import extract_emails_from_text, extract_urls_from_text
from services.external_scraper import scrape_multiple_urls

async def extract_emails(results: list[dict], on_progress=None, on_log=None) -> list[dict]:
    """
    Main extraction pipeline:
    1. YouTube Channel and Video Descriptions (from Search results)
    2. Recent Video Descriptions (via recursive API lookup)
    """
    total = len(results)

    if on_log: on_log(f"Starting concurrent lightweight extraction for {total} candidates...")
    
    # Allow 20 channels to be processed concurrently, supercharging the speed
    sem = asyncio.Semaphore(20)

    async def process_channel(idx, row):
        async with sem:
            channel_name = row["channelName"]
            channel_id = row["channelId"]
            
            # --- TIER 0: Pre-found Email (e.g., via Google Discovery) ---
            if row.get("EMAIL") and row["EMAIL"] != "nil":
                if on_progress: on_progress(idx + 1, total, channel_name, row["EMAIL"])
                return
                
            # --- TIER 1: YouTube API (Search Snippets + Full Desc) ---
            full_context = f"{row.get('channelDescription','')} {row.get('videoDescription','')}"
            fast_check = extract_emails_from_text(full_context)
            if fast_check:
                row["EMAIL"] = fast_check[0]
                if on_progress: on_progress(idx + 1, total, channel_name, fast_check[0])
                return

            # --- TIER 1.5: External Link Inspection (The "Secret Weapon") ---
            urls = extract_urls_from_text(full_context)
            if urls:
                if on_log: on_log(f"  [external] Found {len(urls)} link(s) for {channel_name}, inspecting top 3...")
                try:
                    # Scan top 3 high-value links (Linktree, socials, personal sites)
                    external_emails = await scrape_multiple_urls(urls[:3], on_log)
                    if external_emails:
                        row["EMAIL"] = external_emails[0]
                        if on_progress: on_progress(idx + 1, total, channel_name, external_emails[0])
                        return
                except Exception as e:
                    if on_log: on_log(f"  [external] Error inspecting links for {channel_name}: {str(e)[:50]}")

            # --- TIER 2: Direct Handle Dorking (Deep Scan) ---
            from services.google_discovery import dork_specific_channel
            try:
                dork_emails = await asyncio.to_thread(dork_specific_channel, channel_name, on_log)
                if dork_emails:
                    row["EMAIL"] = dork_emails[0]
                    if on_log: on_log(f"  [dork] SUCCESS: Found via deep search for {channel_name}")
                    if on_progress: on_progress(idx + 1, total, channel_name, dork_emails[0])
                    return
            except Exception:
                pass

            # If we reach here, we didn't find an email in any description.
            row["EMAIL"] = "nil"
            if on_progress: on_progress(idx + 1, total, channel_name, None)

    tasks = [process_channel(idx, row) for idx, row in enumerate(results)]
    await asyncio.gather(*tasks)

    if on_log:
        on_log(f"Extraction complete for {len(results)} candidates.")

    return results
