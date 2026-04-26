"""
Main Extraction Pipeline — Refactored to < 300 lines.
"""

import asyncio
import os
import uuid
import traceback
from datetime import datetime, timezone
from googleapiclient.errors import HttpError

from services.youtube import get_channel_details, is_strictly_rejected
from services.youtube_crawler import crawl_youtube_search_async
from services.utils.browser_manager import BrowserManager
from services.google_discovery import discover_channels_via_google
from services.scraper import extract_emails
from services.excel import generate_excel
from core.job_manager import get_job, log_to_job
from core.models import ExtractionRequest
from core.pipeline_steps.pre_filter import pre_filter_crawled_video
from core.pipeline_steps.health_check import check_api_health
from core.config import MAX_KEYWORDS_PER_JOB, CRAWLER_DELAY_MS, USE_LOCAL_BROWSER

def run_extraction(job_id: str, req: ExtractionRequest):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try: loop.run_until_complete(_do_run_extraction(job_id, req))
    finally:
        try: loop.run_until_complete(loop.shutdown_asyncgens())
        except: pass
        loop.close()

async def _do_run_extraction(job_id: str, req: ExtractionRequest):
    job = get_job(job_id)
    if not job or not await check_api_health(job_id): return
    
    keywords = [k.strip() for k in req.keyword.split(",") if k.strip()][:MAX_KEYWORDS_PER_JOB]
    search_slots = [(k, req.region) for k in keywords]
    results, seen_channel_ids = [], set()
    cont_tokens = {slot: None for slot in search_slots}
    exhausted_slots, page_count = set(), 0
    max_views, max_subs = (req.maxViews if req.maxViews > 0 else None), (req.maxSubs if req.maxSubs > 0 else None)

    persistent_context, persistent_page = (await BrowserManager.get_page(region=req.region)) if USE_LOCAL_BROWSER else (None, None)

    try:
        while len(results) < req.leadSize and page_count < 500:
            active_slots = [s for s in search_slots if s not in exhausted_slots]
            if not active_slots: break
            
            cur_kw, cur_reg = active_slots[page_count % len(active_slots)]
            page_count += 1
            log_to_job(job_id, f"[Page {page_count}] Crawling '{cur_kw}'...")

            batch, next_token = await crawl_youtube_search_async(
                cur_kw, cur_reg, req.dateFilter, req.videoType, cont_tokens[(cur_kw, cur_reg)],
                lambda m: log_to_job(job_id, f"  [crawler] {m}"), persistent_page
            )
            cont_tokens[(cur_kw, cur_reg)] = next_token
            if not next_token: exhausted_slots.add((cur_kw, cur_reg))
            if not batch: continue

            pre_filtered = []
            for v in batch:
                cid = v.get("channelId")
                if cid and cid not in seen_channel_ids and not pre_filter_crawled_video(v, req.minViews, max_views, req.videoType, cur_kw):
                    seen_channel_ids.add(cid)
                    pre_filtered.append(v)

            if pre_filtered:
                log_to_job(job_id, f"  {len(pre_filtered)} candidates. Enriching...")
                try:
                    details = get_channel_details([v["channelId"] for v in pre_filtered])
                    for v in pre_filtered:
                        cd = details.get(v["channelId"])
                        if cd and req.minSubs <= cd["subscriberCount"] <= (max_subs or float('inf')):
                            results.append({
                                "channelId": v["channelId"], "channelName": v["channelTitle"], "numberOfSubscribers": cd["subscriberCount"],
                                "EMAIL": "nil", "channelUrl": cd["channelUrl"], "Country": cd.get("country") or req.region,
                                "viewCount": v["viewCount"], "url": f"https://www.youtube.com/watch?v={v['videoId']}", "title": v["title"],
                                "channelDescription": cd.get("description", ""), "videoDescription": v.get("description", ""), "duration": v["duration"]
                            })
                except HttpError as e:
                    if "quotaExceeded" in str(e): break
            if CRAWLER_DELAY_MS > 0: await asyncio.sleep(CRAWLER_DELAY_MS / 1000.0)

        # Final scraping and export
        job["total"] = len(results)
        results = await extract_emails(results, lambda c, t, n, e: log_to_job(job_id, f" [{c}/{t}] {n} - {e}"), region=req.region)
        job["filePath"] = generate_excel(results, req.keyword)
        job["status"] = "completed"
    except Exception as e:
        log_to_job(job_id, f"[ERR] {e}")
        job["status"] = "failed"
    finally:
        if persistent_context: await persistent_context.close()
        job["finishedAt"] = datetime.now(timezone.utc).isoformat()
