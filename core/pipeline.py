import asyncio
import os
import sys
import re
import uuid
import time
import traceback
from datetime import datetime, timezone
from googleapiclient.errors import HttpError

from services.youtube import get_channel_details, filter_results, is_strictly_rejected
from services.youtube_crawler import crawl_youtube_search
from services.google_discovery import discover_channels_via_google
from services.scraper import extract_emails
from services.excel import generate_excel
from core.job_manager import get_job, log_to_job
from core.models import ExtractionRequest
from core.config import (
    MAX_KEYWORDS_PER_JOB,
    BOTH_REGION_SEQUENCE,
    GOOGLE_DISCOVERY_ENABLED,
    CRAWLER_ENABLED,
    CRAWLER_DELAY_MS,
    YOUTUBE_EXCLUSION_KEYWORDS as EXCLUSION_KEYWORDS,
    YOUTUBE_STRICT_EXCLUSIONS as STRICT_EXCLUSIONS,
    YOUTUBE_PRIORITY_KEYWORDS as PRIORITY_KEYWORDS,
    YOUTUBE_CHANNEL_EXCLUSION_KEYWORDS as CHANNEL_EXCLUSION_KEYWORDS,
    YOUTUBE_AUTHORITY_KEYWORDS as AUTHORITY_KEYWORDS,
    YOUTUBE_AUTHORITY_MIN_DURATION as AUTHORITY_MIN_DUR,
    YOUTUBE_LONG_MIN_DURATION as LONG_MIN_DUR,
)


def run_extraction(job_id: str, req: ExtractionRequest):
    """Entry point for the background task, running in a dedicated thread and event loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_do_run_extraction(job_id, req))
    finally:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception:
            pass
        loop.close()


# ---------------------------------------------------------------------------
# Pre-filter: runs on raw crawler data BEFORE any YouTube API calls
# ---------------------------------------------------------------------------

def _pre_filter_crawled_video(
    v: dict,
    min_views: int,
    max_views: int | None,
    video_type: str,
    search_keyword: str,
) -> str | None:
    """
    Lightweight pre-filter using data from the web crawl (no API calls).
    Returns a rejection reason string, or None if the video passes.
    """
    title = v.get("title", "")
    channel_name = v.get("channelTitle", "")
    desc = v.get("description", "")
    views = v.get("viewCount", 0)
    dur_s = v.get("duration_seconds", 0)

    full_text = f"{title} {desc} {channel_name}".upper()
    channel_name_up = channel_name.upper()

    # 1. Strict language rejection (Devanagari, Hindi, etc.)
    if is_strictly_rejected(title, desc, channel_name):
        return "language"

    # 2. Duration policy
    if video_type == "Long":
        is_authority = any(kw in search_keyword.upper() for kw in AUTHORITY_KEYWORDS)
        min_dur = AUTHORITY_MIN_DUR if is_authority else LONG_MIN_DUR
        if dur_s > 0 and dur_s < min_dur:
            return "duration"
    elif video_type == "Shorts" and dur_s > 60:
        return "duration"

    # 3. View count range
    if views > 0:
        if views < min_views:
            return "viewCount"
        if max_views and views > max_views:
            return "viewCount"

    # 4. Priority/Exclusion keyword logic
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


# ---------------------------------------------------------------------------
# Main extraction pipeline
# ---------------------------------------------------------------------------

async def _do_run_extraction(job_id: str, req: ExtractionRequest):
    """Full extraction pipeline: Crawl -> Pre-Filter -> Enrich -> Scrape Emails -> Export."""
    job = get_job(job_id)
    if not job:
        return

    print(f"DEBUG: Starting extraction for job {job_id}")
    log_to_job(job_id, "DEBUG: Extraction process entered.")
    
    try:
        # ---- Keyword / Region Setup ----
        keywords = [k.strip() for k in req.keyword.split(",") if k.strip()]
        if not keywords:
            keywords = ["Keyword"]
        if len(keywords) > MAX_KEYWORDS_PER_JOB:
            log_to_job(job_id, f"Keyword count exceeded limit; using first {MAX_KEYWORDS_PER_JOB} values.")
            keywords = keywords[:MAX_KEYWORDS_PER_JOB]

        search_regions = BOTH_REGION_SEQUENCE if req.region == "Both" else [req.region]
        search_slots = [(keyword, region) for keyword in keywords for region in search_regions]

        results = []
        seen_channel_ids = set()
        videos_searched = 0

        # Track continuation tokens per slot for page-by-page crawling
        cont_tokens: dict[tuple, str | None] = {slot: None for slot in search_slots}
        exhausted_slots: set[tuple] = set()
        slot_idx = 0
        # Per-slot consecutive stale (empty/no-yield) page counter
        stale_counts: dict[tuple, int] = {slot: 0 for slot in search_slots}
        max_stale_per_slot = 8  # Exhaust a slot after this many consecutive empty pages
        hard_page_limit = 500  # Safety cap to prevent infinite loops
        page_count = 0

        max_views = req.maxViews if req.maxViews > 0 else None
        max_subs = req.maxSubs if req.maxSubs > 0 else None

        log_to_job(job_id, f"Starting Apify-style web crawl for {len(keywords)} keyword(s) across {len(search_regions)} region(s).")
        log_to_job(job_id, f"Target: {req.leadSize} leads | Views: {req.minViews}-{req.maxViews or '∞'} | Subs: {req.minSubs}-{req.maxSubs or '∞'}")

        # ---- Main Crawl Loop ----
        while len(results) < req.leadSize and page_count < hard_page_limit:
            active_slots = [s for s in search_slots if s not in exhausted_slots]
            if not active_slots:
                log_to_job(job_id, "All keyword-region combinations exhausted.")
                break

            current_kw, current_region = active_slots[slot_idx % len(active_slots)]
            slot_idx += 1
            page_count += 1

            # Determine if this is a first page or continuation
            token = cont_tokens.get((current_kw, current_region))
            is_first = token is None and page_count <= len(search_slots)

            log_to_job(
                job_id,
                f"[Page {page_count}] Crawling '{current_kw}' in {current_region}... "
                f"(Leads: {len(results)}/{req.leadSize}, Scanned: {videos_searched})"
            )

            # ---- Crawl YouTube Search ----
            batch_videos, new_token = crawl_youtube_search(
                keyword=current_kw,
                region=current_region,
                date_filter=req.dateFilter,
                video_type=req.videoType,
                continuation_token=token,
                on_log=lambda m: log_to_job(job_id, f"  [crawler] {m}"),
            )

            # Update continuation token
            if new_token:
                cont_tokens[(current_kw, current_region)] = new_token
            else:
                exhausted_slots.add((current_kw, current_region))

            if not batch_videos:
                slot_key = (current_kw, current_region)
                stale_counts[slot_key] = stale_counts.get(slot_key, 0) + 1
                if stale_counts[slot_key] >= max_stale_per_slot:
                    log_to_job(job_id, f"Slot '{current_kw}' / {current_region} exhausted after {max_stale_per_slot} empty pages.")
                    exhausted_slots.add(slot_key)
                continue

            # ---- Pre-Filter (FREE, no API calls) ----
            pre_filtered = []
            rejections: dict[str, int] = {}
            for v in batch_videos:
                cid = v.get("channelId", "")
                if not cid or cid in seen_channel_ids:
                    continue
                
                seen_channel_ids.add(cid)  # Mark as seen immediately to prevent same-page duplicates

                reason = _pre_filter_crawled_video(v, req.minViews, max_views, req.videoType, current_kw)
                if reason:
                    rejections[reason] = rejections.get(reason, 0) + 1
                    continue

                pre_filtered.append(v)

            videos_searched += len(batch_videos)
            job["videosSearched"] = videos_searched

            if rejections:
                reason_strs = [f"{k}: {v}" for k, v in rejections.items() if v > 0]
                log_to_job(job_id, f"  [pre-filter] Rejected {sum(rejections.values())}: {', '.join(reason_strs)}")

            if not pre_filtered:
                log_to_job(job_id, f"  No candidates passed pre-filter from this page.")
                slot_key = (current_kw, current_region)
                stale_counts[slot_key] = stale_counts.get(slot_key, 0) + 1
                if stale_counts[slot_key] >= max_stale_per_slot:
                    log_to_job(job_id, f"Slot '{current_kw}' / {current_region} exhausted after {max_stale_per_slot} low-yield pages.")
                    exhausted_slots.add(slot_key)
                continue

            # Reset stale counter for this slot since we got results
            stale_counts[(current_kw, current_region)] = 0
            log_to_job(job_id, f"  {len(pre_filtered)} candidates passed pre-filter. Enriching via YouTube API...")

            # ---- Enrich via YouTube API (only for pre-filtered channels) ----
            channel_ids = list(set(v["channelId"] for v in pre_filtered))
            seen_channel_ids.update(channel_ids)

            try:
                channel_details = get_channel_details(channel_ids)
            except HttpError as e:
                import json as json_mod
                try:
                    err_data = json_mod.loads(e.content.decode("utf-8"))
                    reason = err_data.get("error", {}).get("errors", [{}])[0].get("reason")
                except Exception:
                    reason = None

                if reason == "quotaExceeded":
                    log_to_job(job_id, "[ERR] YouTube API quota exceeded during channel enrichment.")
                    job["status"] = "failed"
                    job["error"] = "YouTube API Quota Exceeded. Please wait for reset or use a different key."
                    job["finishedAt"] = datetime.now(timezone.utc).isoformat()
                    return
                else:
                    log_to_job(job_id, f"[WARN] Channel detail API error: {e}. Skipping this batch.")
                    continue

            # ---- Final Filter (subscriber count + country) ----
            from services.utils.extraction import extract_emails_from_text

            batch_results = []
            for v in pre_filtered:
                cid = v["channelId"]
                cd = channel_details.get(cid, {})
                if not cd:
                    continue

                subs = cd.get("subscriberCount", 0)
                if subs < req.minSubs or (max_subs and subs > max_subs):
                    log_to_job(job_id, f"  Skipped '{v['channelTitle']}' (Subs {subs} outside range).")
                    continue

                # Region/Country check
                from core.config import ALLOWED_COUNTRIES_BOTH, ALLOWED_COUNTRIES_US, ALLOWED_COUNTRIES_UK
                allowed_map = {"Both": ALLOWED_COUNTRIES_BOTH, "US": ALLOWED_COUNTRIES_US, "UK": ALLOWED_COUNTRIES_UK}
                target_allowed = allowed_map.get(req.region, ALLOWED_COUNTRIES_BOTH)
                channel_country = (cd.get("country") or "").strip().upper()
                if channel_country and channel_country not in target_allowed:
                    log_to_job(job_id, f"  Skipped '{v['channelTitle']}' (Country {channel_country} not in {target_allowed}).")
                    continue

                # Extract email from description (free check before deep scan)
                desc = f"{cd.get('description', '')} {v.get('description', '')}"
                found_emails = extract_emails_from_text(desc)

                row = {
                    "title": v["title"],
                    "id": v["videoId"],
                    "channelId": cid,
                    "viewCount": v["viewCount"],
                    "date": v.get("publishedText", ""),
                    "likes": 0,
                    "duration": v["duration"],
                    "url": f"https://www.youtube.com/watch?v={v['videoId']}",
                    "channelName": v["channelTitle"],
                    "channelUrl": cd.get("channelUrl", f"https://www.youtube.com/channel/{cid}"),
                    "numberOfSubscribers": subs,
                    "Country": "UK" if cd.get("country") == "GB" else (cd.get("country") or current_region),
                    "channelDescription": cd.get("description", ""),
                    "videoDescription": v.get("description", ""),
                    "EMAIL": found_emails[0] if found_emails else "nil",
                }
                batch_results.append(row)

            results.extend(batch_results)
            log_to_job(
                job_id,
                f"  +{len(batch_results)} leads this page. Total: {len(results)}/{req.leadSize}"
            )

            if len(results) >= req.leadSize:
                log_to_job(job_id, f"Target lead count reached ({req.leadSize}).")
                results = results[:req.leadSize]
                break

            # Throttle between pages
            if CRAWLER_DELAY_MS > 0:
                await asyncio.sleep(CRAWLER_DELAY_MS / 1000.0)

        # Log the reason we exited the crawl loop
        if len(results) >= req.leadSize:
            log_to_job(job_id, f"✅ Target reached! {len(results)}/{req.leadSize} leads collected across {page_count} pages.")
        elif page_count >= hard_page_limit:
            log_to_job(job_id, f"⚠️ Safety page limit ({hard_page_limit}) reached. Got {len(results)}/{req.leadSize} leads.")
        else:
            log_to_job(
                job_id,
                f"Crawl pipeline finished. {len(results)} leads from {page_count} pages (Scanned: {videos_searched})."
            )

            # ---- Google Dork Discovery (supplemental) ----
        if GOOGLE_DISCOVERY_ENABLED and len(results) < req.leadSize:
            log_to_job(job_id, "Running Google Dork discovery for additional channels...")
            google_discovered_ids = []
            google_candidates = {}

            for kw in keywords:
                google_results = await asyncio.to_thread(
                    discover_channels_via_google,
                    kw,
                    region=req.region,
                    on_log=lambda m: log_to_job(job_id, f"  {m}"),
                )
                for gr in google_results:
                    ch_id = gr["channelId"]
                    # Even if it's a handle, we add it to a list to resolve via API
                    if ch_id not in seen_channel_ids:
                        google_discovered_ids.append(ch_id)
                        google_candidates[ch_id] = gr

            if google_discovered_ids:
                log_to_job(job_id, f"Fetching YouTube metadata for {len(google_discovered_ids)} discovered channels...")
                google_details = get_channel_details(google_discovered_ids)

                from services.utils.extraction import extract_emails_from_text

                google_new = 0
                for input_id in google_discovered_ids:
                    gr = google_candidates[input_id]
                    gd = google_details.get(input_id, {})
                    
                    # CANONICAL ID RESOLUTION:
                    # If we searched by handle, gd["id"] contains the UC... ID.
                    # We MUST use the UC ID for seen_channel_ids to prevent duplicates with the crawl loop.
                    canonical_id = gd.get("id") or (input_id if input_id.startswith("UC") else None)
                    if not canonical_id:
                        continue
                        
                    if canonical_id in seen_channel_ids:
                        continue # Already found this channel via crawl or another handle
                    
                    seen_channel_ids.add(canonical_id)

                    email = gr["emails"][0] if gr["emails"] else "nil"
                    if email == "nil":
                        desc_to_check = f"{gd.get('description', '')} {gr.get('snippet', '')}"
                        found = extract_emails_from_text(desc_to_check)
                        if found:
                            email = found[0]

                    row = {
                        "title": f"[Discovery] {gd.get('title', input_id)}",
                        "id": "",
                        "channelId": canonical_id,
                        "viewCount": gd.get("viewCount", 0),
                        "date": "",
                        "likes": 0,
                        "duration": "",
                        "url": gd.get("channelUrl") or gr["channelUrl"],
                        "channelName": gd.get('title', input_id),
                        "channelUrl": gd.get("channelUrl") or gr["channelUrl"],
                        "numberOfSubscribers": gd.get('subscriberCount', 0),
                        "Country": gd.get('country') or req.region,
                        "channelDescription": gd.get('description') or gr.get("snippet", ""),
                        "videoDescription": gr.get("snippet", ""),
                        "EMAIL": email,
                    }

                    views = row["viewCount"]
                    subs = row["numberOfSubscribers"]
                    if (row["viewCount"] > 0) and (views < req.minViews or (max_views and views > max_views)):
                        continue
                    if subs < req.minSubs or (max_subs and subs > max_subs):
                        continue

                    if is_strictly_rejected(row["title"], row['channelDescription'], row["channelName"]):
                        continue

                    results.append(row)
                    google_new += 1

                if google_new:
                    log_to_job(job_id, f"  [google] Added {google_new} new unique channels.")

        # ---- Final Deduplication (BEFORE Email Scraping) ----
        unique_results = {}
        for r in results:
            cid = r.get("channelId")
            if cid and cid not in unique_results:
                unique_results[cid] = r
            elif not cid:
                unique_results[f"no-id-{uuid.uuid4()}"] = r
        results = list(unique_results.values())

        # ---- Final Trim ----
        if len(results) > req.leadSize:
            results = results[:req.leadSize]

        if not results:
            log_to_job(job_id, "No channels matched your filter criteria.")
            filepath = generate_excel([], req.keyword)
            job["filePath"] = filepath
            job["status"] = "completed"
            job["finishedAt"] = datetime.now(timezone.utc).isoformat()
            return

        # ---- Email Scraping Phase ----
        job["total"] = len(results)
        log_to_job(job_id, f"Scraping emails from {len(results)} UNIQUE channels...")

        def on_progress(current, total, name, email):
            job["progress"] = current
            status = f"found: {email}" if email else "no public email"
            log_to_job(job_id, f"  [{current}/{total}] {name} - {status}")
            if email:
                job["emailsFound"] += 1

        def on_log_msg(message: str):
            log_to_job(job_id, f"  [scraper] {message}")

        # Reset count before starting scraping to ensure UI sync
        job["emailsFound"] = 0
        
        # Add initial count from already found emails (e.g. via discovery snippets)
        for r in results:
            if r.get("EMAIL") and r["EMAIL"] != "nil":
                job["emailsFound"] += 1

        results = await extract_emails(results, on_progress, on_log_msg)
        
        # Recalculate one final time to be safe
        final_email_count = sum(1 for r in results if r.get("EMAIL") and r["EMAIL"] != "nil")
        job["emailsFound"] = final_email_count
        
        log_to_job(job_id, f"Email extraction complete - {final_email_count} unique emails found.")

        final_count = len(results)
        job["total"] = final_count
        log_to_job(job_id, f"Pipeline complete. {final_count} unique leads ready for export.")

        # ---- Export to Excel ----
        log_to_job(job_id, "Generating Excel file...")
        filepath = generate_excel(results, req.keyword)
        job["filePath"] = filepath
        log_to_job(job_id, f"[OK] Export complete: {os.path.basename(filepath)}")

        job["status"] = "completed"
        job["finishedAt"] = datetime.now(timezone.utc).isoformat()

    except HttpError as e:
        job["status"] = "failed"
        job["finishedAt"] = datetime.now(timezone.utc).isoformat()
        import json
        try:
            err_data = json.loads(e.content.decode("utf-8"))
            reason = err_data.get("error", {}).get("errors", [{}])[0].get("reason")
        except Exception:
            reason = None

        if reason == "quotaExceeded":
            job["error"] = "YouTube API Quota Exceeded. Please wait for reset or use a different key."
            log_to_job(job_id, "[ERR] YouTube API quota exceeded (10,000 unit limit reached).")
        else:
            job["error"] = f"YouTube API Error: {type(e).__name__}"
            log_to_job(job_id, f"[ERR] YouTube API HttpError: {e}")

    except Exception as e:
        traceback.print_exc()
        job["status"] = "failed"
        job["error"] = "Extraction failed due to an internal error."
        job["finishedAt"] = datetime.now(timezone.utc).isoformat()
        log_to_job(job_id, f"[ERR] Error: {type(e).__name__}: {e}")
