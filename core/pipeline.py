import asyncio
import os
import sys
import uuid
import traceback
from datetime import datetime, timezone
from googleapiclient.errors import HttpError

from services.youtube import search_videos, get_video_details, get_channel_details, filter_results, is_strictly_rejected
from services.google_discovery import discover_channels_via_google
from services.scraper import extract_emails
from services.excel import generate_excel
from core.job_manager import get_job, log_to_job
from core.models import ExtractionRequest
from core.config import (
    MAX_KEYWORDS_PER_JOB,
    BOTH_REGION_SEQUENCE,
    MAX_API_FETCHES,
    MAX_STALE_BATCHES,
    MIN_MATCH_TARGET_ABSOLUTE,
    MIN_MATCH_TARGET_DIVISOR,
    GOOGLE_DISCOVERY_ENABLED
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

async def _do_run_extraction(job_id: str, req: ExtractionRequest):
    """Full extraction pipeline: Search -> Filter -> Scrape Emails -> Export Excel."""
    job = get_job(job_id)
    if not job:
        return

    try:
        # Step 1-4: Loop to fetch exactly up to Search Pool Size
        keywords = [k.strip() for k in req.keyword.split(",") if k.strip()]
        if not keywords:
            keywords = ["Keyword"]
        if len(keywords) > MAX_KEYWORDS_PER_JOB:
            log_to_job(job_id, f"Keyword count exceeded limit; using first {MAX_KEYWORDS_PER_JOB} values.")
            keywords = keywords[:MAX_KEYWORDS_PER_JOB]

        search_regions = BOTH_REGION_SEQUENCE if req.region == "Both" else [req.region]
        search_slots = [(keyword, region) for keyword in keywords for region in search_regions]

        results = []
        page_tokens = {slot: None for slot in search_slots}
        exhausted_slots = set()
        seen_channel_ids = set()
        videos_searched = 0
        max_api_fetches = MAX_API_FETCHES
        fetches = 0
        slot_idx = 0
        stale_batches = 0
        max_stale_batches = MAX_STALE_BATCHES
        min_match_target_for_early_stop = max(
            MIN_MATCH_TARGET_ABSOLUTE,
            req.searchPoolSize // MIN_MATCH_TARGET_DIVISOR,
        )

        while videos_searched < req.searchPoolSize and fetches < max_api_fetches:
            active_slots = [slot for slot in search_slots if slot not in exhausted_slots]
            if not active_slots:
                log_to_job(job_id, "All keyword-region combinations exhausted.")
                break

            current_kw, current_region = active_slots[slot_idx % len(active_slots)]
            slot_idx += 1
            fetches += 1

            log_to_job(
                job_id,
                f"[Batch {fetches}] Searching '{current_kw}' in {current_region}... "
                f"(Scanned {videos_searched}/{req.searchPoolSize})"
            )

            batch_videos, new_token = search_videos(
                current_kw,
                current_region,
                req.dateFilter,
                max_results=50,
                page_token=page_tokens[(current_kw, current_region)],
                video_type=req.videoType,
            )

            if not batch_videos or not new_token:
                exhausted_slots.add((current_kw, current_region))

            if not batch_videos:
                continue

            page_tokens[(current_kw, current_region)] = new_token

            candidate_videos = [v for v in batch_videos if v["channelId"] not in seen_channel_ids]
            if not candidate_videos:
                stale_batches += 1
                log_to_job(job_id, "Batch had only already-selected channels; skipping detail lookups.")
                if stale_batches >= max_stale_batches and len(results) >= min_match_target_for_early_stop:
                    log_to_job(job_id, "Stopping early to protect API quota after repeated low-yield batches.")
                    break
                continue

            video_ids = [v["videoId"] for v in candidate_videos]
            video_details = get_video_details(video_ids)

            channel_ids = list(set(v["channelId"] for v in candidate_videos))
            channel_details = get_channel_details(channel_ids)

            # Optimization: Mark these as seen immediately so they aren't processed in the next batch
            # even if they are rejected by the filters below.
            seen_channel_ids.update(channel_ids)

            # --- Process Candidates (Inclusive Mode) ---
            from services.utils.extraction import extract_emails_from_text
            processed_candidates = []
            for v in candidate_videos:
                vid, cid = v["videoId"], v["channelId"]
                vd, cd = video_details.get(vid, {}), channel_details.get(cid, {})
                if not vd or not cd: continue
                
                # Check for email in the fetched metadata
                desc = f"{cd.get('description', '')} {vd.get('description', '')}"
                found = extract_emails_from_text(desc)
                v["EMAIL"] = found[0] if found else "nil"
                
                # Ensure full metadata is carried forward
                v["title"] = vd.get("title", v["title"])
                v["description"] = vd.get("description", v["description"])
                processed_candidates.append(v)

            # Trim the batch so we never overshoot the pool size target
            remaining = req.searchPoolSize - videos_searched
            if remaining <= 0:
                log_to_job(job_id, f"Search pool reached explicit limit ({req.searchPoolSize} channels processed).")
                break

            if len(processed_candidates) > remaining:
                log_to_job(job_id, f"Trimming last batch to fit exact pool size of {req.searchPoolSize}.")
                processed_candidates = processed_candidates[:remaining]

            videos_searched += len(processed_candidates)
            job["videosSearched"] = videos_searched
            log_to_job(job_id, f"Processed {len(processed_candidates)} channels this batch. (Total scanned: {videos_searched}/{req.searchPoolSize})")

            max_views = req.maxViews if req.maxViews > 0 else None
            max_subs = req.maxSubs if req.maxSubs > 0 else None

            # --- Second: Apply numerical filters ---
            batch_results = filter_results(
                processed_candidates,
                video_details,
                channel_details,
                req.minViews,
                max_views,
                req.minSubs,
                max_subs,
                req.region,
                video_type=req.videoType,
                search_keyword=current_kw,
                on_log=lambda m: log_to_job(job_id, m),
            )

            new_unique = 0
            for br in batch_results:
                results.append(br)
                new_unique += 1

            if new_unique == 0:
                stale_batches += 1
            else:
                stale_batches = 0

            log_to_job(
                job_id,
                f"Found {len(batch_results)} matches in this batch. "
                f"Added {new_unique} new channels. Total matches so far: {len(results)}"
            )

            if stale_batches >= max_stale_batches and len(results) >= min_match_target_for_early_stop:
                log_to_job(job_id, "Stopping early to protect API quota after repeated low-yield batches.")
                break
        
        log_to_job(job_id, f"Search pipeline finished. Total videos scanned: {videos_searched}/{req.searchPoolSize}. Found {len(results)} potential matches.")

        # Step 4b: Google Dork Discovery (supplemental)
        if GOOGLE_DISCOVERY_ENABLED:
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
                    if ch_id not in seen_channel_ids:
                        seen_channel_ids.add(ch_id)
                        google_discovered_ids.append(ch_id)
                        google_candidates[ch_id] = gr

            if google_discovered_ids:
                log_to_job(job_id, f"Fetching YouTube metadata for {len(google_discovered_ids)} discovered channels...")
                google_details = get_channel_details(google_discovered_ids)
                
                google_new = 0
                for ch_id in google_discovered_ids:
                    gr = google_candidates[ch_id]
                    gd = google_details.get(ch_id, {})
                    
                    # --- Extract Email ---
                    email = gr["emails"][0] if gr["emails"] else "nil"
                    if email == "nil":
                        from services.utils.extraction import extract_emails_from_text
                        desc_to_check = f"{gd.get('description', '')} {gr.get('snippet', '')}"
                        found = extract_emails_from_text(desc_to_check)
                        if found:
                            email = found[0]

                    row = {
                        "title": f"[Discovery] {gd.get('title', ch_id)}",
                        "id": "",
                        "channelId": gd.get("id", ch_id),
                        "viewCount": gd.get("viewCount", 0),
                        "date": "",
                        "likes": 0,
                        "duration": "",
                        "url": gr["channelUrl"],
                        "channelName": gd.get('title', ch_id),
                        "channelUrl": gr["channelUrl"],
                        "numberOfSubscribers": gd.get('subscriberCount', 0),
                        "Country": gd.get('country') or req.region,
                        "channelDescription": gd.get('description') or gr.get("snippet", ""),
                        "videoDescription": gr.get("snippet", ""),
                        "EMAIL": email,
                    }

                    # --- Numerical Filtering ---
                    views = row["viewCount"]
                    subs = row["numberOfSubscribers"]
                    max_views = req.maxViews if req.maxViews > 0 else None
                    max_subs = req.maxSubs if req.maxSubs > 0 else None

                    if (row["viewCount"] > 0) and (views < req.minViews or (max_views and views > max_views)):
                        log_to_job(job_id, f"  [google] Skipped '{row['channelName']}' (Views {views} outside range).")
                        continue
                    if subs < req.minSubs or (max_subs and subs > max_subs):
                        log_to_job(job_id, f"  [google] Skipped '{row['channelName']}' (Subs {subs} outside range).")
                        continue

                    # --- Final Quality Check: Apply Strict Language Filtering ---
                    if is_strictly_rejected(
                        row["title"],
                        row['channelDescription'],
                        row["channelName"]
                    ):
                        log_to_job(job_id, f"  [google] Skipped '{row['channelName']}' (Matches language exclusion).")
                        continue

                    results.append(row)
                    google_new += 1
                
                if google_new:
                    log_to_job(job_id, f"  [google] Successfully added {google_new} new channels with rich metadata.")

        if not results:
            log_to_job(job_id, "No channels matched your filter criteria.")
            filepath = generate_excel([], req.keyword)
            job["filePath"] = filepath
            job["status"] = "completed"
            job["finishedAt"] = datetime.now(timezone.utc).isoformat()
            return

        # Step 5: Scrape emails
        job["total"] = len(results)
        log_to_job(job_id, f"Scraping emails from {len(results)} channels using lightweight HTTP requests...")


        def on_progress(current, total, name, email):
            job["progress"] = current
            status = f"found: {email}" if email else "no public email"
            log_to_job(job_id, f"  [{current}/{total}] {name} - {status}")
            if email:
                job["emailsFound"] += 1

        def on_log_msg(message: str):
            log_to_job(job_id, f"  [scraper] {message}")

        # Run the concurrent lightweight HTTP-based extraction pipeline
        results = await extract_emails(results, on_progress, on_log_msg)
        log_to_job(job_id, f"Email extraction complete - {job['emailsFound']} emails found.")

        # Step 5b: Final Deduplication
        unique_results = {}
        for r in results:
            cid = r.get("channelId")
            if cid and cid not in unique_results:
                unique_results[cid] = r
            elif not cid:
                unique_results[f"no-id-{uuid.uuid4()}"] = r
        results = list(unique_results.values())

        # Final count update for UI consistency - ONLY count the actual leads going into the report
        final_count = len(results)
        job["total"] = final_count
        log_to_job(job_id, f"Pipeline complete. {final_count} unique leads with emails ready for export.")

        # Step 6: Export to Excel
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
