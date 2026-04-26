import os
import httpx
from core.job_manager import log_to_job

async def check_api_health(job_id):
    sa_key = os.getenv("SCRAPER_API_KEY")
    if not sa_key:
        log_to_job(job_id, "[CRUCIAL ERR] ScraperAPI Key is MISSING!")
        return False
    try:
        test_url = f"http://api.scraperapi.com?api_key={sa_key}&url=https://www.google.com"
        async with httpx.AsyncClient(timeout=10) as client:
            res = await client.get(test_url)
            if res.status_code == 403:
                log_to_job(job_id, "[CRUCIAL ERR] SCRAPER-API CREDITS EXHAUSTED (403)")
                return False
            log_to_job(job_id, "[OK] Proxy network initialized.")
            return True
    except Exception as e:
        log_to_job(job_id, f"[WARN] Proxy health check failed: {e}")
        return True
