import re

with open(r"d:\MattScrape\services\scraper.py", "r") as f:
    text = f.read()

# Make imports async
text = text.replace("from playwright.sync_api import sync_playwright", "from playwright.async_api import async_playwright\nimport asyncio")

# Replace function definitions
text = text.replace("def _try_extract_from_about(", "async def _try_extract_from_about(")
text = text.replace("def _try_extract_from_links(", "async def _try_extract_from_links(")
text = text.replace("def _extract_email_from_channel(", "async def _extract_email_from_channel(")

# Add await to playwright page calls
text = re.sub(r"(page\.(?:goto|wait_for_timeout|content|inner_text|eval_on_selector_all))", r"await \1", text)
text = text.replace("btn.first.click", "await btn.first.click")
text = text.replace("btn.count()", "await btn.count()")

# We must update extract_emails to orchestrate the async browsers.
# We'll completely replace extract_emails with an async semaphore version.
extract_fn = """async def extract_emails(results: list[dict], on_progress=None, on_log=None) -> list[dict]:
    proxy_url = _scraper_api_proxy_url()
    
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            proxy={"server": proxy_url} if SCRAPER_API_KEY else None,
        )
        
        sem = asyncio.Semaphore(5)  # Max 5 concurrent proxy connections
        total = len(results)
        
        async def process_channel(row, idx):
            channel_url = row["channelUrl"]
            channel_name = row["channelName"]
            
            # Fast check
            desc = row.get("channelDescription", "")
            email = None
            if desc:
                fast_check = _extract_emails_from_text(desc)
                if fast_check:
                    email = fast_check[0]
                    row["EMAIL"] = email
                    if on_progress: on_progress(idx + 1, total, channel_name, email)
                    return
                    
            if on_log: on_log(f"Testing browser extraction for: {channel_name}...")
            
            async with sem:
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    viewport={"width": 1280, "height": 720},
                )
                page = await context.new_page()
                email = await _extract_email_from_channel(page, channel_url, on_log)
                row["EMAIL"] = email or "nil"
                await context.close()
                
                if on_progress:
                    on_progress(idx + 1, total, channel_name, email)
                    
        tasks = [process_channel(row, i) for i, row in enumerate(results)]
        await asyncio.gather(*tasks)
        
        await browser.close()
        
    return results
"""

# Replace the old extract_emails function
text = re.sub(r"def extract_emails\(.*?\)(?s:.)*", extract_fn, text, count=1)

# Fix time.sleep to asyncio.sleep
text = text.replace("time.sleep(RETRY_DELAY_MS / 1000)", "await asyncio.sleep(RETRY_DELAY_MS / 1000)")

# Also, update internal calls in _extract_email_from_channel to await
text = text.replace("_try_extract_from_about(page, channel_url, on_log)", "await _try_extract_from_about(page, channel_url, on_log)")
text = text.replace("_try_extract_from_links(page, channel_url)", "await _try_extract_from_links(page, channel_url)")


with open(r"d:\MattScrape\services\scraper.py", "w") as f:
    f.write(text)
print("Rewrite complete.")
