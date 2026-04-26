"""
Scraper for YouTube About Page — Refactored to < 300 lines.
"""

import asyncio
import re
import uuid
from services.utils.browser_manager import BrowserManager
from services.about_scraper.captcha_solver import solve_captcha_automated, wait_for_manual_solve
from services.about_scraper.ui_interactions import click_view_email_button, inject_status_banner

async def extract_email_from_about_page(channel_url, on_log=None, session_id=None, region="US"):
    from core.config import BROWSER_TIMEOUT_MS, ENABLE_ADVANCED_STEALTH, BROWSER_ZOOM
    if "youtube.com" not in channel_url: return None
    if not channel_url.endswith("/about") and "/@" in channel_url:
        channel_url = channel_url.split("?")[0].rstrip("/") + "/about"

    current_session = session_id or str(uuid.uuid4())[:12]
    context, page = await BrowserManager.get_page(optimize=True, session_id=current_session, region=region)
    if not page: return None

    try:
        if on_log: on_log(f"[about] Opening: {channel_url}")
        await page.bring_to_front()
        if BROWSER_ZOOM and BROWSER_ZOOM != 1.0:
            try: await page.add_init_script(f"document.documentElement.style.zoom = '{int(BROWSER_ZOOM * 100)}%'")
            except: pass

        for attempt in range(3):
            try:
                await page.goto(channel_url, wait_until="load", timeout=60000)
                if any(x in await page.content() for x in ["You're offline", "Connect to the internet"]):
                    new_session = str(uuid.uuid4())[:12]
                    await context.close()
                    context, page = await BrowserManager.get_page(optimize=True, session_id=new_session, region=region)
                    raise Exception("Proxy Blocked")
                break
            except Exception as e:
                if attempt == 2: raise e
                await asyncio.sleep(6)

        if not await click_view_email_button(page, on_log, ENABLE_ADVANCED_STEALTH):
            if on_log: on_log("  [about] Button not found.")
            return None

        await page.wait_for_load_state("networkidle", timeout=10000)
        await asyncio.sleep(2)

        content = await page.content()
        if any(x in content.lower() for x in ["recaptcha", "g-recaptcha", "captcha"]):
            await inject_status_banner(page, "ROBOT STATUS: Solving CAPTCHA...")
            if not await solve_captcha_automated(page, on_log):
                await wait_for_manual_solve(page, on_log)

            submit_selectors = ["button#submit-btn", "yt-button-renderer#submit-button", "button:has-text('Submit')"]
            for sel in submit_selectors:
                try:
                    btn = await page.wait_for_selector(sel, timeout=5000)
                    if btn:
                        await btn.click()
                        await asyncio.sleep(4)
                        break
                except: continue

        await asyncio.sleep(2)
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        post_content = await page.content()
        
        for sel in ["yt-formatted-string[id='email']", "yt-formatted-string#email", "a[href^='mailto:']"]:
            el = await page.query_selector(sel)
            if el:
                matches = re.findall(email_pattern, await el.inner_text())
                if matches: return matches[0]

        matches = re.findall(email_pattern, post_content)
        matches = [e for e in matches if all(x not in e for x in ["sentry.io", "google.com", "youtube-ui"])]
        return matches[0] if matches else None

    except Exception as e:
        if on_log: on_log(f"  [about] Error: {e}")
        return None
    finally:
        await context.close()
