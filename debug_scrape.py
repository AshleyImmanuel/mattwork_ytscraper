"""
Debug script: See what Playwright actually sees on a YouTube channel page.
Takes screenshots so we can diagnose why 0 emails are found.
"""
from playwright.sync_api import sync_playwright
import re
import os

EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

# A well-known small channel that has a public email
TEST_CHANNEL = os.getenv("DEBUG_TEST_CHANNEL", "https://www.youtube.com/@mkbhd")
DEBUG_PAGE_TIMEOUT_MS = int(os.getenv("DEBUG_PAGE_TIMEOUT_MS", "30000"))
DEBUG_POST_LOAD_WAIT_MS = int(os.getenv("DEBUG_POST_LOAD_WAIT_MS", "3000"))
DEBUG_USER_AGENT = os.getenv(
    "DEBUG_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36",
)

def run():
    with sync_playwright() as pw:
        print("[1] Launching Chromium...")
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=DEBUG_USER_AGENT,
            viewport={"width": 1280, "height": 720},
        )
        page = context.new_page()

        # Step 1: Visit the channel's About page
        about_url = TEST_CHANNEL.rstrip("/") + "/about"
        print(f"[2] Navigating to: {about_url}")
        page.goto(about_url, wait_until="domcontentloaded", timeout=DEBUG_PAGE_TIMEOUT_MS)
        page.wait_for_timeout(DEBUG_POST_LOAD_WAIT_MS)

        # Take screenshot
        page.screenshot(path="debug_about_page.png", full_page=True)
        print("[3] Screenshot saved: debug_about_page.png")

        # Check what URL we actually landed on
        print(f"[4] Current URL: {page.url}")
        print(f"[5] Page title: {page.title()}")

        # Get page text
        page_text = page.inner_text("body")
        print(f"[6] Page text length: {len(page_text)} chars")
        print(f"[7] First 500 chars of body:\n{page_text[:500]}")

        # Search for emails
        emails = EMAIL_REGEX.findall(page_text)
        print(f"\n[8] Emails found in page text: {emails}")

        # Also check HTML source
        html = page.content()
        emails_html = EMAIL_REGEX.findall(html)
        print(f"[9] Emails found in HTML source: {emails_html}")

        # Check if consent page is showing
        if "consent" in page.url.lower() or "agree" in page_text.lower()[:200]:
            print("\n[!] CONSENT/COOKIE PAGE DETECTED - YouTube is blocking!")
            # Try to accept consent
            try:
                agree_btn = page.locator('button:has-text("Accept all")')
                if agree_btn.count() > 0:
                    print("[!] Clicking 'Accept all' button...")
                    agree_btn.first.click()
                    page.wait_for_timeout(DEBUG_POST_LOAD_WAIT_MS)
                    page.screenshot(path="debug_after_consent.png", full_page=True)
                    print("[!] Screenshot after consent: debug_after_consent.png")
                    
                    page_text = page.inner_text("body")
                    emails = EMAIL_REGEX.findall(page_text)
                    print(f"[!] Emails after consent: {emails}")
            except Exception as e:
                print(f"[!] Consent handling failed: {e}")

        browser.close()
        print("\n[10] Done!")

if __name__ == "__main__":
    run()
