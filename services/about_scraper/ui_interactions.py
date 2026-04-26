import asyncio
from services.utils.stealth_utils import human_click

async def click_view_email_button(page, on_log=None, stealth=True):
    button_selectors = [
        "yt-button-view-model button",
        "tp-yt-paper-button#button",
        "button[aria-label*='email']",
        "text='View email address'",
        "ytd-channel-about-metadata-renderer button"
    ]
    for selector in button_selectors:
        try:
            button = await page.wait_for_selector(selector, timeout=5000)
            if button:
                if stealth:
                    await human_click(page, selector)
                else:
                    await button.click()
                return True
        except: continue
    return False

async def inject_status_banner(page, text):
    await page.evaluate(f"""
        const banner = document.getElementById('bot-status-banner') || document.createElement('div');
        banner.id = 'bot-status-banner';
        banner.style.position = 'fixed';
        banner.style.top = '0';
        banner.style.left = '0';
        banner.style.width = '100%';
        banner.style.backgroundColor = '#2c3e50';
        banner.style.color = 'white';
        banner.style.textAlign = 'center';
        banner.style.padding = '10px';
        banner.style.zIndex = '9999';
        banner.style.fontSize = '18px';
        banner.textContent = '{text}';
        if (!document.getElementById('bot-status-banner')) document.body.prepend(banner);
    """)
