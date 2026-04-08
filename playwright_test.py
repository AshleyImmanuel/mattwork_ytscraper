import asyncio
import sys

# Optional fix for Windows loop policies if we run this
# if sys.platform == "win32":
#     asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from playwright.async_api import async_playwright

async def run():
    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            print("Successfully launched chromium!")
            await browser.close()
    except Exception as e:
        import traceback
        traceback.print_exc()

asyncio.run(run())
