import asyncio
import time

try:
    from playwright_recaptcha import recaptchav2
except ImportError:
    recaptchav2 = None

async def solve_captcha_automated(page, on_log=None):
    """Automation attempt (Multi-Retry Loop)"""
    if not recaptchav2:
        return False
        
    for solve_attempt in range(1, 4):
        if on_log: on_log(f"  [about] [CAPTCHA] Solver Attempt {solve_attempt}/3...")
        await page.evaluate(f"document.getElementById('bot-status-banner').textContent = 'ROBOT STATUS: Solving CAPTCHA (Attempt {solve_attempt}/3)...'")
        try:
            async with recaptchav2.AsyncSolver(page) as solver:
                await solver.solve_recaptcha()
            if on_log: on_log(f"  [about] [CAPTCHA] [SUCCESS] Solved automatically.")
            return True
        except Exception:
            await asyncio.sleep(2)
    return False

async def wait_for_manual_solve(page, on_log):
    """Wait up to 5 minutes for the user to solve the captcha manually."""
    if on_log: on_log("  [about] ACTION REQUIRED: Please solve CAPTCHA manually!")
    await page.evaluate("""
        const b = document.getElementById('bot-status-banner');
        if(b) {
            b.style.backgroundColor = 'red';
            b.textContent = 'ACTION REQUIRED: Please solve CAPTCHA manually!';
        }
    """)
    start_time = time.time()
    while time.time() - start_time < 300:
        content = await page.content()
        if not any(indicator in content.lower() for indicator in ["recaptcha", "g-recaptcha", "captcha"]):
            return True
        if await page.query_selector("button#submit-btn:not([disabled])"):
            return True
        await asyncio.sleep(2)
    return False
