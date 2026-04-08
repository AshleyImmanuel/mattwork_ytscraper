import os
from dotenv import load_dotenv

load_dotenv()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")
SCRAPER_API_KEY = os.getenv("SCRAPER_API_KEY", "")
