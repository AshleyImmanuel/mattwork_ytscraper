# 🎯 YT LeadMiner

**High-precision lead generation targeted at top YouTube creators.**

YT LeadMiner is an automated pipeline that discovers YouTube channels matching custom criteria (views, subscribers, region, niche keywords), scrapes their contact emails, and exports everything to a clean Excel report.

---

## ✨ Features

- **Smart Web Crawling** — Crawls YouTube search results page-by-page using ScraperAPI or a local Playwright browser with natural scrolling to avoid bot detection.
- **Intelligent Filtering** — Pre-filters videos by view count, subscriber range, duration, upload date, region, and content type (Shorts vs. Long-form) without wasting YouTube API quota.
- **Multi-Source Email Extraction** — Finds contact emails from YouTube About pages, channel descriptions, external website links, and Google Dork discovery.
- **Automated CAPTCHA Solving** — Integrates `playwright-recaptcha` for hands-free audio CAPTCHA solving when using a local browser.
- **Bulk Keyword Support** — Process multiple search terms in a single job with a visual bulk-edit modal.
- **Real-Time Progress UI** — Dark-mode dashboard with live terminal logs, progress bar, and elapsed timer.
- **Excel Export** — One-click download of an `.xlsx` report with all lead data.

---

## 🏗️ Architecture

```
MattScrape/
├── main.py                     # FastAPI entry point
├── requirements.txt            # Python dependencies
├── render.yaml                 # Render.com deployment config
├── .env.example                # Environment variable template
│
├── api/
│   └── endpoints.py            # REST API routes (/extract, /status, /download)
│
├── core/
│   ├── config.py               # All configuration & env-var parsing
│   ├── models.py               # Pydantic request models
│   ├── pipeline.py             # Main extraction orchestration logic
│   ├── job_manager.py          # In-memory job tracking
│   └── middleware.py           # Rate limiting middleware
│
├── services/
│   ├── youtube.py              # YouTube Data API v3 helpers
│   ├── youtube_crawler.py      # Web crawler (ScraperAPI + Playwright)
│   ├── youtube_about_scraper.py# About-page email scraping
│   ├── google_discovery.py     # Google Dork channel discovery
│   ├── scraper.py              # Email scraping orchestrator
│   ├── external_scraper.py     # External website link scanner
│   ├── excel.py                # Excel report generator
│   └── utils/
│       ├── browser_manager.py  # Playwright browser lifecycle
│       ├── extraction.py       # Email regex & validation
│       ├── stealth_utils.py    # Anti-detection helpers
│       └── youtube_helpers.py  # Shared YouTube utilities
│
└── frontend/
    ├── index.html              # Single-page dashboard
    ├── style.css               # Dark-mode UI styles
    └── app.js                  # Client-side JS (form, polling, download)
```

---

## 🚀 Quick Start

### Prerequisites

- **Python 3.11+**
- **FFmpeg** (required for automated CAPTCHA audio solving)
- A **YouTube Data API v3** key ([get one here](https://console.cloud.google.com/apis/library/youtube.googleapis.com))
- A **ScraperAPI** key ([get one here](https://www.scraperapi.com/)) — used for proxy/geo-routing

### Installation

```bash
# Clone the repository
git clone https://github.com/AshleyImmanuel/mattwork_ytscraper.git
cd mattwork_ytscraper

# Create and activate a virtual environment
python -m venv venv
# Windows:
venv\Scripts\activate
# macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install chromium
```

### Configuration

Copy the example environment file and fill in your API keys:

```bash
cp .env.example .env
```

Edit `.env` with your keys:

```env
YOUTUBE_API_KEY=your_youtube_api_key_here
SCRAPER_API_KEY=your_scraperapi_key_here
```

See [`.env.example`](.env.example) for all available configuration options.

### Run

```bash
python main.py
```

The app will start on `http://localhost:8001`. Open it in your browser and start extracting leads.

---

## ⚙️ Configuration Reference

All settings are controlled via environment variables (`.env` file). Key options:

| Variable | Default | Description |
|---|---|---|
| `YOUTUBE_API_KEY` | — | YouTube Data API v3 key (required) |
| `SCRAPER_API_KEY` | — | ScraperAPI key for proxy requests (required) |
| `USE_LOCAL_BROWSER` | `True` | Use local Playwright browser instead of ScraperAPI for crawling |
| `BROWSER_HEADLESS` | `True` | Run browser in headless mode |
| `CRAWLER_MAX_PAGES` | `50` | Max search result pages to crawl per keyword |
| `GOOGLE_DISCOVERY_ENABLED` | `False` | Enable supplemental Google Dork channel discovery |
| `SCRAPER_CONCURRENCY` | `3` | Parallel email scraping workers |
| `MAX_CONCURRENT_JOBS` | `2` | Max simultaneous extraction jobs |
| `ENABLE_API_DOCS` | `False` | Expose Swagger docs at `/docs` |

---

## 📡 API Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/extract` | Start a new extraction job |
| `GET` | `/api/status/{job_id}` | Poll job progress, logs, and stats |
| `GET` | `/api/download/{job_id}` | Download the completed Excel report |
| `GET` | `/` | Serve the frontend dashboard |

### Example: Start an Extraction

```bash
curl -X POST http://localhost:8001/api/extract \
  -H "Content-Type: application/json" \
  -d '{
    "keyword": "marketing strategy",
    "minViews": 1000,
    "maxViews": 500000,
    "minSubs": 10000,
    "maxSubs": 50000,
    "region": "US",
    "dateFilter": "This Year",
    "videoType": "All",
    "leadSize": 100
  }'
```

---

## 🔄 Extraction Pipeline

1. **Web Crawl** — Searches YouTube via ScraperAPI or local Playwright browser, parsing `ytInitialData` JSON from search result pages.
2. **Pre-Filter** — Filters out irrelevant content (gaming, music, tutorials, etc.) using keyword exclusion lists. Zero API cost.
3. **API Enrichment** — Fetches channel metadata (subscribers, country, description) via YouTube Data API v3 for candidates that pass pre-filtering.
4. **Email Scraping** — Visits each channel's About page and linked external websites to extract contact emails.
5. **Google Discovery** *(optional)* — Searches Google for additional YouTube channels mentioning emails/contact info.
6. **Excel Export** — Compiles all lead data into a downloadable `.xlsx` report.

---

## 🔐 Persistent Hosting & Session Management

To run YT LeadMiner on a hosted server while maintaining a signed-in state, you need to choose the right hosting strategy.

### 1. Advanced Persistence (Session & Auth)
*   **IP Persistence**: The scraper uses `BROWSER_PROXY_SESSION` to ensure ScraperAPI assigns a consistent IP address, preventing Google from flagging the session.
*   **Auth Persistence**: Use `YOUTUBE_COOKIES` to inject an authenticated session.
    1.  Sign in locally (`python scratch/sign_in_helper.py`).
    2.  Export cookies (`python scratch/export_cookies.py`).
    3.  Set the Base64 output as an environment variable `YOUTUBE_COOKIES`.

### 2. Recommended Hosting Platforms

| Mode | Recommended Platforms | Why? |
|---|---|---|
| **Full Persistent Browser** (`USE_LOCAL_BROWSER=True`) | **DigitalOcean**, **Linode**, **Vultr**, **Railway** | Requires a VPS or persistent disk volume to store the `.browser_data` folder and support for Playwright's system dependencies. |
| **Lightweight Crawler** (`USE_LOCAL_BROWSER=False`) | **Render**, **Heroku**, **Railway** | Best for ScraperAPI-based crawling. No persistent disk required; relies on proxy rotation. |

---

## 🌐 Deployment

### VPS Placement (DigitalOcean, Linode, AWS)
For the best results with a **Persistent Browser**, use a VPS:
1.  Provision a Ubuntu server.
2.  Install dependencies: `pip install -r requirements.txt && playwright install-deps`.
3.  Run in a screen/tmux session or as a systemd service.
4.  Persistence is handled natively by the filesystem (`.browser_data`).

### Render.com Setup (Crawler Mode)
1.  Fork this repository.
2.  Set `USE_LOCAL_BROWSER=False` and `BROWSER_HEADLESS=True`.
3.  Configure `YOUTUBE_API_KEY` and `SCRAPER_API_KEY`.
4.  (Optional) Inject `YOUTUBE_COOKIES` for authenticated API/Proxy calls.

---

## 📄 License

This project is proprietary. All rights reserved.
