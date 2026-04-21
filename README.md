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

To run YT LeadMiner on a hosted server (like Render, Heroku, or a VPS) while maintaining a signed-in state or avoiding frequent CAPTCHAs, follow this session-persistence workflow.

### 1. IP Persistence (ScraperAPI Sessions)
By default, the scraper uses a fixed `BROWSER_PROXY_SESSION` in your `.env`. This ensures ScraperAPI assigns you the same IP address for all requests in a session. 
- **Benefit**: Google is less likely to flag your session as suspicious if the IP remains consistent.

### 2. Auth Persistence (Cookie Injection)
Since you cannot manually solve captchas or sign in on a headless server, you can export your local session cookies and inject them into the server.

#### Step-by-Step implementation:
1. **Sign in locally**: Run `python scratch/sign_in_helper.py`. High-quality cookies are generated in `.browser_data`.
2. **Export Cookies**: Run `python scratch/export_cookies.py`.
3. **Copy the Output**: The script will generate a long Base64 string in `scratch/exported_cookies.txt`.
4. **Set Environment Variable**: Add a new environment variable on your hosting provider:
   - **Key**: `YOUTUBE_COOKIES`
   - **Value**: `[The long Base64 string fromexported_cookies.txt]`

> [!IMPORTANT]
> The `BrowserManager` automatically detects this variable and injects the cookies into every browser context, effectively "cloning" your logged-in session to the cloud.

---

## 🌐 Deployment

A `render.yaml` is included for one-click deployment on [Render](https://render.com).

### Render.com Setup
1. Fork this repository.
2. Connect the repository to Render.
3. Configure the following **Environment Variables**:
   - `YOUTUBE_API_KEY`: Your Google API key.
   - `SCRAPER_API_KEY`: Your ScraperAPI key.
   - `USE_LOCAL_BROWSER`: `False` (Recommended for hosted environments).
   - `BROWSER_HEADLESS`: `True`.
   - `YOUTUBE_COOKIES`: (Optional) Your exported Base64 session string.

> [!WARNING]
> While `USE_LOCAL_BROWSER=True` works locally, most cloud providers (like Render) require specialized Docker images to run Playwright with a GUI. Setting `USE_LOCAL_BROWSER=False` switches the search crawler to use ScraperAPI's proxy network instead of a local browser instance.

---

## 📄 License

This project is proprietary. All rights reserved.
