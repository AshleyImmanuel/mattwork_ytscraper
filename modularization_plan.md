# Implementation Plan - Modularization & Zero Hardcoding

The objective is to refactor the current 700+ line `main.py` into a cleaner, modular structure and ensure that **no values are hardcoded**. Everything from rate limits to file retention will be configurable via environment variables.

## Proposed Architecture

I will reorganize the codebase into the following structure:

```
d:\MattScrape\
├── main.py                # App initialization & entry point
├── api/
│   └── endpoints.py        # FastAPI routes (Refactored from main.py)
├── core/
│   ├── config.py           # Centralized configuration (NO hardcoding here)
│   ├── middleware.py       # Rate limiting & Security headers
│   ├── job_manager.py      # Job state, cleanup, and logging utilities
│   ├── pipeline.py         # The actual extraction pipeline logic
│   └── models.py           # Pydantic request/response models
└── services/               # (Existing) YouTube, Scraper, Excel services
```

## Proposed Changes

### 1. Centralized Configuration
- **[NEW] [core/config.py](file:///d:/MattScrape/core/config.py)**
    - Will contain a `Settings` class (or clean constants) that loads *everything* from `.env`.
    - Includes: API keys, port, host, rate limits, body limits, job retention times, concurrent job limits, etc.
    - **[MODIFY] [.env](file:///d:/MattScrape/.env)**: Add defaults for all newly configurable settings.

### 2. Middleware & Security
- **[NEW] [core/middleware.py](file:///d:/MattScrape/core/middleware.py)**
    - Move all rate-limiting logic (deque, locks, hit tracking).
    - Move security header application logic.
    - Move request body limit enforcement.

### 3. Job State Management
- **[NEW] [core/job_manager.py](file:///d:/MattScrape/core/job_manager.py)**
    - Encapsulate the `jobs` dictionary.
    - Move the `_cleanup_jobs` and `_log` functions here.
    - Provide a clean API for adding, updating, and retrieving job status.

### 4. Background Pipeline
- **[NEW] [core/pipeline.py](file:///d:/MattScrape/core/pipeline.py)**
    - Move `run_extraction` and `_do_run_extraction`.
    - This keeps the "heavy lifting" separate from the API definitions.

### 5. API Layer
- **[NEW] [api/endpoints.py](file:///d:/MattScrape/api/endpoints.py)**
    - Defined the FastAPI `APIRouter`.
    - Move the REST endpoints (`/api/extract`, `/api/status`, `/api/download`).

### 6. Clean Entry Point
- **[MODIFY] [main.py](file:///d:/MattScrape/main.py)**
    - Remove the 600+ lines of logic.
    - Initialize the FastAPI app.
    - Include the routers from `api/endpoints.py`.
    - Apply the middleware from `core/middleware.py`.
    - Handle the Windows event loop policy.

## Verification Plan

### Automated Tests
- I will verify the server starts correctly after the split.
- I will run a test extraction to ensure the cross-module communication (Routes -> JobManager -> Pipeline) works seamlessly.

### Manual Verification
- Verify that changing a value in `.env` (e.g., `MAX_CONCURRENT_JOBS`) is correctly reflected in the app behavior without changing code.
- Ensure the frontend still connects to all endpoints correctly.

## User Review Required

> [!IMPORTANT]
> This refactor will significantly change the file structure. All imports across the project (including `services/`) will be updated to reflect the new paths.

> [!CAUTION]
> I will temporarily move the existing `config.py` content into the new `core/` structure to maintain a single source of truth.
