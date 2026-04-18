"""
YT LeadMiner - FastAPI Server
Entry point for the modularized backend.
"""
import os
import sys
import asyncio

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.exceptions import RequestValidationError
from fastapi import Request

from core.config import APP_HOST,   APP_PORT, ENABLE_API_DOCS
from core.middleware import rate_limit_middleware_logic
from api.endpoints import router as api_router

app = FastAPI(
    title="YT LeadMiner",
    version="2.2",
    docs_url="/docs" if ENABLE_API_DOCS else None,
    redoc_url="/redoc" if ENABLE_API_DOCS else None,
    openapi_url="/openapi.json" if ENABLE_API_DOCS else None,
)

# ---- Middleware ----
@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    return await rate_limit_middleware_logic(request, call_next)

# ---- Exception Handlers ----
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Log detailed 422 errors to console for debugging."""
    print(f"\n[422] Validation Error at {request.url.path}")
    print(f"Details: {exc.errors()}")
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors(), "error": "Validation failed. Check your inputs."},
    )

from fastapi.responses import JSONResponse # Ensure JSONResponse is available

# ---- Routes ----
app.include_router(api_router)

# ---- Serve frontend ----
FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "frontend")
app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

@app.get("/")
async def serve_index():
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))

# ---- Run ----
if __name__ == "__main__":
    import uvicorn

    print(f"INFO: Starting modular server on {APP_HOST}:{APP_PORT}")
    uvicorn.run(
        "main:app",
        host=APP_HOST,
        port=APP_PORT,
        reload=os.getenv("UVICORN_RELOAD", "false").lower() == "true",
        loop="asyncio"
    )
