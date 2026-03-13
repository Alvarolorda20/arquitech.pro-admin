"""Thin entrypoint for ASGI servers and local execution."""

from pathlib import Path

import uvicorn
from dotenv import load_dotenv

from src.app.main import app

# Load env vars from backend-admin/.env even if launched from a different cwd.
load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env", override=False)

__all__ = ["app"]


if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
