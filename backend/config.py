"""
Application configuration for ResolveX Flask backend.
"""
import os
from pathlib import Path

# Project root: student-grievance-system/
BASE_DIR = Path(__file__).resolve().parent.parent

# SQLite database file location (same file RASA actions use)
DATABASE_PATH = Path(os.environ.get("RESOLVEX_DATABASE_PATH", BASE_DIR / "backend" / "database.db"))

# Vercel serverless functions are deployed in a read-only filesystem,
# so use /tmp for the database when running on Vercel.
if os.environ.get("VERCEL", "").lower() in ("1", "true", "yes"):
    DATABASE_PATH = Path("/tmp/backend/database.db")

SECRET_KEY = os.environ.get("RESOLVEX_SECRET_KEY", "dev-change-me-college-project")
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"
