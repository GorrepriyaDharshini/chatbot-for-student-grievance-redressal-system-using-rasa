from backend.app import app

# Expose Flask application for Vercel entrypoint detection.
application = app
