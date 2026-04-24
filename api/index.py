from backend.app import app

# Vercel Python serverless function expects an exposed Flask app object.
application = app
