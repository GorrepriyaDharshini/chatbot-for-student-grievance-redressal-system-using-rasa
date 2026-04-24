"""Root entry point for Vercel Flask deployment."""
from backend.app import app

# Export both names so Vercel / WSGI loaders can find the Flask app.
application = app

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=5000)
