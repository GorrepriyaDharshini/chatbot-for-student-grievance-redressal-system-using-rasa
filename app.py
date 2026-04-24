"""Root entry point for Vercel Flask deployment."""
from backend.app import app

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=5000)
