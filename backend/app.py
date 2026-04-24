"""
ResolveX — Flask API and static frontend server.
Run from project root: python backend/app.py
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request
from functools import wraps
from pathlib import Path

from flask import ( # type: ignore
    Flask,
    jsonify,
    request,
    send_from_directory,
    session,
)
from flask_cors import CORS # pyright: ignore[reportMissingModuleSource]
from werkzeug.security import check_password_hash, generate_password_hash # pyright: ignore[reportMissingImports]
from werkzeug.utils import secure_filename # pyright: ignore[reportMissingImports]

# Ensure project root is on PYTHONPATH (student-grievance-system/)
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend import models  # noqa: E402
from backend.config import SECRET_KEY  # noqa: E402

FRONTEND_DIR = ROOT / "frontend"
UPLOAD_DIR = ROOT / "uploads"
# Rasa REST API (server-side proxy avoids browser CORS / mixed-host issues)
if os.environ.get("VERCEL", "").lower() in ("1", "true", "yes"):
    RASA_SERVER = os.environ.get("RASA_SERVER", "").rstrip("/")
else:
    RASA_SERVER = os.environ.get("RASA_SERVER", "http://127.0.0.1:5005").rstrip("/")

app = Flask(__name__, static_folder=str(FRONTEND_DIR), static_url_path="")
app.config["SECRET_KEY"] = SECRET_KEY
CORS(app, supports_credentials=True, resources={r"/api/*": {"origins": "*"}})

models.init_db()


def _parse_rasa_rest_body(raw: str) -> list:
    """Extract assistant text lines from Rasa REST webhook JSON response."""
    if not raw or not raw.strip():
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return [raw.strip()]
    out = []
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict) and item.get("text"):
                out.append(str(item["text"]))
    elif isinstance(data, dict) and data.get("text"):
        out.append(str(data["text"]))
    return out


def login_required_student(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if "student_id" not in session:
            return jsonify({"ok": False, "error": "Unauthorized"}), 401
        return fn(*args, **kwargs)

    return wrapper


def login_required_admin(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if "admin_id" not in session:
            return jsonify({"ok": False, "error": "Unauthorized"}), 401
        return fn(*args, **kwargs)

    return wrapper


# --- Static pages ---

@app.route("/")
def serve_index():
    return send_from_directory(FRONTEND_DIR, "index.html")


@app.route("/<path:filename>")
def serve_frontend(filename):
    # Only serve known extensions to avoid catching API routes
    if filename.startswith("api/"):
        return jsonify({"error": "Not found"}), 404
    target = FRONTEND_DIR / filename
    if target.is_file():
        return send_from_directory(FRONTEND_DIR, filename)
    return send_from_directory(FRONTEND_DIR, "index.html")


# --- Auth ---


@app.route("/api/register", methods=["POST"])
@app.route("/register", methods=["POST"])
def register():
    data = request.get_json(force=True, silent=True) or {}
    try:
        models.create_student(
            name=data["name"],
            roll_number=data["roll_number"],
            email=data["email"].strip().lower(),
            department=data.get("department") or "AIML",
            phone=data.get("phone") or "",
            password=data["password"],
        )
        return jsonify({"ok": True, "message": "Account created"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/login", methods=["POST"])
@app.route("/login", methods=["POST"])
def login():
    data = request.get_json(force=True, silent=True) or {}
    username = (data.get("username") or data.get("email") or "").strip()
    password = data.get("password") or ""
    user = models.verify_student(username, password)
    if not user:
        return jsonify({"ok": False, "error": "Invalid credentials"}), 401
    session["student_id"] = user["id"]
    session["student_name"] = user["name"]
    session["student_email"] = user["email"]
    session["student_roll"] = user["roll_number"]
    return jsonify(
        {
            "ok": True,
            "user": {
                "id": user["id"],
                "name": user["name"],
                "email": user["email"],
                "roll_number": user["roll_number"],
                "department": user["department"],
            },
        }
    )


@app.route("/api/logout", methods=["POST"])
def logout():
    session.pop("student_id", None)
    session.pop("student_name", None)
    session.pop("student_email", None)
    session.pop("student_roll", None)
    return jsonify({"ok": True})


@app.route("/api/admin/login", methods=["POST"])
@app.route("/admin/login", methods=["POST"])
def admin_login():
    data = request.get_json(force=True, silent=True) or {}
    username = (data.get("username") or data.get("email") or "").strip()
    password = data.get("password") or ""
    admin = models.verify_admin(username, password)
    if not admin:
        return jsonify({"ok": False, "error": "Invalid credentials"}), 401
    session["admin_id"] = admin["id"]
    session["admin_username"] = admin["username"]
    session["admin_email"] = admin["email"]
    return jsonify(
        {
            "ok": True,
            "admin": {
                "username": admin["username"],
                "email": admin["email"],
            },
        }
    )


@app.route("/api/admin/logout", methods=["POST"])
def admin_logout():
    session.pop("admin_id", None)
    session.pop("admin_username", None)
    session.pop("admin_email", None)
    return jsonify({"ok": True})


@app.route("/api/me", methods=["GET"])
def me():
    if "student_id" not in session:
        return jsonify({"logged_in": False})
    row = models.get_student_by_id(session["student_id"])
    if not row:
        return jsonify({"logged_in": False})
    return jsonify(
        {
            "logged_in": True,
            "user": {
                "id": row["id"],
                "name": row["name"],
                "roll_number": row["roll_number"],
                "email": row["email"],
                "department": row["department"],
                "phone": row["phone"],
            },
        }
    )


@app.route("/api/profile", methods=["GET", "POST"])
@login_required_student
def profile():
    sid = session["student_id"]
    if request.method == "GET":
        row = models.get_student_by_id(sid)
        d = models.row_to_dict(row)
        d.pop("password", None)
        return jsonify({"ok": True, "profile": d})
    data = request.get_json(force=True, silent=True) or {}
    row = models.get_student_by_id(sid)
    if not row:
        return jsonify({"ok": False, "error": "Student not found"}), 404
    models.update_student(
        sid,
        name=data.get("name"),
        phone=data.get("phone"),
        department=data.get("department"),
    )
    # Optional password change
    if data.get("new_password"):
        current = data.get("current_password") or ""
        if not current or not check_password_hash(row["password"], current):
            return jsonify({"ok": False, "error": "Current password is incorrect"}), 400
        if len(str(data["new_password"])) < 6:
            return jsonify({"ok": False, "error": "New password must be at least 6 characters"}), 400
        with models.get_db() as conn:
            conn.execute(
                "UPDATE students SET password = ? WHERE id = ?",
                (generate_password_hash(data["new_password"]), sid),
            )
    return jsonify({"ok": True})


# --- Rasa (server-side proxy; student chat on home.html) ---


@app.route("/api/rasa/health", methods=["GET"])
@login_required_student
def rasa_health():
    if not RASA_SERVER:
        return jsonify(
            {
                "ok": True,
                "rasa_reachable": False,
                "rasa_server": RASA_SERVER,
                "error": "RASA_SERVER is not configured. Set the environment variable on Vercel.",
            }
        )
    url = f"{RASA_SERVER}/version"
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=3) as resp:
            ok = 200 <= resp.status < 300
        return jsonify({"ok": True, "rasa_reachable": ok, "rasa_server": RASA_SERVER})
    except (urllib.error.URLError, OSError):
        return jsonify(
            {
                "ok": True,
                "rasa_reachable": False,
                "rasa_server": RASA_SERVER,
            }
        )


@app.route("/api/rasa/message", methods=["POST"])
@login_required_student
def rasa_message():
    data = request.get_json(force=True, silent=True) or {}
    message = (data.get("message") or "").strip()
    if not message:
        return jsonify({"ok": False, "error": "message required"}), 400
    sid = session["student_id"]
    sender = f"student_{sid}"
    url = f"{RASA_SERVER}/webhooks/rest/webhook"
    if not RASA_SERVER:
        return jsonify(
            {
                "ok": False,
                "error": "RASA_SERVER is not configured. Set RASA_SERVER to your hosted Rasa endpoint.",
            }
        ), 503
    payload = json.dumps({"sender": sender, "message": message}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        return jsonify(
            {
                "ok": False,
                "error": f"Rasa HTTP {e.code}: {err_body or e.reason}",
            }
        ), 502
    except urllib.error.URLError as e:
        return jsonify(
            {
                "ok": False,
                "error": "Cannot reach Rasa. Start it from the rasa_bot folder: "
                "rasa run --enable-api --cors \"*\" (and rasa run actions).",
                "detail": str(e.reason),
            }
        ), 502
    except OSError as e:
        return jsonify({"ok": False, "error": str(e)}), 502

    replies = _parse_rasa_rest_body(raw)
    if not replies:
        replies = ["(No text reply from the bot — check Rasa logs and training.)"]
    return jsonify({"ok": True, "replies": replies})


# --- Complaints ---


@app.route("/api/complaints", methods=["GET", "POST"])
@app.route("/complaints", methods=["GET", "POST"])
def complaints():
    if request.method == "GET":
        cid = request.args.get("complaint_id")
        if cid:
            row = models.get_complaint_by_code(cid.strip().upper())
            if not row:
                return jsonify({"ok": False, "error": "Not found"}), 404
            d = models.row_to_dict(row)
            return jsonify(
                {"ok": True, "complaint": models.mask_anonymous_complaint(d)}
            )
        if "student_id" in session:
            rows = models.list_complaints_for_student(session["student_id"])
            return jsonify(
                {
                    "ok": True,
                    "complaints": [models.row_to_dict(r) for r in rows],
                }
            )
        return jsonify({"ok": False, "error": "Login or provide complaint_id"}), 400

    # POST — multipart (student form) or JSON (API / bot)
    ct = (request.content_type or "").lower()
    if "multipart/form-data" in ct:
        if "student_id" not in session:
            return jsonify({"ok": False, "error": "Login required"}), 401
        sid = session["student_id"]
        title = (request.form.get("title") or "").strip()
        category = (request.form.get("category") or "other").strip().lower() or "other"
        priority = (request.form.get("priority") or "medium").strip().lower() or "medium"
        description = (request.form.get("description") or "").strip()
        is_anonymous = (request.form.get("anonymous") or "").lower() in (
            "1",
            "true",
            "on",
            "yes",
        )
        attachment_path = ""
        f = request.files.get("attachment")
        if f and f.filename:
            UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
            safe = secure_filename(f.filename) or "file"
            unique = f"{sid}_{int(time.time())}_{safe}"
            dest = UPLOAD_DIR / unique
            f.save(str(dest))
            attachment_path = f"uploads/{unique}"
        if not description and not title:
            return jsonify({"ok": False, "error": "title or description required"}), 400
        if not description:
            description = title
        try:
            complaint_id = models.insert_complaint(
                int(sid),
                category=category,
                description=description,
                status="pending",
                title=title,
                priority=priority,
                is_anonymous=is_anonymous,
                attachment_path=attachment_path or None,
            )
            return jsonify({"ok": True, "complaint_id": complaint_id})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 400

    data = request.get_json(force=True, silent=True) or {}
    sid = session.get("student_id") or data.get("student_id")
    if not sid:
        return jsonify({"ok": False, "error": "student_id required"}), 400
    try:
        complaint_id = models.insert_complaint(
            int(sid),
            category=data.get("category") or "general",
            description=data.get("description") or "",
            status=data.get("status") or "pending",
            title=(data.get("title") or "").strip(),
            priority=(data.get("priority") or "medium").strip().lower(),
            is_anonymous=bool(data.get("is_anonymous") or data.get("anonymous")),
            attachment_path=(data.get("attachment_path") or "").strip() or None,
        )
        return jsonify({"ok": True, "complaint_id": complaint_id})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/complaints/<complaint_id>", methods=["DELETE"])
@login_required_student
def delete_complaint(complaint_id):
    cid = (complaint_id or "").strip().upper()
    if not cid:
        return jsonify({"ok": False, "error": "complaint_id required"}), 400
    deleted = models.delete_complaint_for_student(cid, session["student_id"])
    if not deleted:
        return jsonify({"ok": False, "error": "Complaint not found"}), 404
    return jsonify({"ok": True})


@app.route("/api/update_status", methods=["POST"])
@app.route("/update_status", methods=["POST"])
@login_required_admin
def update_status():
    data = request.get_json(force=True, silent=True) or {}
    cid = (data.get("complaint_id") or "").strip().upper()
    status = data.get("status") or "pending"
    if not cid:
        return jsonify({"ok": False, "error": "complaint_id required"}), 400
    models.update_complaint_status(cid, status)
    return jsonify({"ok": True})


@app.route("/api/feedback", methods=["POST"])
@app.route("/feedback", methods=["POST"])
@login_required_student
def feedback():
    data = request.get_json(force=True, silent=True) or {}
    cid = (data.get("complaint_id") or "").strip().upper()
    rating = data.get("rating")
    if not cid or rating is None:
        return jsonify({"ok": False, "error": "complaint_id and rating required"}), 400
    models.add_complaint_feedback(cid, rating, data.get("feedback"))
    return jsonify({"ok": True})


@app.route("/api/faculty_feedback", methods=["POST"])
@login_required_student
def faculty_feedback_route():
    data = request.get_json(force=True, silent=True) or {}
    try:
        student_roll = session.get("student_roll") or ""
        faculty_name = (data.get("faculty_name") or "").strip()
        if not faculty_name:
            return jsonify({"ok": False, "error": "faculty_name required"}), 400
        dept = data.get("department") or "AIML"
        ratings = data.get("ratings")
        if isinstance(ratings, dict) and ratings:
            nums = []
            for v in ratings.values():
                if v is None or v == "":
                    continue
                n = int(v)
                if n < 1 or n > 5:
                    return jsonify({"ok": False, "error": "Ratings must be 1–5"}), 400
                nums.append(n)
            if not nums:
                return jsonify({"ok": False, "error": "No ratings provided"}), 400
            avg = sum(nums) / len(nums)
            summary = int(min(5, max(1, round(avg))))
            payload = json.dumps({"criterion_ratings": ratings, "average": avg})
            models.add_faculty_feedback(
                faculty_name=faculty_name,
                department=dept,
                rating=summary,
                comments=payload,
                student_id=student_roll,
            )
        else:
            models.add_faculty_feedback(
                faculty_name=faculty_name,
                department=dept,
                rating=int(data["rating"]),
                comments=data.get("comments") or "",
                student_id=student_roll,
            )
        return jsonify({"ok": True})
    except (KeyError, TypeError, ValueError) as e:
        return jsonify({"ok": False, "error": str(e) or "Invalid payload"}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


# --- FAQs ---


@app.route("/api/faqs", methods=["GET", "POST"])
@app.route("/faqs", methods=["GET", "POST"])
def faqs():
    if request.method == "GET":
        rows = models.list_faqs()
        return jsonify({"ok": True, "faqs": [models.row_to_dict(r) for r in rows]})

    # POST: admin CRUD via op field
    if "admin_id" not in session:
        return jsonify({"ok": False, "error": "Admin only"}), 401
    data = request.get_json(force=True, silent=True) or {}
    op = data.get("op") or "create"
    if op == "create":
        fid = models.create_faq(data["question"], data["answer"])
        return jsonify({"ok": True, "id": fid})
    if op == "update":
        models.update_faq(int(data["id"]), data["question"], data["answer"])
        return jsonify({"ok": True})
    if op == "delete":
        models.delete_faq(int(data["id"]))
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "Unknown op"}), 400


# --- Discussion ---


@app.route("/api/discussion", methods=["GET", "POST"])
@app.route("/discussion", methods=["GET", "POST"])
def discussion():
    if request.method == "GET":
        topics = models.list_discussions()
        # Include comments count / full thread
        out = []
        for t in topics:
            comments = models.get_discussion_comments(t["id"])
            t2 = dict(t)
            t2["comments"] = comments
            out.append(t2)
        return jsonify({"ok": True, "discussions": out})

    if "student_id" not in session:
        return jsonify({"ok": False, "error": "Login required"}), 401
    data = request.get_json(force=True, silent=True) or {}
    if data.get("discussion_id"):
        models.add_discussion_comment(
            int(data["discussion_id"]),
            session["student_id"],
            data.get("comment") or data.get("content") or "",
        )
        return jsonify({"ok": True})
    content = data.get("content")
    if not content:
        return jsonify({"ok": False, "error": "content required"}), 400
    did = models.create_discussion(session["student_id"], content)
    return jsonify({"ok": True, "id": did})


# --- Admin data ---


@app.route("/api/admin/me", methods=["GET"])
def admin_me():
    if "admin_id" not in session:
        return jsonify({"logged_in": False})
    return jsonify(
        {
            "logged_in": True,
            "username": session.get("admin_username"),
            "email": session.get("admin_email"),
        }
    )


@app.route("/api/admin/stats", methods=["GET"])
@login_required_admin
def admin_stats():
    return jsonify({"ok": True, "stats": models.complaint_stats()})


@app.route("/api/admin/complaints", methods=["GET"])
@login_required_admin
def admin_complaints():
    status = request.args.get("status")
    rows = models.list_all_complaints(status=status if status else None)
    return jsonify(
        {
            "ok": True,
            "complaints": [
                models.mask_anonymous_complaint(models.row_to_dict(r)) for r in rows
            ],
        }
    )


@app.route("/api/admin/feedback", methods=["GET"])
@login_required_admin
def admin_feedback():
    return jsonify(
        {
            "ok": True,
            "complaint_feedback": [
                models.row_to_dict(r) for r in models.list_complaint_feedback()
            ],
            "faculty_feedback": [
                models.row_to_dict(r) for r in models.list_faculty_feedback()
            ],
        }
    )


if __name__ == "__main__":
    # Flask on 5000; RASA on 5005
    app.run(host="0.0.0.0", port=5000, debug=True)
