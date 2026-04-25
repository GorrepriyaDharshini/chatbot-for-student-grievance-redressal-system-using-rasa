"""
PostgreSQL data access layer for ResolveX.
Supports both SQLite (local dev) and PostgreSQL (production/Supabase).
"""
import csv
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from werkzeug.security import generate_password_hash, check_password_hash

# Try to import psycopg2 for PostgreSQL
try:
    import psycopg2
    import psycopg2.extras
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

# Check if we should use PostgreSQL
DATABASE_URL = os.environ.get("POSTGRES_URL") or os.environ.get("DATABASE_URL")
USE_POSTGRES = bool(DATABASE_URL and HAS_PSYCOPG2)

# SQLite fallback path
BASE_DIR = Path(__file__).parent.parent
DATABASE_PATH = BASE_DIR / "backend" / "database.db"

# AIML staff portal
AIML_ADMIN_USERNAME = "aiml_admin"
AIML_ADMIN_PASSWORD = "ResolveX_AIML_2026"


# ── Connection helpers ────────────────────────────────────────────────────────

def _connect_postgres():
    url = DATABASE_URL
    # Fix URL scheme for psycopg2
    if url and url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    conn = psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)
    conn.autocommit = False
    return conn


def _connect_sqlite():
    path = DATABASE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def get_db():
    if USE_POSTGRES:
        conn = _connect_postgres()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    else:
        conn = _connect_sqlite()
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()


def _ph(n=1):
    """Return placeholder: %s for postgres, ? for sqlite."""
    return "%s" if USE_POSTGRES else "?"


def _placeholders(n):
    ph = "%s" if USE_POSTGRES else "?"
    return ", ".join([ph] * n)


def _fetchone(conn, sql, params=()):
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.fetchone()


def _fetchall(conn, sql, params=()):
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.fetchall()


def _execute(conn, sql, params=()):
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur


# ── Schema init ───────────────────────────────────────────────────────────────

def init_db():
    """Create tables and seed defaults."""
    with get_db() as conn:
        if USE_POSTGRES:
            _init_postgres(conn)
        else:
            _init_sqlite(conn)

        # Seed admin
        row = _fetchone(conn, f"SELECT COUNT(*) AS c FROM admins")
        count = row["c"] if row else 0
        if count == 0:
            ph = _ph()
            _execute(conn, f"""
                INSERT INTO admins (email, username, password)
                VALUES ({ph}, {ph}, {ph})
            """, (
                "aiml-admin@resolvex.local",
                AIML_ADMIN_USERNAME,
                generate_password_hash(AIML_ADMIN_PASSWORD),
            ))

        # Seed FAQs
        row = _fetchone(conn, "SELECT COUNT(*) AS c FROM faqs")
        count = row["c"] if row else 0
        if count == 0:
            ph2 = f"({_ph()}, {_ph()})"
            faqs = [
                ("How do I track my complaint?",
                 "Use your Complaint ID (e.g. CMP1234) on the My Complaints page or ask the chatbot."),
                ("Can I submit anonymously?",
                 "Yes. The Raise Grievance flow allows anonymous submission where policy permits."),
                ("Who handles AIML department grievances?",
                 "Department coordinators review complaints and update status in the admin panel."),
            ]
            for q, a in faqs:
                _execute(conn, f"INSERT INTO faqs (question, answer) VALUES ({_ph()}, {_ph()})", (q, a))

        maybe_import_csv(conn)


def _init_postgres(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS students (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            roll_number TEXT NOT NULL UNIQUE,
            email TEXT NOT NULL UNIQUE,
            department TEXT NOT NULL,
            phone TEXT,
            password TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS admins (
            id SERIAL PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            username TEXT,
            password TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS complaints (
            id SERIAL PRIMARY KEY,
            complaint_id TEXT NOT NULL UNIQUE,
            student_id INTEGER NOT NULL,
            category TEXT NOT NULL,
            description TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            title TEXT DEFAULT '',
            priority TEXT DEFAULT 'medium',
            is_anonymous INTEGER DEFAULT 0,
            attachment_path TEXT,
            FOREIGN KEY (student_id) REFERENCES students(id)
        );
        CREATE TABLE IF NOT EXISTS complaint_feedback (
            id SERIAL PRIMARY KEY,
            complaint_id TEXT NOT NULL,
            rating INTEGER NOT NULL,
            feedback TEXT,
            FOREIGN KEY (complaint_id) REFERENCES complaints(complaint_id)
        );
        CREATE TABLE IF NOT EXISTS faculty_feedback (
            id SERIAL PRIMARY KEY,
            faculty_name TEXT NOT NULL,
            department TEXT NOT NULL,
            rating INTEGER NOT NULL,
            comments TEXT,
            student_id TEXT
        );
        CREATE TABLE IF NOT EXISTS faqs (
            id SERIAL PRIMARY KEY,
            question TEXT NOT NULL,
            answer TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS discussions (
            id SERIAL PRIMARY KEY,
            student_id INTEGER NOT NULL,
            content TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (student_id) REFERENCES students(id)
        );
        CREATE TABLE IF NOT EXISTS discussion_comments (
            id SERIAL PRIMARY KEY,
            discussion_id INTEGER NOT NULL,
            student_id INTEGER,
            comment TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (discussion_id) REFERENCES discussions(id),
            FOREIGN KEY (student_id) REFERENCES students(id)
        );
    """)


def _init_sqlite(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS students (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            roll_number TEXT NOT NULL UNIQUE,
            email TEXT NOT NULL UNIQUE,
            department TEXT NOT NULL,
            phone TEXT,
            password TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS admins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            username TEXT,
            password TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS complaints (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            complaint_id TEXT NOT NULL UNIQUE,
            student_id INTEGER NOT NULL,
            category TEXT NOT NULL,
            description TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            title TEXT DEFAULT '',
            priority TEXT DEFAULT 'medium',
            is_anonymous INTEGER DEFAULT 0,
            attachment_path TEXT,
            FOREIGN KEY (student_id) REFERENCES students(id)
        );
        CREATE TABLE IF NOT EXISTS complaint_feedback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            complaint_id TEXT NOT NULL,
            rating INTEGER NOT NULL,
            feedback TEXT,
            FOREIGN KEY (complaint_id) REFERENCES complaints(complaint_id)
        );
        CREATE TABLE IF NOT EXISTS faculty_feedback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            faculty_name TEXT NOT NULL,
            department TEXT NOT NULL,
            rating INTEGER NOT NULL,
            comments TEXT,
            student_id TEXT
        );
        CREATE TABLE IF NOT EXISTS faqs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            question TEXT NOT NULL,
            answer TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS discussions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_id INTEGER NOT NULL,
            content TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (student_id) REFERENCES students(id)
        );
        CREATE TABLE IF NOT EXISTS discussion_comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            discussion_id INTEGER NOT NULL,
            student_id INTEGER,
            comment TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (discussion_id) REFERENCES discussions(id),
            FOREIGN KEY (student_id) REFERENCES students(id)
        );
    """)


# ── Helpers ───────────────────────────────────────────────────────────────────

def row_to_dict(row):
    if row is None:
        return None
    if isinstance(row, dict):
        return dict(row)
    return {k: row[k] for k in row.keys()}


def mask_anonymous_complaint(d):
    if not d:
        return d
    if d.get("is_anonymous"):
        d = dict(d)
        d["student_name"] = "Anonymous"
        d["student_email"] = ""
    return d


def _last_insert_id(conn, table="students"):
    if USE_POSTGRES:
        row = _fetchone(conn, f"SELECT lastval() AS id")
        return row["id"]
    else:
        return _fetchone(conn, "SELECT last_insert_rowid() AS id")["id"]


# ── Students ──────────────────────────────────────────────────────────────────

def create_student(name, roll_number, email, department, phone, password):
    with get_db() as conn:
        ph = _placeholders(6)
        _execute(conn, f"""
            INSERT INTO students (name, roll_number, email, department, phone, password)
            VALUES ({ph})
        """, (name, roll_number, email, department, phone or "", generate_password_hash(password)))
        return _last_insert_id(conn, "students")


def get_student_by_email(email):
    with get_db() as conn:
        return row_to_dict(_fetchone(conn, f"SELECT * FROM students WHERE email = {_ph()}", (email,)))


def get_student_by_roll_number(roll_number):
    roll = (roll_number or "").strip()
    if not roll:
        return None
    with get_db() as conn:
        return row_to_dict(_fetchone(conn, f"SELECT * FROM students WHERE roll_number = {_ph()}", (roll,)))


def get_student_by_id(sid):
    with get_db() as conn:
        return row_to_dict(_fetchone(conn, f"SELECT * FROM students WHERE id = {_ph()}", (sid,)))


def verify_student(username, password):
    roll = (username or "").strip()
    if not roll:
        return None
    row = get_student_by_roll_number(roll)
    if not row:
        return None
    if check_password_hash(row["password"], password):
        return row
    return None


def update_student(sid, name=None, phone=None, department=None):
    fields, values = [], []
    ph = _ph()
    if name is not None:
        fields.append(f"name = {ph}")
        values.append(name)
    if phone is not None:
        fields.append(f"phone = {ph}")
        values.append(phone)
    if department is not None:
        fields.append(f"department = {ph}")
        values.append(department)
    if not fields:
        return
    values.append(sid)
    with get_db() as conn:
        _execute(conn, f"UPDATE students SET {', '.join(fields)} WHERE id = {ph}", values)


# ── Admins ────────────────────────────────────────────────────────────────────

def verify_admin(username, password):
    u = (username or "").strip()
    if not u:
        return None
    with get_db() as conn:
        row = row_to_dict(_fetchone(conn, f"SELECT * FROM admins WHERE username = {_ph()}", (u,)))
    if not row:
        return None
    if check_password_hash(row["password"], password):
        return row
    return None


# ── Complaints ────────────────────────────────────────────────────────────────

def generate_complaint_id():
    import random
    for _ in range(20):
        cid = f"CMP{random.randint(1000, 9999)}"
        with get_db() as conn:
            exists = _fetchone(conn, f"SELECT 1 FROM complaints WHERE complaint_id = {_ph()}", (cid,))
        if not exists:
            return cid
    return f"CMP{random.randint(100000, 999999)}"


def insert_complaint(student_id, category, description, status="pending",
                     title="", priority="medium", is_anonymous=False, attachment_path=None):
    safe_category = (str(category or "").strip().lower() or "other")[:80]
    safe_description = str(description or "").strip() or "No description provided."
    safe_title = str(title or "").strip()
    if not safe_title:
        first_line = safe_description.splitlines()[0].strip()
        safe_title = (first_line[:180] or "General grievance")
    safe_status = str(status or "").strip().lower() or "pending"
    safe_priority = str(priority or "").strip().lower() or "medium"
    safe_attachment = str(attachment_path or "").strip() or "chatbot"
    complaint_id = generate_complaint_id()
    ph = _placeholders(9)
    with get_db() as conn:
        _execute(conn, f"""
            INSERT INTO complaints (
                complaint_id, student_id, category, description, status,
                title, priority, is_anonymous, attachment_path
            ) VALUES ({ph})
        """, (complaint_id, student_id, safe_category, safe_description,
              safe_status, safe_title, safe_priority, 1 if is_anonymous else 0, safe_attachment))
    return complaint_id


def get_complaint_by_code(complaint_id):
    with get_db() as conn:
        return row_to_dict(_fetchone(conn, f"""
            SELECT c.*, s.name AS student_name, s.email AS student_email
            FROM complaints c
            JOIN students s ON s.id = c.student_id
            WHERE c.complaint_id = {_ph()}
        """, (complaint_id,)))


def list_complaints_for_student(student_id):
    with get_db() as conn:
        rows = _fetchall(conn, f"""
            SELECT * FROM complaints WHERE student_id = {_ph()} ORDER BY id DESC
        """, (student_id,))
        return [row_to_dict(r) for r in rows]


def list_all_complaints(status=None):
    with get_db() as conn:
        if status:
            rows = _fetchall(conn, f"""
                SELECT c.*, s.name AS student_name, s.email AS student_email
                FROM complaints c JOIN students s ON s.id = c.student_id
                WHERE c.status = {_ph()} ORDER BY c.id DESC
            """, (status,))
        else:
            rows = _fetchall(conn, """
                SELECT c.*, s.name AS student_name, s.email AS student_email
                FROM complaints c JOIN students s ON s.id = c.student_id
                ORDER BY c.id DESC
            """)
        return [row_to_dict(r) for r in rows]


def update_complaint_status(complaint_id, status):
    with get_db() as conn:
        _execute(conn, f"UPDATE complaints SET status = {_ph()} WHERE complaint_id = {_ph()}",
                 (status, complaint_id))


def delete_complaint_for_student(complaint_id, student_id):
    with get_db() as conn:
        cur = _execute(conn, f"""
            DELETE FROM complaints WHERE complaint_id = {_ph()} AND student_id = {_ph()}
        """, (complaint_id, int(student_id)))
        return cur.rowcount > 0


# ── Feedback ──────────────────────────────────────────────────────────────────

def add_complaint_feedback(complaint_id, rating, feedback):
    with get_db() as conn:
        ph = _placeholders(3)
        _execute(conn, f"INSERT INTO complaint_feedback (complaint_id, rating, feedback) VALUES ({ph})",
                 (complaint_id, int(rating), feedback or ""))


def add_faculty_feedback(faculty_name, department, rating, comments, student_id=None):
    with get_db() as conn:
        ph = _placeholders(5)
        _execute(conn, f"""
            INSERT INTO faculty_feedback (faculty_name, department, rating, comments, student_id)
            VALUES ({ph})
        """, (faculty_name, department, int(rating), comments or "", student_id or None))


def count_complaint_feedback():
    with get_db() as conn:
        r = _fetchone(conn, "SELECT COUNT(*) AS c FROM complaint_feedback")
        return r["c"]


def list_complaint_feedback():
    with get_db() as conn:
        rows = _fetchall(conn, "SELECT * FROM complaint_feedback ORDER BY id DESC")
        return [row_to_dict(r) for r in rows]


def list_faculty_feedback():
    with get_db() as conn:
        rows = _fetchall(conn, "SELECT * FROM faculty_feedback ORDER BY id DESC")
        return [row_to_dict(r) for r in rows]


# ── FAQs ──────────────────────────────────────────────────────────────────────

def list_faqs():
    with get_db() as conn:
        rows = _fetchall(conn, "SELECT * FROM faqs ORDER BY id ASC")
        return [row_to_dict(r) for r in rows]


def create_faq(question, answer):
    with get_db() as conn:
        ph = _placeholders(2)
        _execute(conn, f"INSERT INTO faqs (question, answer) VALUES ({ph})", (question, answer))
        return _last_insert_id(conn, "faqs")


def update_faq(fid, question, answer):
    with get_db() as conn:
        ph = _ph()
        _execute(conn, f"UPDATE faqs SET question = {ph}, answer = {ph} WHERE id = {ph}",
                 (question, answer, fid))


def delete_faq(fid):
    with get_db() as conn:
        _execute(conn, f"DELETE FROM faqs WHERE id = {_ph()}", (fid,))


# ── Discussions ───────────────────────────────────────────────────────────────

def list_discussions():
    with get_db() as conn:
        rows = _fetchall(conn, """
            SELECT d.*, s.name AS author_name
            FROM discussions d JOIN students s ON s.id = d.student_id
            ORDER BY d.id DESC
        """)
        return [row_to_dict(r) for r in rows]


def get_discussion_comments(discussion_id):
    with get_db() as conn:
        rows = _fetchall(conn, f"""
            SELECT dc.*, s.name AS author_name
            FROM discussion_comments dc
            LEFT JOIN students s ON s.id = dc.student_id
            WHERE dc.discussion_id = {_ph()}
            ORDER BY dc.id ASC
        """, (discussion_id,))
        return [row_to_dict(r) for r in rows]


def create_discussion(student_id, content):
    with get_db() as conn:
        ph = _placeholders(2)
        _execute(conn, f"INSERT INTO discussions (student_id, content) VALUES ({ph})",
                 (student_id, content))
        return _last_insert_id(conn, "discussions")


def add_discussion_comment(discussion_id, student_id, comment):
    with get_db() as conn:
        ph = _placeholders(3)
        _execute(conn, f"""
            INSERT INTO discussion_comments (discussion_id, student_id, comment)
            VALUES ({ph})
        """, (discussion_id, student_id, comment))


# ── Stats ─────────────────────────────────────────────────────────────────────

def complaint_stats():
    with get_db() as conn:
        total = _fetchone(conn, "SELECT COUNT(*) AS c FROM complaints")["c"]
        pending = _fetchone(conn, "SELECT COUNT(*) AS c FROM complaints WHERE status = 'pending'")["c"]
        resolved = _fetchone(conn, "SELECT COUNT(*) AS c FROM complaints WHERE status = 'resolved'")["c"]
        fb = _fetchone(conn, "SELECT COUNT(*) AS c FROM complaint_feedback")["c"]
    return {"total": total, "pending": pending, "resolved": resolved, "feedback": fb}


# ── CSV import ────────────────────────────────────────────────────────────────

def maybe_import_csv(conn=None):
    data_dir = BASE_DIR / "data"
    csm_path = data_dir / "csm.csv"
    if not csm_path.is_file():
        return

    def _do(c):
        row = _fetchone(c, "SELECT COUNT(*) AS c FROM students")
        n = row["c"] if row else 0
        if n > 0:
            return
        ph = _placeholders(6)
        with csm_path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                sid = (row.get("student_id") or "").strip()
                if not sid:
                    continue
                name = (row.get("student_name") or "").strip().strip('"')
                email = f"{sid.lower()}@students.local"
                pw_hash = generate_password_hash(sid)
                _execute(c, f"""
                    INSERT INTO students (name, roll_number, email, department, phone, password)
                    VALUES ({ph})
                """, (name, sid, email, "AIML", "", pw_hash))

    if conn is not None:
        _do(conn)
    else:
        with get_db() as c:
            _do(c)