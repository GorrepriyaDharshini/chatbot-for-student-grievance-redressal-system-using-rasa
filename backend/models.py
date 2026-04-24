"""
SQLite data access layer for ResolveX.
Keeps schema aligned with project requirements.
"""
import csv
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from werkzeug.security import generate_password_hash, check_password_hash # pyright: ignore[reportMissingImports]

from backend.config import BASE_DIR, DATABASE_PATH

# AIML staff portal (documented for operators; change via DB if needed)
AIML_ADMIN_USERNAME = "aiml_admin"
AIML_ADMIN_PASSWORD = "ResolveX_AIML_2026"


def _connect(db_path: Path | None = None):
    path = db_path or DATABASE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def get_db():
    conn = _connect()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    """Create tables and seed a default admin if none exists."""
    with get_db() as conn:
        conn.executescript(
            """
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
                password TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS complaints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                complaint_id TEXT NOT NULL UNIQUE,
                student_id INTEGER NOT NULL,
                category TEXT NOT NULL,
                description TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
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
                -- Optional student identifier (roll number or ID string)
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
            """
        )
        _migrate_complaints_schema(conn)
        _migrate_admins_username(conn)
        _migrate_faculty_feedback_student_id(conn)

        row = conn.execute("SELECT COUNT(*) AS c FROM admins").fetchone()
        if row and row["c"] == 0:
            conn.execute(
                "INSERT INTO admins (email, username, password) VALUES (?, ?, ?)",
                (
                    "aiml-admin@resolvex.local",
                    AIML_ADMIN_USERNAME,
                    generate_password_hash(AIML_ADMIN_PASSWORD),
                ),
            )

        row = conn.execute("SELECT COUNT(*) AS c FROM faqs").fetchone()
        if row and row["c"] == 0:
            conn.executemany(
                "INSERT INTO faqs (question, answer) VALUES (?, ?)",
                [
                    (
                        "How do I track my complaint?",
                        "Use your Complaint ID (e.g. CMP1234) on the My Complaints page or ask the chatbot.",
                    ),
                    (
                        "Can I submit anonymously?",
                        "Yes. The Raise Grievance flow allows anonymous submission where policy permits.",
                    ),
                    (
                        "Who handles AIML department grievances?",
                        "Department coordinators review complaints and update status in the admin panel.",
                    ),
                ],
            )

        maybe_import_csv(conn)

        if os.environ.get("RESOLVEX_SYNC_ROSTER", "").lower() in ("1", "true", "yes"):
            sync_students_from_csm_csv(conn)


def _migrate_admins_username(conn):
    """Add admins.username; move AIML login to username + password."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(admins)").fetchall()}
    if "username" not in cols:
        conn.execute("ALTER TABLE admins ADD COLUMN username TEXT")
        pw = generate_password_hash(AIML_ADMIN_PASSWORD)
        conn.execute(
            """
            UPDATE admins SET
                username = ?,
                password = ?,
                email = 'aiml-admin@resolvex.local'
            WHERE id = (SELECT MIN(id) FROM admins)
            """,
            (AIML_ADMIN_USERNAME, pw),
        )


def _migrate_complaints_schema(conn):
    """Add title, priority, anonymous flag, attachment path if missing (SQLite)."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(complaints)").fetchall()}
    if "title" not in cols:
        conn.execute("ALTER TABLE complaints ADD COLUMN title TEXT DEFAULT ''")
    if "priority" not in cols:
        conn.execute("ALTER TABLE complaints ADD COLUMN priority TEXT DEFAULT 'medium'")
    if "is_anonymous" not in cols:
        conn.execute("ALTER TABLE complaints ADD COLUMN is_anonymous INTEGER DEFAULT 0")
    if "attachment_path" not in cols:
        conn.execute("ALTER TABLE complaints ADD COLUMN attachment_path TEXT")


def _normalize_complaint_status(raw: str) -> str:
    sl = (raw or "").strip().lower()
    if "progress" in sl:
        return "in progress"
    if "resolved" in sl or "closed" in sl:
        return "resolved"
    return "pending"


def _clear_student_related_data(conn):
    conn.executescript(
        """
        DELETE FROM complaint_feedback;
        DELETE FROM discussion_comments;
        DELETE FROM discussions;
        DELETE FROM complaints;
        DELETE FROM students;
        """
    )


def _import_complaints_from_csv(conn, path: Path):
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            roll = (row.get("student_id") or "").strip()
            if not roll:
                continue
            got = conn.execute(
                "SELECT id FROM students WHERE roll_number = ?", (roll,)
            ).fetchone()
            if not got:
                continue
            internal_id = got["id"]
            raw_cid = (row.get("complaint_id") or "").strip()
            cid = f"CMP{raw_cid}" if raw_cid else "CMP0"
            title = (row.get("complaint_name") or "").strip().strip('"')
            desc = (row.get("complaint_details") or "").strip().strip('"')
            dept = (row.get("department_assigned") or "").strip().strip('"')
            status = _normalize_complaint_status(row.get("status") or "")
            category = (title.split("-")[0].strip().lower().replace(" ", "_") or "general")
            full_desc = f"[{dept}] {desc}" if dept else desc
            conn.execute(
                """
                INSERT INTO complaints (
                    complaint_id, student_id, category, description, status,
                    title, priority, is_anonymous, attachment_path
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, 0, '')
                """,
                (
                    cid,
                    internal_id,
                    category,
                    full_desc,
                    status,
                    title,
                    "medium",
                ),
            )


def sync_students_from_csm_csv(conn):
    """Insert or update every row in data/csm.csv (password = student_id). Keeps complaints."""
    path = BASE_DIR / "data" / "csm.csv"
    if not path.is_file():
        return
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        dept = "AIML"
        for row in reader:
            sid = (row.get("student_id") or "").strip()
            if not sid:
                continue
            name = (row.get("student_name") or "").strip().strip('"')
            email = f"{sid.lower()}@students.local"
            pw_hash = generate_password_hash(sid)
            conn.execute(
                """
                INSERT INTO students (name, roll_number, email, department, phone, password)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(roll_number) DO UPDATE SET
                    name = excluded.name,
                    email = excluded.email,
                    department = excluded.department,
                    password = excluded.password
                """,
                (name, sid, email, dept, "", pw_hash),
            )


def maybe_import_csv(conn=None):
    """
    Load data/data/csm.csv and data/complaints.csv when students table is empty,
    or when RESOLVEX_REPLACE_CSV=1 (clears students, complaints, discussions).
    """
    data_dir = BASE_DIR / "data"
    csm_path = data_dir / "csm.csv"
    if not csm_path.is_file():
        return

    def _do(c):
        force = os.environ.get("RESOLVEX_REPLACE_CSV", "").lower() in (
            "1",
            "true",
            "yes",
        )
        n = c.execute("SELECT COUNT(*) AS c FROM students").fetchone()["c"]
        if n > 0 and not force:
            return
        _clear_student_related_data(c)
        with csm_path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            dept = "AIML"
            for row in reader:
                sid = (row.get("student_id") or "").strip()
                if not sid:
                    continue
                name = (row.get("student_name") or "").strip().strip('"')
                email = f"{sid.lower()}@students.local"
                pw_hash = generate_password_hash(sid)
                c.execute(
                    """
                    INSERT INTO students (name, roll_number, email, department, phone, password)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (name, sid, email, dept, "", pw_hash),
                )
        comp_path = data_dir / "complaints.csv"
        if comp_path.is_file():
            _import_complaints_from_csv(c, comp_path)

    if conn is not None:
        _do(conn)
        return
    with get_db() as c:
        _do(c)


# --- Student helpers ---

def create_student(name, roll_number, email, department, phone, password):
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO students (name, roll_number, email, department, phone, password)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                name,
                roll_number,
                email,
                department,
                phone or "",
                generate_password_hash(password),
            ),
        )
        return conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]


def get_student_by_email(email):
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM students WHERE email = ?", (email,)
        ).fetchone()


def get_student_by_roll_number(roll_number: str):
    roll = (roll_number or "").strip()
    if not roll:
        return None
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM students WHERE roll_number = ?", (roll,)
        ).fetchone()


def get_student_by_id(sid):
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM students WHERE id = ?", (sid,)
        ).fetchone()


def verify_student(username, password):
    """Login with student ID (roll_number) as username; password is the same student ID string."""
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
    fields = []
    values = []
    if name is not None:
        fields.append("name = ?")
        values.append(name)
    if phone is not None:
        fields.append("phone = ?")
        values.append(phone)
    if department is not None:
        fields.append("department = ?")
        values.append(department)
    if not fields:
        return
    values.append(sid)
    with get_db() as conn:
        conn.execute(
            f"UPDATE students SET {', '.join(fields)} WHERE id = ?",
            values,
        )


# --- Admin ---

def verify_admin(username, password):
    u = (username or "").strip()
    if not u:
        return None
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM admins WHERE username = ?", (u,)
        ).fetchone()
    if not row:
        return None
    if check_password_hash(row["password"], password):
        return row
    return None


# --- Complaints ---

def generate_complaint_id():
    import random

    for _ in range(20):
        cid = f"CMP{random.randint(1000, 9999)}"
        with get_db() as conn:
            exists = conn.execute(
                "SELECT 1 FROM complaints WHERE complaint_id = ?", (cid,)
            ).fetchone()
        if not exists:
            return cid
    return f"CMP{random.randint(100000, 999999)}"


def insert_complaint(
    student_id,
    category,
    description,
    status="pending",
    title="",
    priority="medium",
    is_anonymous=False,
    attachment_path=None,
):
    safe_category = (str(category or "").strip().lower() or "other")[:80]
    safe_description = str(description or "").strip() or "No description provided."
    safe_title = str(title or "").strip()
    if not safe_title:
        # Title fallback: first non-empty sentence/line from complaint body
        first_line = safe_description.splitlines()[0].strip()
        safe_title = (first_line[:180] or "General grievance")
    safe_status = str(status or "").strip().lower() or "pending"
    safe_priority = str(priority or "").strip().lower() or "medium"
    safe_attachment = str(attachment_path or "").strip() or "chatbot"
    complaint_id = generate_complaint_id()
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO complaints (
                complaint_id, student_id, category, description, status,
                title, priority, is_anonymous, attachment_path
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                complaint_id,
                student_id,
                safe_category,
                safe_description,
                safe_status,
                safe_title,
                safe_priority,
                1 if is_anonymous else 0,
                safe_attachment,
            ),
        )
    return complaint_id


def get_complaint_by_code(complaint_id):
    with get_db() as conn:
        return conn.execute(
            """
            SELECT c.*, s.name AS student_name, s.email AS student_email
            FROM complaints c
            JOIN students s ON s.id = c.student_id
            WHERE c.complaint_id = ?
            """,
            (complaint_id,),
        ).fetchone()


def list_complaints_for_student(student_id):
    with get_db() as conn:
        return conn.execute(
            """
            SELECT * FROM complaints WHERE student_id = ?
            ORDER BY id DESC
            """,
            (student_id,),
        ).fetchall()


def list_all_complaints(status=None):
    with get_db() as conn:
        if status:
            return conn.execute(
                """
                SELECT c.*, s.name AS student_name, s.email AS student_email
                FROM complaints c
                JOIN students s ON s.id = c.student_id
                WHERE c.status = ?
                ORDER BY c.id DESC
                """,
                (status,),
            ).fetchall()
        return conn.execute(
            """
            SELECT c.*, s.name AS student_name, s.email AS student_email
            FROM complaints c
            JOIN students s ON s.id = c.student_id
            ORDER BY c.id DESC
            """
        ).fetchall()


def update_complaint_status(complaint_id, status):
    with get_db() as conn:
        conn.execute(
            "UPDATE complaints SET status = ? WHERE complaint_id = ?",
            (status, complaint_id),
        )


def delete_complaint_for_student(complaint_id, student_id):
    """Delete a complaint only if it belongs to the logged-in student."""
    with get_db() as conn:
        cur = conn.execute(
            "DELETE FROM complaints WHERE complaint_id = ? AND student_id = ?",
            (complaint_id, int(student_id)),
        )
        return cur.rowcount > 0


def row_to_dict(row):
    if row is None:
        return None
    return {k: row[k] for k in row.keys()}


def mask_anonymous_complaint(d):
    """Hide submitter identity for anonymous complaints (admin/public views)."""
    if not d:
        return d
    if d.get("is_anonymous"):
        d = dict(d)
        d["student_name"] = "Anonymous"
        d["student_email"] = ""
    return d


def _migrate_faculty_feedback_student_id(conn):
    """Ensure faculty_feedback has a student_id column (for per-student ratings)."""
    cols = {
        row[1] for row in conn.execute("PRAGMA table_info(faculty_feedback)").fetchall()
    }
    if "student_id" not in cols:
        conn.execute("ALTER TABLE faculty_feedback ADD COLUMN student_id TEXT")


# --- Feedback ---

def add_complaint_feedback(complaint_id, rating, feedback):
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO complaint_feedback (complaint_id, rating, feedback)
            VALUES (?, ?, ?)
            """,
            (complaint_id, int(rating), feedback or ""),
        )


def add_faculty_feedback(
    faculty_name,
    department,
    rating,
    comments,
    student_id: str | None = None,
):
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO faculty_feedback (
                faculty_name,
                department,
                rating,
                comments,
                student_id
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (faculty_name, department, int(rating), comments or "", student_id or None),
        )


def count_complaint_feedback():
    with get_db() as conn:
        r = conn.execute("SELECT COUNT(*) AS c FROM complaint_feedback").fetchone()
        return r["c"]


def list_complaint_feedback():
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM complaint_feedback ORDER BY id DESC"
        ).fetchall()


def list_faculty_feedback():
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM faculty_feedback ORDER BY id DESC"
        ).fetchall()


# --- FAQs ---

def list_faqs():
    with get_db() as conn:
        return conn.execute("SELECT * FROM faqs ORDER BY id ASC").fetchall()


def create_faq(question, answer):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO faqs (question, answer) VALUES (?, ?)", (question, answer)
        )
        return conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]


def update_faq(fid, question, answer):
    with get_db() as conn:
        conn.execute(
            "UPDATE faqs SET question = ?, answer = ? WHERE id = ?",
            (question, answer, fid),
        )


def delete_faq(fid):
    with get_db() as conn:
        conn.execute("DELETE FROM faqs WHERE id = ?", (fid,))


# --- Discussion ---

def list_discussions():
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT d.*, s.name AS author_name
            FROM discussions d
            JOIN students s ON s.id = d.student_id
            ORDER BY d.id DESC
            """
        ).fetchall()
        return [row_to_dict(r) for r in rows]


def get_discussion_comments(discussion_id):
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT dc.*, s.name AS author_name
            FROM discussion_comments dc
            LEFT JOIN students s ON s.id = dc.student_id
            WHERE dc.discussion_id = ?
            ORDER BY dc.id ASC
            """,
            (discussion_id,),
        ).fetchall()
        return [row_to_dict(r) for r in rows]


def create_discussion(student_id, content):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO discussions (student_id, content) VALUES (?, ?)",
            (student_id, content),
        )
        return conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]


def add_discussion_comment(discussion_id, student_id, comment):
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO discussion_comments (discussion_id, student_id, comment)
            VALUES (?, ?, ?)
            """,
            (discussion_id, student_id, comment),
        )


# --- Stats (admin dashboard) ---

def complaint_stats():
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) AS c FROM complaints").fetchone()["c"]
        pending = conn.execute(
            "SELECT COUNT(*) AS c FROM complaints WHERE status = 'pending'"
        ).fetchone()["c"]
        resolved = conn.execute(
            "SELECT COUNT(*) AS c FROM complaints WHERE status = 'resolved'"
        ).fetchone()["c"]
        fb = conn.execute("SELECT COUNT(*) AS c FROM complaint_feedback").fetchone()[
            "c"
        ]
    return {"total": total, "pending": pending, "resolved": resolved, "feedback": fb}