# ResolveX — Student Grievance Redressal System (AIML)

Full-stack college project: **Flask + SQLite** backend, **Bootstrap** frontend, and **RASA** chatbot with forms and custom actions that read/write the same SQLite database.

## Project layout

```
student-grievance-system/
├── backend/           # Flask app + SQLite (database.db created on first run)
├── rasa_bot/          # NLU, stories, rules, domain, actions
├── frontend/          # Static HTML/CSS/JS
├── requirements.txt
└── README.md
```

## Prerequisites

- **Python 3.8+** for Flask.
- **Python 3.8–3.10 (not 3.11+)** for **RASA Open Source 3.6.x** — if your default Python is 3.11 or newer, create the RASA venv with **3.10** (e.g. `py -3.10 -m venv .venv-rasa` on Windows).
- Windows / macOS / Linux.

## Setup

```bash
cd student-grievance-system
python -m venv .venv

# Windows PowerShell
.\.venv\Scripts\Activate.ps1

# macOS/Linux
source .venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt

# RASA (use a Python 3.10 venv — see Prerequisites)
# pip install -r requirements-rasa.txt
```

### Environment for custom actions

The RASA action server imports `backend.models`. From the **`student-grievance-system`** folder:

**Windows (PowerShell):**

```powershell
$env:PYTHONPATH = "$PWD"
```

**macOS/Linux:**

```bash
export PYTHONPATH="$(pwd)"
```

## Initialize database

The first Flask or action run calls `init_db()` and creates `backend/database.db`.

- **Default admin:** `admin@aiml.edu` / `admin123`
- Register students from the login page (**Create an Account**).

## Run RASA (three terminals)

All commands assume `cd student-grievance-system` and venv activated, with `PYTHONPATH` set to the project root.

1. **Action server** (port **5055**), from **`student-grievance-system`** with `PYTHONPATH` set:

   ```bash
   cd rasa_bot
   rasa run actions
   ```

2. **RASA server + REST + Socket.IO** (port **5005**), from **`rasa_bot`**:

   The bundled `credentials.yml` enables **`socketio`** (required by **rasa-webchat**). Then:

   ```bash
   rasa train
   rasa run --enable-api --cors "*"
   ```

3. **Flask** (port **5000**), from **`student-grievance-system`**:

   ```bash
   cd ..
   python backend/app.py
   ```

Open **http://localhost:5000/login.html**, sign in as a student, then open **Raise Grievances** — the **rasa-webchat** widget talks to `http://localhost:5005`.

> The chatbot submits complaints using `student_id` passed as **Webchat `customData`**, populated after login from `localStorage`.

## API overview (also mirrored without `/api` prefix)

| Method | Path | Notes |
|--------|------|--------|
| POST | `/api/login`, `/api/register`, `/api/logout` | Student auth |
| POST | `/api/admin/login`, `/api/admin/logout` | Admin auth |
| GET | `/api/me`, `/api/profile` | Student profile |
| POST | `/api/profile` | Update profile / optional password |
| GET | `/api/complaints?complaint_id=CMP1234` | Single complaint |
| GET | `/api/complaints` | List (student session) |
| POST | `/api/complaints` | Create complaint (JSON + session or `student_id`) |
| POST | `/api/update_status` | Admin: update status |
| POST | `/api/feedback` | Student: post-resolution feedback |
| POST | `/api/faculty_feedback` | Student: faculty feedback |
| GET/POST | `/api/faqs` | FAQs (admin POST with `op`: create/update/delete) |
| GET/POST | `/api/discussion` | Forum threads + comments |

## Troubleshooting

- **Webchat bubble never appears:** The stock widget defaults to **`withRules: true`**, which hides the UI until a Botfront “session_confirm” event. This project sets **`withRules: false`** in `raise_grievance.html` so the launcher shows with normal Rasa OSS.
- **Webchat cannot connect:** Ensure `rasa_bot/credentials.yml` includes the **`socketio`** block, RASA is on **5005**, action server on **5055**, and CORS is enabled. Check the browser console for Socket.IO errors.
- **`ModuleNotFoundError: backend` on actions:** Set `PYTHONPATH` to the **`student-grievance-system`** directory (parent of `backend`).
- **`rasa train` errors on RegexEntityExtractor:** Upgrade/downgrade RASA to match `requirements.txt` or adjust `rasa_bot/config.yml` per your installed RASA version docs.

## License / use

Educational prototype for an AIML department demo — harden auth, validation, and deployment before any production use.
