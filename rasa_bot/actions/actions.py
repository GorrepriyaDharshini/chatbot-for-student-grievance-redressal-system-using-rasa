"""
Custom RASA actions: SQLite-backed complaint submit and status check.

Run action server from project root with PYTHONPATH including the repo root
so `backend.models` resolves (see README).
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Text

from rasa_sdk import Action, Tracker
from rasa_sdk.executor import CollectingDispatcher
from rasa_sdk.events import AllSlotsReset, SlotSet, UserUtteranceReverted

# Project root: student-grievance-system/
ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend import models  # noqa: E402

logger = logging.getLogger(__name__)

# Ensure DB exists (Flask also calls init_db; safe to repeat)
models.init_db()


def _metadata(tracker: Tracker) -> Dict[str, Any]:
    # Latest user message may carry customData from rasa-webchat
    last = tracker.latest_message or {}
    meta = last.get("metadata") or {}
    if not meta:
        # Some channels put metadata on the parse_data root
        meta = last.get("message_metadata") or {}
    return meta


def _resolve_student_id(tracker: Tracker, email_slot: Text | None) -> int | None:
    meta = _metadata(tracker)
    sid = meta.get("student_id")
    if sid is not None:
        try:
            return int(sid)
        except (TypeError, ValueError):
            pass
    # Flask REST proxy uses sender "student_<id>" (see backend app.py)
    sender = (getattr(tracker, "sender_id", None) or "").strip()
    if sender.startswith("student_"):
        tail = sender[8:].strip()
        if tail.isdigit():
            try:
                return int(tail)
            except ValueError:
                pass
    if email_slot:
        row = models.get_student_by_email(email_slot.strip().lower())
        if row:
            return int(row["id"])
    return None


def _format_complaint_description(
    base: str, name: str, roll: str, email: str, dept: str
) -> str:
    header = (
        f"Submitted via chatbot.\n"
        f"Name: {name}\nRoll: {roll}\nEmail: {email}\nDepartment: {dept}\n\n"
    )
    return header + (base or "").strip()


class ActionSubmitComplaint(Action):
    def name(self) -> Text:
        return "action_submit_complaint"

    def run(
        self,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any],
    ) -> List[Dict[Text, Any]]:
        name = tracker.get_slot("name")
        roll = tracker.get_slot("roll_number")
        email = tracker.get_slot("email")
        department = tracker.get_slot("department")
        category = tracker.get_slot("category") or "other"
        brief_explanation = tracker.get_slot("brief_explanation") or ""
        description = tracker.get_slot("description") or ""

        student_id = _resolve_student_id(tracker, email)
        if not student_id:
            dispatcher.utter_message(
                text=(
                    "I could not link this chat to your student account. "
                    "Please log in on the website first, open Raise Grievances, "
                    "and try again — or register with the same email you use on ResolveX."
                )
            )
            return [AllSlotsReset()]

        # Prefer authoritative profile fields from DB when available.
        student_row = models.get_student_by_id(student_id)
        if student_row:
            name = name or student_row["name"]
            roll = roll or student_row["roll_number"]
            email = email or student_row["email"]
            department = department or student_row["department"]

        norm_category = str(category).strip().lower()
        if "hostel" in norm_category or "mess" in norm_category or "food" in norm_category:
            norm_category = "hostel"
        elif norm_category not in {"academic", "infrastructure", "harassment", "facilities", "other", "hostel"}:
            norm_category = "other"

        brief_text = str(brief_explanation).strip() or "General grievance"
        detail_text = str(description).strip() or brief_text
        full_description = _format_complaint_description(
            detail_text, str(name), str(roll), str(email), str(department)
        )
        try:
            cid = models.insert_complaint(
                student_id=student_id,
                category=norm_category[:80],
                description=full_description,
                status="pending",
                title=brief_text[:180],
                priority="medium",
                is_anonymous=False,
                attachment_path="chatbot",
            )
        except Exception as e:  # pragma: no cover - defensive
            logger.exception("insert_complaint failed")
            dispatcher.utter_message(
                text=f"Sorry, storing the complaint failed: {e}. Please try again later."
            )
            return [AllSlotsReset()]

        dispatcher.utter_message(
            text=(
                f"Complaint registered successfully! Your Complaint ID is **{cid}**. "
                f"Use it under My Complaints or ask me “status of {cid}”."
            )
        )
        return [AllSlotsReset()]


class ActionCheckStatus(Action):
    def name(self) -> Text:
        return "action_check_status"

    def run(
        self,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any],
    ) -> List[Dict[Text, Any]]:
        entities = tracker.latest_message.get("entities", []) or []
        cids = [
            e.get("value")
            for e in entities
            if e.get("entity") == "complaint_id" and e.get("value")
        ]
        complaint_id = cids[0] if cids else None
        if not complaint_id:
            text = tracker.latest_message.get("text", "") or ""
            upper = text.upper().replace(" ", "")
            if "CMP" in upper:
                start = upper.index("CMP")
                tail = upper[start:]
                # grab CMP + digits
                buf = []
                for ch in tail:
                    if ch.isalnum():
                        buf.append(ch)
                    if len(buf) >= 7:  # CMP + 4 digits minimum
                        break
                guess = "".join(buf)
                if guess.startswith("CMP") and len(guess) >= 6:
                    complaint_id = guess

        if not complaint_id:
            dispatcher.utter_message(
                text="Please share your Complaint ID (for example **CMP4821**)."
            )
            return []

        row = models.get_complaint_by_code(str(complaint_id).upper())
        if not row:
            dispatcher.utter_message(
                text=f"I could not find a complaint with ID **{complaint_id.upper()}**."
            )
            return []

        status = row["status"]
        dispatcher.utter_message(
            text=(
                f"Complaint **{row['complaint_id']}** is currently marked as **{status}**."
            )
        )
        return []


class ActionDefaultFallback(Action):
    def name(self) -> Text:
        return "action_default_fallback"

    def run(
        self,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any],
    ) -> List[Dict[Text, Any]]:
        requested = tracker.get_slot("requested_slot")
        active_loop = tracker.active_loop.get("name") if tracker.active_loop else None

        # If user is in complaint form, keep the form going instead of hard failing.
        if active_loop == "complaint_form" and requested:
            dispatcher.utter_message(
                text=(
                    "I didn't fully get that, but let's continue your complaint. "
                    f"Please share {requested.replace('_', ' ')}."
                )
            )
            return [UserUtteranceReverted(), SlotSet("requested_slot", requested)]

        dispatcher.utter_message(
            text=(
                "I’m not sure I understood. You can say 'register complaint', "
                "'check status CMP1234', 'faq', or 'discussion'."
            )
        )
        return [UserUtteranceReverted()]
