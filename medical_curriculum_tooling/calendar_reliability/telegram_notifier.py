#!/usr/bin/env python3
"""Reliable, secret-safe Telegram Bot API delivery for calendar alerts.

The notifier is deliberately independent from calendar publication. A delivery
failure is recorded and returned to the caller, but it never invalidates a
good calendar publication. The receipt stores only fingerprints, timestamps,
message IDs, and sanitized error classes; it never stores credentials.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo


SCHEMA_VERSION = 1
MAX_SENT_RECEIPTS = 500
DEFAULT_RETRIES = 3
DEFAULT_RECEIPT = Path("alerts/telegram-delivery.json")
DEFAULT_API_BASE = "https://api.telegram.org"
PROVISIONAL_ALERT_TYPES = {"possible_cancellation"}


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def alert_fingerprint(event: dict[str, Any]) -> str:
    """Return a stable identity that excludes observation time and credentials."""
    explicit = event.get("fingerprint")
    if explicit:
        return str(explicit)
    occurrences = event.get("events")
    if not isinstance(occurrences, list):
        occurrence = event.get("event")
        occurrences = [occurrence] if isinstance(occurrence, dict) else []
    occurrence_identity = []
    for occurrence in occurrences:
        if isinstance(occurrence, dict):
            occurrence_identity.append(
                {
                    "uid": occurrence.get("uid"),
                    "summary": occurrence.get("summary"),
                    "start": occurrence.get("start"),
                    "end": occurrence.get("end"),
                    "location": occurrence.get("location"),
                    "status": occurrence.get("status"),
                }
            )
    identity = {
        "feed_id": event.get("feed_id", "all"),
        "type": event.get("type", "calendar_alert"),
        "uid": event.get("uid"),
        "summary": event.get("summary"),
        "message": event.get("message"),
        "occurrences": occurrence_identity,
    }
    digest = hashlib.sha256(canonical_json(identity).encode("utf-8")).hexdigest()[:24]
    return f"calendar-{digest}"


def _safe_text(value: Any, limit: int = 1200) -> str:
    text = "" if value is None else str(value).strip()
    return text[:limit]


def _human_text(value: Any, limit: int = 1200) -> str:
    text = _safe_text(value, limit)
    if text and text == text.upper() and any(character.isalpha() for character in text):
        text = text.title()
    return text


def _feed_label(value: Any) -> str:
    text = _safe_text(value, 200).replace("-", " ")
    return text.title() if text else "All feeds"


def _event_occurrences(event: dict[str, Any]) -> list[dict[str, Any]]:
    occurrences = event.get("events")
    if isinstance(occurrences, list):
        return [item for item in occurrences if isinstance(item, dict)]
    occurrence = event.get("event")
    if isinstance(occurrence, dict):
        return [occurrence]
    if event.get("start") or event.get("end") or event.get("location"):
        return [event]
    return []


def _description_fields(description: Any) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in _safe_text(description, 4000).splitlines():
        if ":" not in line:
            continue
        label, value = line.split(":", 1)
        label = label.strip().lower()
        value = value.strip()
        if label and value and label not in fields:
            fields[label] = value
    return fields


def _parse_datetime(value: Any, timezone_id: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(timezone_id))
    try:
        return parsed.astimezone(ZoneInfo(timezone_id))
    except Exception:  # noqa: BLE001 - formatting must remain nonfatal.
        return parsed


def _format_occurrence(occurrence: dict[str, Any], timezone_id: str) -> list[str]:
    start = _parse_datetime(occurrence.get("start"), timezone_id)
    end = _parse_datetime(occurrence.get("end"), timezone_id)
    metadata = occurrence.get("metadata") if isinstance(occurrence.get("metadata"), dict) else {}
    all_day = bool(metadata.get("all_day"))
    if start and end and not all_day:
        when = f"{start.strftime('%A %d %B %Y')}, {start.strftime('%H:%M')}-{end.strftime('%H:%M')} ({timezone_id})"
    elif start:
        when = f"{start.strftime('%A %d %B %Y')} (all day, {timezone_id})"
    else:
        when = "Date/time not provided by the source"
    location = _human_text(occurrence.get("location")) or "Location not provided by the source"
    fields = _description_fields(occurrence.get("description"))
    lines = [f"When: {when}", f"Where: {location}"]
    if fields.get("teacher"):
        lines.append(f"Teacher: {_human_text(fields['teacher'])}")
    return lines


def _event_title(summary: Any) -> tuple[str, str | None]:
    text = _human_text(summary, 1200) or "UCSC calendar event"
    match = re.match(r"^(.*?)\s*\(([^()]*)\)\s*$", text)
    if match:
        return match.group(1).strip(), match.group(2).strip()
    return text, None


def render_message(event: dict[str, Any], fingerprint: str) -> str:
    alert_type = str(event.get("type", "calendar_alert"))
    timezone_id = _safe_text(event.get("timezone_id") or "Europe/Rome", 80)
    occurrences = _event_occurrences(event)
    summaries = [item.get("summary") for item in occurrences if item.get("summary")]
    summary = event.get("summary") or (summaries[0] if summaries else None)
    title, course = _event_title(summary)
    labels = {
        "explicit_cancellation": "📅 UCSC calendar update — cancellation confirmed",
        "confirmed_cancellation": "📅 UCSC calendar update — cancellation confirmed",
        "cancellation_reversed": "📅 UCSC calendar update — event restored",
        "event_updated": "📅 UCSC calendar update — details changed",
        "pipeline_failure": "⚠️ UCSC calendar problem — publication failed",
        "watchdog_failure": "⚠️ UCSC calendar problem — integrity check failed",
        "publication_stale": "⚠️ UCSC calendar problem — publication is stale",
        "publication_recovered": "✅ UCSC calendar recovered",
        "integrity_mismatch": "⚠️ UCSC calendar problem — integrity mismatch",
        "native_mismatch": "⚠️ UCSC calendar problem — Mac calendar mismatch",
        "heartbeat": "✅ UCSC calendar monitor check",
    }
    lines = [labels.get(alert_type, f"UCSC calendar update — {_human_text(alert_type).lower()}")]
    if title:
        lines.append(f"Event: {title}")
    if course:
        lines.append(f"Course: {course}")
    lines.append(f"Feed: {_feed_label(event.get('feed_id', 'all'))}")
    if len(occurrences) > 1:
        lines.append(f"Occurrences: {len(occurrences)}")
    for index, occurrence in enumerate(occurrences):
        if len(occurrences) > 1:
            lines.append(f"Occurrence {index + 1}:")
        lines.extend(_format_occurrence(occurrence, timezone_id))
    if alert_type == "explicit_cancellation":
        lines.append("Reason: The official source explicitly marked this event suspended.")
        lines.append("Action: Treat this event as cancelled and do not travel for it.")
    elif alert_type == "confirmed_cancellation":
        lines.append("Reason: The event was absent from two consecutive valid official snapshots.")
        lines.append("Action: Treat this event as cancelled and check the live timetable if needed.")
    elif alert_type == "cancellation_reversed":
        lines.append("Reason: The event reappeared in the official source.")
        lines.append("Action: Use the restored calendar details.")
    elif alert_type == "event_updated":
        lines.append("Reason: The source identifier changed, but the event matched the previous schedule.")
        lines.append("Action: Use the details shown in the current calendar entry.")
    elif alert_type == "heartbeat":
        lines.append("Telegram delivery is working; this is a monitor check, not a class change.")
    elif event.get("message"):
        lines.append(f"Details: {_human_text(event['message'])}")
    return "\n".join(lines)[:4000]


def _empty_ledger() -> dict[str, Any]:
    return {"version": SCHEMA_VERSION, "updated_at_utc": None, "sent": {}, "failures": {}}


def load_ledger(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict) or not isinstance(payload.get("sent"), dict):
            raise ValueError("invalid Telegram delivery ledger")
        payload.setdefault("version", SCHEMA_VERSION)
        payload.setdefault("failures", {})
        return payload
    except (FileNotFoundError, json.JSONDecodeError, OSError, ValueError):
        return _empty_ledger()


def save_ledger(path: Path, ledger: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ledger["version"] = SCHEMA_VERSION
    ledger["updated_at_utc"] = iso(utc_now())
    sent = ledger.get("sent", {})
    if isinstance(sent, dict) and len(sent) > MAX_SENT_RECEIPTS:
        newest = sorted(sent.items(), key=lambda item: str(item[1].get("sent_at_utc", "")))[-MAX_SENT_RECEIPTS:]
        ledger["sent"] = dict(newest)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(ledger, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _post_message(
    token: str,
    chat_id: str,
    message: str,
    *,
    opener: Callable[..., Any] = urlopen,
    api_base: str = DEFAULT_API_BASE,
) -> int:
    request = Request(
        f"{api_base.rstrip('/')}/bot{token}/sendMessage",
        data=urlencode(
            {
                "chat_id": chat_id,
                "text": message,
                "disable_web_page_preview": "true",
            }
        ).encode("utf-8"),
        headers={"Content-Type": "application/x-www-form-urlencoded", "User-Agent": "ucsc-calendar-telegram-notifier/1.0"},
        method="POST",
    )
    try:
        with opener(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        exc.close()
        raise RuntimeError(f"telegram_http_{exc.code}") from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise RuntimeError(f"telegram_transport_{type(exc).__name__}") from exc
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise RuntimeError("telegram_invalid_response") from exc
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        error_code = payload.get("error_code") if isinstance(payload, dict) else None
        raise RuntimeError(f"telegram_api_rejected_{error_code or 'unknown'}")
    result = payload.get("result")
    if not isinstance(result, dict) or not result.get("message_id"):
        raise RuntimeError("telegram_missing_message_id")
    return int(result["message_id"])


def notify_alert(
    event: dict[str, Any],
    receipt_path: Path,
    *,
    token: str | None = None,
    chat_id: str | None = None,
    opener: Callable[..., Any] = urlopen,
    sleep: Callable[[float], None] = time.sleep,
    retries: int = DEFAULT_RETRIES,
    api_base: str = DEFAULT_API_BASE,
) -> dict[str, Any]:
    """Deliver one event, deduplicating successful fingerprints."""
    event = dict(event)
    observed = event.setdefault("observed_at_utc", iso(utc_now()))
    fingerprint = alert_fingerprint(event)
    if event.get("type") in PROVISIONAL_ALERT_TYPES:
        return {"status": "suppressed_provisional", "fingerprint": fingerprint}
    ledger = load_ledger(receipt_path)
    if fingerprint in ledger.get("sent", {}):
        return {"status": "duplicate", "fingerprint": fingerprint}

    token = token if token is not None else os.environ.get("CALENDAR_ALERT_TELEGRAM_BOT_TOKEN")
    chat_id = chat_id if chat_id is not None else os.environ.get("CALENDAR_ALERT_TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        ledger.setdefault("failures", {})[fingerprint] = {
            "status": "not_configured",
            "observed_at_utc": observed,
            "type": event.get("type"),
            "feed_id": event.get("feed_id", "all"),
        }
        save_ledger(receipt_path, ledger)
        return {"status": "not_configured", "fingerprint": fingerprint}

    message = render_message(event, fingerprint)
    attempts = max(1, int(retries))
    last_error = "telegram_delivery_failed"
    for attempt in range(1, attempts + 1):
        try:
            message_id = _post_message(token, chat_id, message, opener=opener, api_base=api_base)
            ledger.setdefault("sent", {})[fingerprint] = {
                "sent_at_utc": iso(utc_now()),
                "message_id": message_id,
                "type": event.get("type"),
                "feed_id": event.get("feed_id", "all"),
            }
            ledger.setdefault("failures", {}).pop(fingerprint, None)
            save_ledger(receipt_path, ledger)
            return {"status": "sent", "fingerprint": fingerprint, "message_id": message_id, "attempts": attempt}
        except Exception as exc:  # noqa: BLE001 - delivery must never break publication.
            last_error = str(exc)
            if attempt < attempts:
                sleep(min(2 ** (attempt - 1), 8))

    ledger.setdefault("failures", {})[fingerprint] = {
        "status": "failed",
        "observed_at_utc": observed,
        "attempts": attempts,
        "error": last_error,
        "type": event.get("type"),
        "feed_id": event.get("feed_id", "all"),
    }
    save_ledger(receipt_path, ledger)
    return {"status": "failed", "fingerprint": fingerprint, "attempts": attempts, "error": last_error}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-file", type=Path)
    parser.add_argument("--event-type", default="calendar_alert")
    parser.add_argument("--feed-id", default="all")
    parser.add_argument("--summary")
    parser.add_argument("--message")
    parser.add_argument("--fingerprint")
    parser.add_argument("--receipt", type=Path, default=DEFAULT_RECEIPT)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--require-config", action="store_true")
    args = parser.parse_args()

    if args.event_file:
        event = json.loads(args.event_file.read_text(encoding="utf-8"))
        if not isinstance(event, dict):
            raise SystemExit("event JSON must be an object")
    else:
        event = {
            "type": args.event_type,
            "feed_id": args.feed_id,
            "summary": args.summary,
            "message": args.message,
        }
        if args.fingerprint:
            event["fingerprint"] = args.fingerprint
    result = notify_alert(event, args.receipt, retries=args.retries)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    if args.require_config and result["status"] == "not_configured":
        return 2
    return 0 if result["status"] in {"sent", "duplicate", "not_configured"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
