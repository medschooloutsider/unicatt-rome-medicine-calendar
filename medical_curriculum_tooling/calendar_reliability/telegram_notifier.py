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
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


SCHEMA_VERSION = 1
MAX_SENT_RECEIPTS = 500
DEFAULT_RETRIES = 3
DEFAULT_RECEIPT = Path("alerts/telegram-delivery.json")
DEFAULT_API_BASE = "https://api.telegram.org"


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
    identity = {
        "feed_id": event.get("feed_id", "all"),
        "type": event.get("type", "calendar_alert"),
        "uid": event.get("uid"),
        "summary": event.get("summary"),
        "message": event.get("message"),
    }
    digest = hashlib.sha256(canonical_json(identity).encode("utf-8")).hexdigest()[:24]
    return f"calendar-{digest}"


def _safe_text(value: Any, limit: int = 1200) -> str:
    text = "" if value is None else str(value).strip()
    return text[:limit]


def render_message(event: dict[str, Any], fingerprint: str) -> str:
    type_labels = {
        "explicit_cancellation": "CANCELLATION CONFIRMED",
        "confirmed_cancellation": "CANCELLATION CONFIRMED",
        "possible_cancellation": "POSSIBLE CHANGE",
        "cancellation_reversed": "CANCELLATION REVERSED",
        "pipeline_failure": "PUBLISHER FAILURE",
        "watchdog_failure": "CALENDAR WATCHDOG FAILURE",
        "publication_stale": "PUBLICATION STALE",
        "publication_recovered": "PUBLICATION RECOVERED",
        "integrity_mismatch": "INTEGRITY MISMATCH",
        "native_mismatch": "NATIVE CALENDAR MISMATCH",
        "heartbeat": "DELIVERY HEARTBEAT",
    }
    alert_type = str(event.get("type", "calendar_alert"))
    lines = [
        f"UCSC Calendar | {type_labels.get(alert_type, alert_type.upper())}",
        f"Feed: {_safe_text(event.get('feed_id', 'all'), 200)}",
    ]
    if event.get("summary"):
        lines.append(f"Summary: {_safe_text(event['summary'])}")
    if event.get("uid"):
        lines.append(f"UID: {_safe_text(event['uid'], 300)}")
    if event.get("message"):
        lines.append(f"Details: {_safe_text(event['message'])}")
    if event.get("observed_at_utc"):
        lines.append(f"Observed: {_safe_text(event['observed_at_utc'], 80)}")
    lines.append(f"Fingerprint: {fingerprint}")
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
