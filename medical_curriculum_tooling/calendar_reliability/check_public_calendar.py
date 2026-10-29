#!/usr/bin/env python3
"""Independent, read-only monitor for the public calendar publication."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from urllib.request import Request, urlopen


DEFAULT_BASE_URL = "https://medschooloutsider.github.io/unicatt-rome-medicine-calendar"
FEED_ROUTES = (
    "unicatt/rome/medicine/year1-lessons",
    "unicatt/rome/medicine/year1-exams",
    "unicatt/rome/medicine/year2-lessons",
    "unicatt/rome/medicine/year2-exams",
    "unicatt/rome/medicine/year3-lessons",
    "unicatt/rome/medicine/year3-exams",
    "unicatt/rome/medicine/italy-holidays",
    "tartu/medicine/year1-lessons",
)


def fetch(url: str) -> bytes:
    request = Request(url, headers={"Accept": "application/json, text/calendar", "User-Agent": "unicatt-calendar-public-sentinel/1.0"})
    with urlopen(request, timeout=20) as response:
        return response.read()


def unfold(text: str) -> list[str]:
    lines: list[str] = []
    for line in text.splitlines():
        if line.startswith((" ", "\t")) and lines:
            lines[-1] += line[1:]
        else:
            lines.append(line)
    return lines


def check(base_url: str, max_age_seconds: int) -> dict[str, object]:
    observed = datetime.now(UTC)
    failures: list[str] = []
    feeds: dict[str, object] = {}
    base = base_url.rstrip("/")
    health_url = f"{base}/health.json"
    try:
        health = json.loads(fetch(health_url).decode("utf-8"))
        if not isinstance(health, dict):
            raise ValueError("health.json is not an object")
        lease = health.get("freshness_lease")
        expires_at = lease.get("expires_at_utc") if isinstance(lease, dict) else None
        lease_expired = False
        if expires_at:
            lease_expired = datetime.fromisoformat(str(expires_at)) < observed
        if health.get("ok") is not True:
            failures.append("health.json: aggregate status is not ok")
        if not expires_at or lease_expired:
            failures.append("health.json: freshness lease expired or missing")
    except Exception as exc:
        health = {"error": f"{type(exc).__name__}: {exc}"}
        failures.append(f"health.json: probe failed: {type(exc).__name__}: {exc}")
    for route in FEED_ROUTES:
        status_url = f"{base}/{route}-status.json"
        ics_url = f"{base}/{route}.ics"
        try:
            status = json.loads(fetch(status_url).decode("utf-8"))
            ics_text = fetch(ics_url).decode("utf-8")
            uids = [line[4:] for line in unfold(ics_text) if line.startswith("UID:")]
            status_uids = [str(row.get("uid")) for row in status.get("events", []) if isinstance(row, dict) and row.get("uid")]
            publication = status.get("last_publication_at_utc") or status.get("generated_at_utc")
            age = None if not publication else int((observed - datetime.fromisoformat(str(publication))).total_seconds())
            detail = {"publication": publication, "age_seconds": age, "event_count": len(uids), "healthy": status.get("healthy"), "stale": status.get("stale")}
            feeds[route] = detail
            if status.get("healthy") is not True or status.get("stale") is True or status.get("last_error"):
                failures.append(f"{route}: unhealthy status")
            if age is None or age < 0 or age > max_age_seconds:
                failures.append(f"{route}: publication age")
            if status.get("content_hash") != hashlib.sha256(ics_text.encode("utf-8")).hexdigest():
                failures.append(f"{route}: content hash mismatch")
            if len(uids) != len(set(uids)):
                failures.append(f"{route}: duplicate UID")
            if sorted(uids) != sorted(status_uids):
                failures.append(f"{route}: status/ICS UID mismatch")
        except Exception as exc:
            failures.append(f"{route}: probe failed: {type(exc).__name__}: {exc}")
            feeds[route] = {"error": f"{type(exc).__name__}: {exc}"}
    return {
        "ok": not failures,
        "checked_at_utc": observed.isoformat(),
        "base_url": base,
        "failures": failures,
        "health": health,
        "feeds": feeds,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--max-age-seconds", type=int, default=45 * 60)
    args = parser.parse_args()
    payload = check(args.base_url, args.max_age_seconds)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
