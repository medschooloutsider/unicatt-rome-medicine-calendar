#!/usr/bin/env python3
"""Durable calendar polling and static publication.

The existing normalizer remains the source-of-truth for event identities and
ICS formatting. This module owns the operational boundary around it:

* no Vercel Blob or other runtime provider is required;
* malformed, empty-after-history, or failed source reads never replace a good
  public feed;
* a missing future event is tentative after one good missing snapshot and
  cancelled only after a second good missing snapshot;
* explicit upstream suspensions are cancelled immediately;
* state, health, alerts, and publication hashes are visible on disk and can
  be served by GitHub Pages or mirrored to another static host.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import urlencode
from urllib.request import Request, urlopen

try:
    from medical_curriculum_tooling import unicatt_calendar_broadcast as source
except ModuleNotFoundError:  # Standalone publication bundle execution.
    import unicatt_calendar_broadcast as source  # type: ignore[no-redef]


STATE_VERSION = 1
DEFAULT_MAX_SOURCE_AGE_SECONDS = 30 * 60
DEFAULT_MAX_PUBLICATION_AGE_SECONDS = 45 * 60
DEFAULT_PUBLIC_BASE_URL = "https://medschooloutsider.github.io/unicatt-rome-medicine-calendar"
ALERT_DIRNAME = "alerts"
STATE_DIRNAME = "state"
PUBLIC_DIRNAME = "public"
FEED_IDS = tuple(source.FEED_SPECS.keys())


class UnsafeSnapshotError(RuntimeError):
    """A source response was not safe to publish."""


@dataclass(frozen=True)
class PipelinePaths:
    root: Path
    public: Path
    state: Path
    alerts: Path
    bootstrap: Path | None = None
    backup: Path | None = None

    @classmethod
    def from_root(
        cls,
        root: Path,
        *,
        public: Path | None = None,
        state: Path | None = None,
        bootstrap: Path | None = None,
        backup: Path | None = None,
    ) -> "PipelinePaths":
        root = root.resolve()
        return cls(
            root=root,
            public=(public or root / PUBLIC_DIRNAME).resolve(),
            state=(state or root / STATE_DIRNAME).resolve(),
            alerts=(root / ALERT_DIRNAME).resolve(),
            bootstrap=bootstrap.resolve() if bootstrap else None,
            backup=backup.resolve() if backup else None,
        )


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = text.encode("utf-8")
    try:
        if path.read_bytes() == encoded:
            return
    except FileNotFoundError:
        pass
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    return payload


def feed_public_paths(paths: PipelinePaths, feed_id: str) -> tuple[Path, Path]:
    spec = source.FEED_SPECS[feed_id]
    route = spec.route_path.lstrip("/")
    ics = paths.public / route
    return ics, ics.with_name(ics.name.replace(".ics", "-status.json"))


def state_path(paths: PipelinePaths, feed_id: str) -> Path:
    return paths.state / f"{feed_id}.json"


def source_event_hash(rows: Iterable[dict[str, Any]]) -> str:
    signatures = []
    for row in rows:
        signatures.append(source.event_signature(row))
    return sha256_text(canonical_json(sorted(signatures)))


def event_rows_from_state(state: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for record in state.get("ledger", {}).values():
        if isinstance(record, dict) and isinstance(record.get("event"), dict):
            rows.append(dict(record["event"]))
    return rows


def default_state(feed_id: str) -> dict[str, Any]:
    return {
        "version": STATE_VERSION,
        "feed_id": feed_id,
        "updated_at_utc": None,
        "last_successful_fetch_at_utc": None,
        "last_publication_at_utc": None,
        "last_failed_at_utc": None,
        "last_error": None,
        "consecutive_failures": 0,
        "health": "uninitialized",
        "stale": True,
        "source_snapshot_hash": None,
        "last_published_source_snapshot_hash": None,
        "published_ics_hash": None,
        "published_event_count": 0,
        "published_events": [],
        "ledger": {},
        "last_alerts": [],
    }


def state_from_status_payload(feed_id: str, payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict) or not isinstance(payload.get("events"), list):
        return None
    observed = payload.get("generated_at_utc") or payload.get("last_successful_fetch_at_utc")
    ledger: dict[str, dict[str, Any]] = {}
    for row in payload["events"]:
        if not isinstance(row, dict) or not row.get("uid"):
            continue
        uid = str(row["uid"])
        ledger[uid] = {
            "uid": uid,
            "event": dict(row),
            "event_signature": source.event_signature(row),
            "first_seen_at_utc": observed,
            "last_seen_at_utc": observed,
            "last_missing_at_utc": None,
            "missing_successes": 0,
            "last_snapshot_present": True,
            "retention_state": "public_status_recovery",
        }
    state = default_state(feed_id)
    state.update(
        {
            "updated_at_utc": observed,
            "last_successful_fetch_at_utc": payload.get("last_successful_fetch_at_utc") or observed,
            "last_publication_at_utc": payload.get("last_publication_at_utc") or observed,
            "health": payload.get("health") or ("healthy" if ledger or payload.get("event_count") == 0 else "degraded"),
            "stale": bool(payload.get("stale", False)),
            "source_snapshot_hash": payload.get("source_snapshot_hash"),
            "last_published_source_snapshot_hash": payload.get("last_published_source_snapshot_hash"),
            "published_ics_hash": payload.get("content_hash"),
            "published_event_count": len(ledger),
            "published_events": [record["event"] for record in ledger.values()],
            "ledger": ledger,
            "last_error": payload.get("last_error"),
            "consecutive_failures": int(payload.get("consecutive_failures") or 0),
            "last_alerts": payload.get("alerts") if isinstance(payload.get("alerts"), list) else [],
        }
    )
    return state


def bootstrap_state(paths: PipelinePaths, feed_id: str) -> dict[str, Any]:
    state = default_state(feed_id)
    if paths.bootstrap is None:
        return state

    spec = source.FEED_SPECS[feed_id]
    candidates = [
        paths.bootstrap / f"{feed_id}-status.json",
        paths.bootstrap / spec.route_path.lstrip("/").replace(".ics", "-status.json"),
    ]
    status_path = next((candidate for candidate in candidates if candidate.exists()), None)
    if status_path is None:
        return state
    try:
        payload = read_json(status_path)
    except (OSError, UnicodeError, ValueError):
        return state
    recovered = state_from_status_payload(feed_id, payload)
    return recovered or state


def recover_state_from_public(paths: PipelinePaths, feed_id: str) -> dict[str, Any] | None:
    _, status_path = feed_public_paths(paths, feed_id)
    if not status_path.exists():
        return None
    try:
        payload = read_json(status_path)
    except (OSError, UnicodeError, ValueError):
        return None
    recovered = state_from_status_payload(feed_id, payload)
    if recovered is not None:
        recovered["last_alerts"] = [{"type": "state_recovered_from_public_status"}]
    return recovered


def load_state(paths: PipelinePaths, feed_id: str) -> dict[str, Any]:
    try:
        payload = read_json(state_path(paths, feed_id))
    except (OSError, UnicodeError, ValueError):
        return recover_state_from_public(paths, feed_id) or bootstrap_state(paths, feed_id)
    if not payload or payload.get("version") != STATE_VERSION:
        return recover_state_from_public(paths, feed_id) or bootstrap_state(paths, feed_id)
    return payload


def save_state(paths: PipelinePaths, state: dict[str, Any]) -> None:
    atomic_write_json(state_path(paths, str(state["feed_id"])), state)


def validate_source_shape(url: str, payload: Any) -> Any:
    if url in {source.LESSONS_ENDPOINT, source.EXAMS_ENDPOINT}:
        if not isinstance(payload, dict):
            raise UnsafeSnapshotError(f"{url} returned {type(payload).__name__}, expected object")
    elif not isinstance(payload, (list, dict)):
        raise UnsafeSnapshotError(f"{url} returned {type(payload).__name__}, expected list/object")
    return payload


def build_fresh_artifact(
    feed_id: str,
    *,
    fetcher: Callable[[str, dict[str, str]], Any] | None = None,
) -> source.FeedArtifact:
    spec = source.FEED_SPECS[feed_id]

    def checked_fetcher(url: str, payload: dict[str, str]) -> Any:
        result = (fetcher or source.fetch_json)(url, payload)
        return validate_source_shape(url, result)

    try:
        artifact = source.build_feed_artifact(spec, fetcher=checked_fetcher)
    except UnsafeSnapshotError:
        raise
    except Exception as exc:
        raise UnsafeSnapshotError(f"{type(exc).__name__}: {exc}") from exc

    rows = [row for row in artifact.events if isinstance(row, dict) and row.get("uid")]
    if len(rows) != len(artifact.events):
        raise UnsafeSnapshotError(f"{feed_id} contained an event without a UID")
    uids = [str(row["uid"]) for row in rows]
    if len(set(uids)) != len(uids):
        raise UnsafeSnapshotError(f"{feed_id} contained duplicate event UIDs")
    return artifact


def parse_event_end(row: dict[str, Any]) -> datetime:
    value = datetime.fromisoformat(str(row["end"]))
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def append_note(description: str, note: str) -> str:
    marker = f"Calendar reliability note: {note}"
    if marker in description:
        return description
    return f"{description}\n\n{marker}" if description else marker


def change_record(
    previous: dict[str, Any] | None,
    event_row: dict[str, Any],
    observed: str,
    *,
    present: bool,
    missing_successes: int = 0,
    retention_state: str,
) -> dict[str, Any]:
    previous_event = previous.get("event") if isinstance(previous, dict) else None
    previous_signature = previous.get("event_signature") if isinstance(previous, dict) else None
    current_signature = source.event_signature(event_row)
    sequence = int((previous_event or {}).get("sequence") or 0) if previous_event else int(event_row.get("sequence") or 0)
    last_modified = (previous_event or {}).get("last_modified_utc") or event_row.get("last_modified_utc")
    if previous_signature and previous_signature != current_signature:
        sequence += 1
        last_modified = observed
    event_row["sequence"] = sequence
    event_row["last_modified_utc"] = last_modified or observed
    return {
        "uid": str(event_row["uid"]),
        "event": event_row,
        "event_signature": source.event_signature(event_row),
        "first_seen_at_utc": (previous or {}).get("first_seen_at_utc") or observed,
        "last_seen_at_utc": observed if present else (previous or {}).get("last_seen_at_utc"),
        "last_missing_at_utc": None if present else observed,
        "missing_successes": missing_successes,
        "last_snapshot_present": present,
        "retention_state": retention_state,
    }


def apply_snapshot(
    feed_id: str,
    state: dict[str, Any],
    fresh: source.FeedArtifact,
    observed_dt: datetime,
) -> tuple[source.FeedArtifact, dict[str, Any], list[dict[str, Any]]]:
    observed = iso(observed_dt)
    fresh_rows = {str(row["uid"]): dict(row) for row in fresh.events}
    prior_ledger = state.get("ledger") if isinstance(state.get("ledger"), dict) else {}
    ledger: dict[str, dict[str, Any]] = {}
    alerts: list[dict[str, Any]] = []

    for uid, row in fresh_rows.items():
        previous = prior_ledger.get(uid) if isinstance(prior_ledger.get(uid), dict) else None
        previous_event = previous.get("event", {}) if previous else {}
        if row.get("status") == "CANCELLED" and previous_event.get("status") != "CANCELLED":
            alerts.append({"type": "explicit_cancellation", "uid": uid, "summary": row.get("summary", "")})
            retention_state = "explicitly_cancelled"
        else:
            retention_state = "present_in_latest_snapshot"
        ledger[uid] = change_record(previous, row, observed, present=True, retention_state=retention_state)

    for uid, previous in prior_ledger.items():
        if uid in fresh_rows or not isinstance(previous, dict) or not isinstance(previous.get("event"), dict):
            continue
        retained = dict(previous["event"])
        missing_successes = int(previous.get("missing_successes") or 0) + 1
        is_past = parse_event_end(retained) <= observed_dt
        if is_past:
            retained["description"] = append_note(
                str(retained.get("description") or ""),
                "retained because the event is past and absent from the upstream snapshot.",
            )
            retention_state = "retained_past_missing"
        elif missing_successes >= 2:
            old_status = retained.get("status")
            retained["status"] = "CANCELLED"
            retained["description"] = append_note(
                str(retained.get("description") or ""),
                "cancelled after two consecutive valid upstream snapshots omitted this future event.",
            )
            retention_state = "cancelled_after_repeated_missing"
            if old_status != "CANCELLED":
                alerts.append({"type": "confirmed_cancellation", "uid": uid, "summary": retained.get("summary", "")})
        else:
            retained["status"] = "TENTATIVE"
            retained["description"] = append_note(
                str(retained.get("description") or ""),
                "tentative because one valid upstream snapshot omitted this future event; awaiting confirmation.",
            )
            retention_state = "tentative_after_first_missing"
            alerts.append({"type": "possible_cancellation", "uid": uid, "summary": retained.get("summary", "")})
        ledger[uid] = change_record(
            previous,
            retained,
            observed,
            present=False,
            missing_successes=missing_successes,
            retention_state=retention_state,
        )

    merged_rows = [record["event"] for record in ledger.values()]
    merged_rows.sort(key=lambda row: (str(row.get("start", "")), str(row.get("summary", "")), str(row.get("uid", ""))))
    merged_events = [source.event_from_status_row(row) for row in merged_rows]
    content = source.render_ics(
        source.FEED_SPECS[feed_id].calendar_name,
        source.FEED_SPECS[feed_id].description,
        merged_events,
        timezone_id=source.FEED_SPECS[feed_id].timezone_id,
    )
    published = source.build_artifact(
        source.FEED_SPECS[feed_id],
        merged_events,
        content,
        observed_dt,
        dict(fresh.source_counts),
        list(fresh.source_urls),
        "fresh",
    )
    return published, ledger, alerts


def status_payload(
    feed_id: str,
    state: dict[str, Any],
    *,
    now: datetime,
    source_counts: dict[str, int] | None = None,
    source_urls: list[str] | None = None,
    source_payload: dict[str, str] | None = None,
) -> dict[str, Any]:
    rows = state.get("published_events") if isinstance(state.get("published_events"), list) else event_rows_from_state(state)
    last_success = state.get("last_successful_fetch_at_utc")
    age = None
    if last_success:
        try:
            age = max(0, int((now - datetime.fromisoformat(str(last_success))).total_seconds()))
        except ValueError:
            age = None
    last_publication = state.get("last_publication_at_utc")
    lease_expires = None
    if last_publication:
        try:
            lease_expires = iso(
                datetime.fromisoformat(str(last_publication))
                + timedelta(seconds=DEFAULT_MAX_PUBLICATION_AGE_SECONDS)
            )
        except ValueError:
            lease_expires = None
    return {
        "feed_id": feed_id,
        "calendar_name": source.FEED_SPECS[feed_id].calendar_name,
        "generated_at_utc": iso(now),
        "last_successful_fetch_at_utc": last_success,
        "last_publication_at_utc": state.get("last_publication_at_utc"),
        "freshness_lease": {
            "max_age_seconds": DEFAULT_MAX_PUBLICATION_AGE_SECONDS,
            "issued_at_utc": last_publication,
            "expires_at_utc": lease_expires,
        },
        "generated_age_seconds": age,
        "event_count": len(rows),
        "cancelled_count": sum(1 for row in rows if row.get("status") == "CANCELLED"),
        "tentative_count": sum(1 for row in rows if row.get("status") == "TENTATIVE"),
        "source_counts": source_counts or {},
        "source_payload": source_payload or dict(source.FEED_SPECS[feed_id].payload),
        "source_urls": source_urls or [],
        "source_snapshot_hash": state.get("source_snapshot_hash"),
        "last_published_source_snapshot_hash": state.get("last_published_source_snapshot_hash"),
        "content_hash": state.get("published_ics_hash"),
        "publication_mode": "github-pages-static",
        "dynamic_refresh": "GitHub Actions poller; no Vercel Blob dependency",
        "health": state.get("health"),
        "healthy": state.get("health") == "healthy",
        "stale": bool(state.get("stale", True)),
        "consecutive_failures": int(state.get("consecutive_failures") or 0),
        "last_error": state.get("last_error"),
        "alerts": state.get("last_alerts", []),
        "retention_policy": source.RETENTION_POLICY,
        "events": rows,
    }


def publication_targets(paths: PipelinePaths) -> list[Path]:
    targets = [paths.public]
    if paths.backup is not None:
        targets.append(paths.backup)
    return targets


def publish_files(paths: PipelinePaths, feed_id: str, ics_text: str, status: dict[str, Any]) -> None:
    spec = source.FEED_SPECS[feed_id]
    route = spec.route_path.lstrip("/")
    for target_root in publication_targets(paths):
        ics_path = target_root / route
        status_path = ics_path.with_name(ics_path.name.replace(".ics", "-status.json"))
        atomic_write_text(ics_path, ics_text)
        atomic_write_json(status_path, status)
        if sha256_text(ics_path.read_bytes().decode("utf-8")) != status["content_hash"]:
            raise IOError(f"published ICS hash mismatch: {ics_path}")


def record_alert(paths: PipelinePaths, feed_id: str, alert: dict[str, Any], now: datetime) -> None:
    payload = {"feed_id": feed_id, "observed_at_utc": iso(now), **alert}
    digest = sha256_text(canonical_json(payload))[:16]
    atomic_write_json(paths.alerts / f"{iso(now).replace(':', '').replace('+00:00', 'Z')}-{feed_id}-{digest}.json", payload)
    atomic_write_json(paths.alerts / "latest.json", payload)
    token = os.environ.get("CALENDAR_ALERT_TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("CALENDAR_ALERT_TELEGRAM_CHAT_ID")
    if token and chat_id:
        message = f"UCSC calendar alert [{feed_id}] {alert.get('type')}: {alert.get('summary', alert.get('message', ''))}"
        try:
            request = Request(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data=urlencode({"chat_id": chat_id, "text": message}).encode("utf-8"),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                method="POST",
            )
            with urlopen(request, timeout=10):
                pass
        except Exception:
            # Alerts must not make a valid calendar publication fail. The JSON
            # alert remains the durable, provider-free receipt.
            pass
    if os.environ.get("CALENDAR_ENABLE_NATIVE_ALERTS") == "1" and platform.system() == "Darwin":
        message = f"UCSC calendar alert [{feed_id}] {alert.get('type')}: {alert.get('summary', alert.get('message', ''))}"
        try:
            subprocess.run(
                ["osascript", "-e", f'display notification "{message.replace(chr(34), chr(39))}" with title "Calendar pipeline"'],
                check=False,
                capture_output=True,
                text=True,
            )
        except Exception:
            pass


def fail_state(state: dict[str, Any], feed_id: str, now: datetime, error: Exception) -> dict[str, Any]:
    state = dict(state)
    state.update(
        {
            "version": STATE_VERSION,
            "feed_id": feed_id,
            "updated_at_utc": iso(now),
            "last_failed_at_utc": iso(now),
            "last_error": {"type": type(error).__name__, "message": str(error)},
            "consecutive_failures": int(state.get("consecutive_failures") or 0) + 1,
            "health": "degraded",
            "stale": True,
        }
    )
    return state


def run_feed(
    feed_id: str,
    paths: PipelinePaths,
    *,
    now: datetime | None = None,
    fetcher: Callable[[str, dict[str, str]], Any] | None = None,
    enable_alerts: bool = True,
) -> dict[str, Any]:
    if feed_id not in source.FEED_SPECS:
        raise KeyError(feed_id)
    observed_dt = now or utc_now()
    state = load_state(paths, feed_id)
    prior_rows = event_rows_from_state(state)
    try:
        fresh = build_fresh_artifact(feed_id, fetcher=fetcher)
        if not fresh.events and prior_rows:
            has_future_history = any(parse_event_end(row) > observed_dt for row in prior_rows)
            if has_future_history:
                raise UnsafeSnapshotError(
                    "valid source response contained zero events while future durable history exists"
                )
        published, ledger, alerts = apply_snapshot(feed_id, state, fresh, observed_dt)
        snapshot_hash = source_event_hash(fresh.events)
        state = dict(state)
        state.update(
            {
                "version": STATE_VERSION,
                "feed_id": feed_id,
                "updated_at_utc": iso(observed_dt),
                "last_successful_fetch_at_utc": iso(observed_dt),
                "last_failed_at_utc": None,
                "last_error": None,
                "consecutive_failures": 0,
                "health": "healthy",
                "stale": False,
                "source_snapshot_hash": snapshot_hash,
                "last_published_source_snapshot_hash": snapshot_hash,
                "published_ics_hash": sha256_text(published.ics_text),
                "published_event_count": published.event_count,
                "published_events": [dict(row) for row in published.events],
                "ledger": ledger,
                "last_alerts": alerts,
            }
        )
        status = status_payload(
            feed_id,
            state,
            now=observed_dt,
            source_counts=published.source_counts,
            source_urls=published.source_urls,
            source_payload=published.source_payload,
        )
        publish_files(paths, feed_id, published.ics_text, status)
        state["last_publication_at_utc"] = iso(observed_dt)
        status = status_payload(
            feed_id,
            state,
            now=observed_dt,
            source_counts=published.source_counts,
            source_urls=published.source_urls,
            source_payload=published.source_payload,
        )
        for target_root in publication_targets(paths):
            ics_path = target_root / source.FEED_SPECS[feed_id].route_path.lstrip("/")
            atomic_write_json(ics_path.with_name(ics_path.name.replace(".ics", "-status.json")), status)
        save_state(paths, state)
        for alert in alerts:
            if enable_alerts:
                record_alert(paths, feed_id, alert, observed_dt)
        return {"feed_id": feed_id, "ok": True, "health": "healthy", "event_count": published.event_count, "alerts": alerts}
    except Exception as exc:
        state = fail_state(state, feed_id, observed_dt, exc)
        status = status_payload(feed_id, state, now=observed_dt)
        try:
            for target_root in publication_targets(paths):
                ics_path = target_root / source.FEED_SPECS[feed_id].route_path.lstrip("/")
                if ics_path.exists():
                    atomic_write_json(ics_path.with_name(ics_path.name.replace(".ics", "-status.json")), status)
            save_state(paths, state)
        finally:
            if enable_alerts:
                record_alert(paths, feed_id, {"type": "pipeline_failure", "message": str(exc)}, observed_dt)
        return {"feed_id": feed_id, "ok": False, "health": "degraded", "error": type(exc).__name__, "message": str(exc)}


def run_all(
    paths: PipelinePaths,
    *,
    feeds: Iterable[str] = FEED_IDS,
    now: datetime | None = None,
    fetcher: Callable[[str, dict[str, str]], Any] | None = None,
    enable_alerts: bool = True,
) -> dict[str, Any]:
    selected_feeds = tuple(feeds)
    results = [run_feed(feed_id, paths, now=now, fetcher=fetcher, enable_alerts=enable_alerts) for feed_id in selected_feeds]
    observed = now or utc_now()
    leases: list[str] = []
    for feed_id in selected_feeds:
        _, status_path = feed_public_paths(paths, feed_id)
        status = read_json(status_path) or {}
        expires = ((status.get("freshness_lease") or {}).get("expires_at_utc"))
        if isinstance(expires, str):
            leases.append(expires)
    payload = {
        "ok": all(result["ok"] for result in results),
        "observed_at_utc": iso(observed),
        "freshness_lease": {
            "max_age_seconds": DEFAULT_MAX_PUBLICATION_AGE_SECONDS,
            "expires_at_utc": min(leases) if leases else None,
            "feed_count": len(selected_feeds),
        },
        "results": results,
    }
    atomic_write_json(paths.public / "health.json", payload)
    return payload


def verify_publication(paths: PipelinePaths, feed_ids: Iterable[str] = FEED_IDS) -> dict[str, Any]:
    errors: list[str] = []
    checked = 0

    def unfold_ics(text: str) -> list[str]:
        lines: list[str] = []
        for line in text.splitlines():
            if line.startswith(" ") and lines:
                lines[-1] += line[1:]
            else:
                lines.append(line)
        return lines

    for feed_id in feed_ids:
        ics_path, status_path = feed_public_paths(paths, feed_id)
        if not ics_path.exists() or not status_path.exists():
            errors.append(f"missing publication: {feed_id}")
            continue
        checked += 1
        status = read_json(status_path) or {}
        ics_text = ics_path.read_bytes().decode("utf-8")
        if status.get("content_hash") != sha256_text(ics_text):
            errors.append(f"hash mismatch: {feed_id}")
        uids = [line[4:] for line in unfold_ics(ics_text) if line.startswith("UID:")]
        if len(uids) != len(set(uids)):
            errors.append(f"duplicate UID: {feed_id}")
        status_uids = [str(row.get("uid")) for row in status.get("events", []) if isinstance(row, dict)]
        if sorted(uids) != sorted(status_uids):
            errors.append(f"status/ICS UID mismatch: {feed_id}")
    return {"ok": not errors, "checked_feeds": checked, "errors": errors}


def unfold_ics_lines(text: str) -> list[str]:
    lines: list[str] = []
    for line in text.splitlines():
        if line.startswith((" ", "\t")) and lines:
            lines[-1] += line[1:]
        else:
            lines.append(line)
    return lines


def remote_watchdog(
    base_url: str = DEFAULT_PUBLIC_BASE_URL,
    *,
    now: datetime | None = None,
    max_publication_age_seconds: int = DEFAULT_MAX_PUBLICATION_AGE_SECONDS,
    feed_ids: Iterable[str] = FEED_IDS,
    opener: Callable[..., Any] = urlopen,
    sync_paths: PipelinePaths | None = None,
) -> dict[str, Any]:
    """Verify the hosted publication that Apple Calendar actually consumes."""
    observed = now or utc_now()
    base = base_url.rstrip("/")
    selected_feeds = tuple(feed_ids)
    failures: list[str] = []
    details: dict[str, Any] = {}
    remote_payloads: dict[str, tuple[dict[str, Any], str]] = {}

    def fetch(url: str) -> bytes:
        request = Request(url, headers={"Accept": "application/json, text/calendar", "User-Agent": "unicatt-calendar-backstop/1.0"})
        with opener(request, timeout=20) as response:
            return response.read()

    for feed_id in feed_ids:
        spec = source.FEED_SPECS[feed_id]
        route = spec.route_path.lstrip("/")
        status_url = f"{base}/{route.replace('.ics', '-status.json')}"
        ics_url = f"{base}/{route}"
        feed_details: dict[str, Any] = {"status_url": status_url, "ics_url": ics_url}
        try:
            status = json.loads(fetch(status_url).decode("utf-8"))
            ics_text = fetch(ics_url).decode("utf-8")
            remote_payloads[feed_id] = (status, ics_text)
            generated = status.get("last_publication_at_utc") or status.get("generated_at_utc")
            age_seconds = None
            if generated:
                age_seconds = int((observed - datetime.fromisoformat(str(generated))).total_seconds())
            uids = [line[4:] for line in unfold_ics_lines(ics_text) if line.startswith("UID:")]
            status_uids = [str(row.get("uid")) for row in status.get("events", []) if isinstance(row, dict) and row.get("uid")]
            feed_details.update(
                {
                    "generated_at_utc": generated,
                    "publication_age_seconds": age_seconds,
                    "healthy": status.get("healthy"),
                    "stale": status.get("stale"),
                    "last_error": status.get("last_error"),
                    "event_count": len(uids),
                }
            )
            if status.get("healthy") is not True or status.get("stale") is True or status.get("last_error"):
                failures.append(f"{feed_id}: hosted status unhealthy")
            if age_seconds is None or age_seconds < 0 or age_seconds > max_publication_age_seconds:
                failures.append(f"{feed_id}: hosted publication freshness")
            if status.get("content_hash") != sha256_text(ics_text):
                failures.append(f"{feed_id}: hosted content hash mismatch")
            if len(uids) != len(set(uids)):
                failures.append(f"{feed_id}: hosted duplicate UID")
            if sorted(uids) != sorted(status_uids):
                failures.append(f"{feed_id}: hosted status/ICS UID mismatch")
        except Exception as exc:
            feed_details["error"] = f"{type(exc).__name__}: {exc}"
            failures.append(f"{feed_id}: hosted probe failed")
        details[feed_id] = feed_details

    if not failures and sync_paths is not None:
        try:
            for feed_id in selected_feeds:
                status, ics_text = remote_payloads[feed_id]
                publish_files(sync_paths, feed_id, ics_text, status)
                recovered = state_from_status_payload(feed_id, status)
                if recovered is not None:
                    recovered["last_alerts"] = [{"type": "remote_publication_sync"}]
                    save_state(sync_paths, recovered)
            atomic_write_json(sync_paths.public / "remote-watchdog.json", {"ok": True, "checked_at_utc": iso(observed), "base_url": base, "feeds": list(selected_feeds)})
        except Exception as exc:
            failures.append(f"local remote mirror failed: {type(exc).__name__}: {exc}")

    return {
        "ok": not failures,
        "checked_at_utc": iso(observed),
        "base_url": base,
        "failures": failures,
        "feeds": details,
    }


def watchdog(
    paths: PipelinePaths,
    *,
    now: datetime | None = None,
    max_source_age_seconds: int = DEFAULT_MAX_SOURCE_AGE_SECONDS,
    max_publication_age_seconds: int = DEFAULT_MAX_PUBLICATION_AGE_SECONDS,
    feed_ids: Iterable[str] = FEED_IDS,
) -> dict[str, Any]:
    observed = now or utc_now()
    failures: list[str] = []
    details: dict[str, Any] = {}
    for feed_id in feed_ids:
        state = load_state(paths, feed_id)
        last_success = state.get("last_successful_fetch_at_utc")
        last_publication = state.get("last_publication_at_utc")
        source_age = None
        publication_age = None
        if last_success:
            source_age = int((observed - datetime.fromisoformat(str(last_success))).total_seconds())
        if last_publication:
            publication_age = int((observed - datetime.fromisoformat(str(last_publication))).total_seconds())
        if source_age is None or source_age > max_source_age_seconds:
            failures.append(f"{feed_id}: source freshness");
        if publication_age is None or publication_age > max_publication_age_seconds:
            failures.append(f"{feed_id}: publication freshness");
        if state.get("health") != "healthy":
            failures.append(f"{feed_id}: {state.get('health')}")
        if state.get("source_snapshot_hash") != state.get("last_published_source_snapshot_hash"):
            failures.append(f"{feed_id}: source changed but publication hash did not advance")
        details[feed_id] = {"source_age_seconds": source_age, "publication_age_seconds": publication_age, "health": state.get("health"), "last_error": state.get("last_error")}
    publication = verify_publication(paths, feed_ids)
    failures.extend(publication["errors"])
    payload = {"ok": not failures, "checked_at_utc": iso(observed), "failures": failures, "feeds": details, "publication": publication}
    atomic_write_json(paths.public / "watchdog.json", payload)
    if failures:
        record_alert(paths, "all", {"type": "watchdog_failure", "message": "; ".join(failures)}, observed)
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("run", "watchdog", "verify", "remote-watchdog"), nargs="?", default="run")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--public", type=Path)
    parser.add_argument("--state", type=Path)
    parser.add_argument("--bootstrap", type=Path)
    parser.add_argument("--backup", type=Path)
    parser.add_argument("--feed", action="append", dest="feeds")
    parser.add_argument("--remote-base", default=DEFAULT_PUBLIC_BASE_URL)
    parser.add_argument("--max-publication-age-seconds", type=int, default=DEFAULT_MAX_PUBLICATION_AGE_SECONDS)
    parser.add_argument("--sync-local", action="store_true")
    parser.add_argument("--no-alerts", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    paths = PipelinePaths.from_root(args.root, public=args.public, state=args.state, bootstrap=args.bootstrap, backup=args.backup)
    feeds = tuple(args.feeds or FEED_IDS)
    if args.command == "run":
        payload = run_all(paths, feeds=feeds, enable_alerts=not args.no_alerts)
    elif args.command == "watchdog":
        payload = watchdog(paths, feed_ids=feeds)
    elif args.command == "remote-watchdog":
        payload = remote_watchdog(
            args.remote_base,
            feed_ids=feeds,
            max_publication_age_seconds=args.max_publication_age_seconds,
            sync_paths=paths if args.sync_local else None,
        )
    else:
        payload = verify_publication(paths, feeds)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())

