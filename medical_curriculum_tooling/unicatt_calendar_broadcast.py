#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
from dataclasses import asdict, dataclass, fields
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

try:
    from school_intelligence.social.scripts.esn_rome_fourvenues_calendar import (
        handle_route as handle_esn_rome_social_route,
    )
except ModuleNotFoundError:
    try:
        from medical_curriculum_tooling.school_intelligence.social.scripts.esn_rome_fourvenues_calendar import (
            handle_route as handle_esn_rome_social_route,
        )
    except Exception as exc:  # pragma: no cover - surfaced through the status route
        handle_esn_rome_social_route = None
        ESN_ROME_SOCIAL_IMPORT_ERROR = exc
    else:
        ESN_ROME_SOCIAL_IMPORT_ERROR = None
except Exception as exc:  # pragma: no cover - surfaced through the status route
    handle_esn_rome_social_route = None
    ESN_ROME_SOCIAL_IMPORT_ERROR = exc
else:
    ESN_ROME_SOCIAL_IMPORT_ERROR = None


ROME_TZ = ZoneInfo("Europe/Rome")
RETENTION_POLICY = "Entries are NOT erased as days pass. Past entries remain in place indefinitely for future audit. Corrections and cancellations retain event identity; no rolling date cutoff is permitted for this or any future station."
UPSTREAM_BASE = "https://secure.unicatt.it/didatticaweb2"
LESSONS_ENDPOINT = f"{UPSTREAM_BASE}/ps/lezioni/"
EXAMS_ENDPOINT = f"{UPSTREAM_BASE}/ps/esami/"
ROOM_CHANGES_ENDPOINT = f"{UPSTREAM_BASE}/cambiaula/"
EXTRA_LESSONS_ENDPOINT = f"{UPSTREAM_BASE}/lezionistraordinarie/"
SUSPENSIONS_ENDPOINT = f"{UPSTREAM_BASE}/sospensioni/"
LESSON_SOURCE_TAG = LESSONS_ENDPOINT

DEFAULT_CACHE_DIR = Path(os.environ.get("UNICATT_CALENDAR_CACHE_DIR", "/tmp/unicatt_calendar_broadcast" if os.environ.get("VERCEL") else str(Path(__file__).resolve().parent / "unicatt_calendar_history"))).expanduser()
SEED_DIR = Path(__file__).resolve().parent / "unicatt_calendar_seed"
REFRESH_LOCAL_HOURS = (5, 6, 9, 10, 13, 14, 17, 18, 21, 22)
HEALTHY_MAX_AGE_SECONDS = 60 * 60 * 36
LEDGER_TTL_SECONDS = 60 * 60 * 24 * 550
HISTORY_LEDGER_PREFIX = "calendar-ledger"
HISTORY_SNAPSHOT_PREFIX = "calendar-history"
ESN_ROME_SOCIAL_ROUTE_PATHS = {
    "/social/esn-rome/fourvenues.ics",
    "/social/esn-rome/fourvenues-status.json",
    "/api/refresh-social",
    "/api/refresh-esn-rome-social",
}


@dataclass(frozen=True)
class FeedSpec:
    feed_id: str
    kind: str
    calendar_name: str
    payload: dict[str, str]
    description: str
    route_path: str
    timezone_id: str = "Europe/Rome"


@dataclass
class CalendarEvent:
    uid: str
    start: datetime
    end: datetime
    summary: str
    location: str
    description: str
    status: str | None
    source_record_id: str
    source_kind: str
    source_date: str
    metadata: dict[str, Any]
    sequence: int = 0
    last_modified_utc: str | None = None


@dataclass
class FeedArtifact:
    feed_id: str
    calendar_name: str
    generated_at_utc: str
    generated_for_local_date: str
    event_count: int
    cancelled_count: int
    source_counts: dict[str, int]
    source_payload: dict[str, str]
    source_urls: list[str]
    content_hash: str
    cache_mode: str
    events: list[dict[str, Any]]
    ics_text: str


@dataclass
class HttpResponse:
    status: int
    headers: dict[str, str]
    body: bytes


FEED_SPECS: dict[str, FeedSpec] = {
    "year2-lessons": FeedSpec(
        feed_id="year2-lessons",
        kind="lessons",
        calendar_name="UCSC Rome Medicine Year 2 Lessons",
        payload={
            "codiceSede": "RM",
            "annoAccademico": "2025",
            "codiceFacolta": "75012",
            "codiceCorsoDiLaurea": "0H0B",
            "annoDiCorso": "2",
        },
        description="Realtime Year 2 lesson feed for UCSC Rome Medicine and Surgery.",
        route_path="/unicatt/rome/medicine/year2-lessons.ics",
    ),
    "year2-exams": FeedSpec(
        feed_id="year2-exams",
        kind="exams",
        calendar_name="UCSC Rome Medicine Year 2 Exams",
        payload={
            "codiceSede": "RM",
            "annoAccademico": "2025",
            "codiceFacolta": "75012",
            "codiceCorsoDiLaurea": "0H0B",
            "annoDiCorso": "2",
        },
        description="Realtime Year 2 exam feed for UCSC Rome Medicine and Surgery.",
        route_path="/unicatt/rome/medicine/year2-exams.ics",
    ),
    "year1-exams": FeedSpec(
        feed_id="year1-exams",
        kind="exams",
        calendar_name="UCSC Rome Medicine Year 1 Exams",
        payload={
            "codiceSede": "RM",
            "annoAccademico": "2025",
            "codiceFacolta": "75012",
            "codiceCorsoDiLaurea": "0H0D",
            "annoDiCorso": "1",
        },
        description="Realtime Year 1 exam feed for UCSC Rome Medicine and Surgery.",
        route_path="/unicatt/rome/medicine/year1-exams.ics",
    ),
}

for _year, _academic, _kind in ((1, "2024", "lessons"), (3, "2026", "lessons"), (3, "2026", "exams")):
    _id = f"year{_year}-{_kind}"
    FEED_SPECS[_id] = FeedSpec(_id, _kind, f"UCSC Rome Medicine Year {_year} {_kind.title()}",
        {"codiceSede": "RM", "annoAccademico": _academic, "codiceFacolta": "75012", "codiceCorsoDiLaurea": "0H0B", "annoDiCorso": str(_year)},
        f"2024-2030 cohort; academic year {_academic}-{int(_academic)+1}. " + ("Historical reconstruction; incomplete coverage must not be interpreted as no lessons." if _year == 1 else "Official published timetable; empty means no dates currently published, not no teaching or exams."),
        f"/unicatt/rome/medicine/{_id}.ics")

FEED_SPECS["italy-holidays"] = FeedSpec("italy-holidays", "holidays", "Italy Holidays", {},
    "Italian national public holidays, 2024 onward; all-day dates. Rome patronal holiday and ordinary Sundays excluded. Future law changes require review.",
    "/unicatt/rome/medicine/italy-holidays.ics")

FEED_SPECS["tartu-y1-lessons"] = FeedSpec(
    "tartu-y1-lessons",
    "tartu-lessons",
    "University of Tartu Medicine Year 1 Lessons",
    {"curriculum": "118617", "academic_year": "2024/2025", "year": "1"},
    "Historical University of Tartu English Medicine Year 1 timetable station built from dated official OIS and course timetable evidence.",
    "/tartu/medicine/year1-lessons.ics",
    "Europe/Tallinn",
)


def load_runtime_cache() -> Any | None:
    try:
        from vercel.functions import RuntimeCache
        return RuntimeCache(namespace="unicatt-calendar-broadcast")
    except Exception:
        # The feed must remain usable when the optional Vercel cache provider
        # is unavailable. Provider failure is handled by the caller as a
        # visible stale/degraded state, never as an import-time crash.
        return None


def ensure_cache_dir() -> Path:
    DEFAULT_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return DEFAULT_CACHE_DIR


def ensure_seed_dir() -> Path:
    SEED_DIR.mkdir(parents=True, exist_ok=True)
    return SEED_DIR


def file_cache_path(key: str) -> Path:
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()
    return ensure_cache_dir() / f"{digest}.json"


def cache_get(key: str, runtime_cache: Any | None) -> dict[str, Any] | None:
    if runtime_cache is not None:
        value = runtime_cache.get(key)
        if isinstance(value, dict):
            return value
    path = file_cache_path(key)
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return None


def cache_set(key: str, payload: dict[str, Any], runtime_cache: Any | None, ttl_seconds: int = 172800) -> None:
    if runtime_cache is not None:
        runtime_cache.set(key, payload, {"ttl": ttl_seconds, "tags": ["unicatt-calendar", key]})
    path = file_cache_path(key)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def fetch_json(url: str, payload: dict[str, str]) -> Any:
    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=45) as response:
        return json.load(response)


def parse_local_datetime(date_text: str, time_text: str) -> datetime:
    return datetime.strptime(f"{date_text} {time_text}", "%d/%m/%Y %H:%M").replace(tzinfo=ROME_TZ)


def compact_whitespace(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split())


def build_location(building: str | None, room: str | None) -> str:
    parts = [compact_whitespace(building), compact_whitespace(room)]
    return " - ".join(part for part in parts if part)


def merge_reason(existing: Any, incoming: Any) -> str | None:
    current = compact_whitespace(str(existing)) if existing is not None else ""
    addition = compact_whitespace(str(incoming)) if incoming is not None else ""
    if not current:
        return addition or None
    if not addition or addition == current:
        return current
    return f"{current}; {addition}"


def build_lesson_summary(item: dict[str, Any]) -> str:
    title = compact_whitespace(item.get("descrizioneInsegnamento"))
    integrated = compact_whitespace(item.get("descrizioneCorsoIntegrato"))
    if integrated and integrated not in title:
        return f"{title} ({integrated})" if title else integrated
    return title or compact_whitespace(item.get("codiceInsegnamento")) or "Lesson"


def build_exam_summary(item: dict[str, Any]) -> str:
    title = compact_whitespace(item.get("descrizioneInsegnamento")) or compact_whitespace(item.get("codiceInsegnamento"))
    exam_type = compact_whitespace(item.get("descTipoAppello")) or "Exam"
    return f"{title} - {exam_type}"


def append_line(lines: list[str], label: str, value: Any) -> None:
    text = compact_whitespace(str(value)) if value is not None else ""
    if text:
        lines.append(f"{label}: {text}")


def bool_line(lines: list[str], label: str, value: bool | None) -> None:
    if value is None:
        return
    lines.append(f"{label}: {'yes' if value else 'no'}")


def fallback_uid(prefix: str, values: list[str]) -> str:
    digest = hashlib.sha1("|".join(values).encode("utf-8")).hexdigest()
    return f"{prefix}-{digest}@codex"


def lesson_uid(item: dict[str, Any], location: str) -> str:
    calendar_occurrence_id = item.get("idCalendarioAula")
    if calendar_occurrence_id:
        return f"unicatt-lesson-occurrence-{calendar_occurrence_id}@codex"
    lesson_id = item.get("idLezione")
    if lesson_id:
        return fallback_uid(
            "unicatt-lesson",
            [
                str(lesson_id),
                compact_whitespace(item.get("dataEvento")),
                compact_whitespace(item.get("oraInizio")),
                compact_whitespace(item.get("oraFine")),
            ],
        )
    return fallback_uid(
        "unicatt-lesson",
        [
            compact_whitespace(item.get("codiceInsegnamento")),
            compact_whitespace(item.get("dataEvento")),
            compact_whitespace(item.get("oraInizio")),
            compact_whitespace(item.get("oraFine")),
            location,
        ],
    )


def exam_uid(item: dict[str, Any], location: str) -> str:
    exam_occurrence_id = item.get("idCalendarioAula")
    if exam_occurrence_id:
        return fallback_uid(
            "unicatt-exam-occurrence",
            [
                str(exam_occurrence_id),
                compact_whitespace(item.get("dataEvento")),
                compact_whitespace(item.get("oraInizio")),
                compact_whitespace(item.get("oraFine")),
                location,
                compact_whitespace(item.get("descTipoAppello")),
            ],
        )
    exam_id = item.get("idEsame")
    if exam_id:
        return fallback_uid(
            "unicatt-exam",
            [
                str(exam_id),
                compact_whitespace(item.get("dataEvento")),
                compact_whitespace(item.get("oraInizio")),
                compact_whitespace(item.get("oraFine")),
                location,
                compact_whitespace(item.get("descTipoAppello")),
            ],
        )
    return fallback_uid(
        "unicatt-exam",
        [
            compact_whitespace(item.get("codiceInsegnamento")),
            compact_whitespace(item.get("dataEvento")),
            compact_whitespace(item.get("oraInizio")),
            compact_whitespace(item.get("oraFine")),
            location,
            compact_whitespace(item.get("descTipoAppello")),
        ],
    )


def lesson_match_key(item: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        compact_whitespace(item.get("codiceInsegnamento")),
        compact_whitespace(item.get("dataEvento")),
        compact_whitespace(item.get("oraInizio")),
        compact_whitespace(item.get("oraFine")),
    )


def normalize_lesson_record(item: dict[str, Any], source_kind: str) -> CalendarEvent:
    location = build_location(item.get("descrizioneEdificio") or item.get("edificio"), item.get("descrizioneAula") or item.get("aula"))
    start = parse_local_datetime(str(item["dataEvento"]), str(item["oraInizio"]))
    end = parse_local_datetime(str(item["dataEvento"]), str(item["oraFine"]))
    lines: list[str] = []
    append_line(lines, "Course code", item.get("codiceInsegnamento"))
    append_line(lines, "Integrated course code", item.get("codiceCorsoIntegrato"))
    append_line(lines, "Integrated course", item.get("descrizioneCorsoIntegrato"))
    append_line(lines, "Teacher", item.get("docente"))
    append_line(lines, "Source date", item.get("dataEvento"))
    append_line(lines, "Building", item.get("descrizioneEdificio") or item.get("edificio"))
    append_line(lines, "Room", item.get("descrizioneAula") or item.get("aula"))
    append_line(lines, "Duration", item.get("durata"))
    append_line(lines, "Reason", item.get("motivo"))
    append_line(lines, "Notes", item.get("note"))
    append_line(lines, "Source", LESSON_SOURCE_TAG)
    append_line(lines, "Source kind", source_kind)
    bool_line(lines, "Room change", bool(item.get("flagCambioAula")))
    bool_line(lines, "Extra lesson", bool(item.get("flagStraordinario")))
    bool_line(lines, "Suspended", bool(item.get("flagSospensione")))
    bool_line(lines, "Exercise", bool(item.get("flagEsercitazione")))
    uid = lesson_uid(item, location)
    source_record_id = str(item.get("idCalendarioAula") or item.get("idLezione") or uid)
    metadata = {
        "codiceInsegnamento": item.get("codiceInsegnamento"),
        "codiceCorsoIntegrato": item.get("codiceCorsoIntegrato"),
        "idLezione": item.get("idLezione"),
        "idCalendarioAula": item.get("idCalendarioAula"),
        "matchKey": list(lesson_match_key(item)),
        "flags": {
            "room_change": bool(item.get("flagCambioAula")),
            "extra_lesson": bool(item.get("flagStraordinario")),
            "suspension": bool(item.get("flagSospensione")),
        },
    }
    return CalendarEvent(
        uid=uid,
        start=start,
        end=end,
        summary=build_lesson_summary(item),
        location=location,
        description="\n".join(lines),
        status="CANCELLED" if bool(item.get("flagSospensione")) else None,
        source_record_id=source_record_id,
        source_kind=source_kind,
        source_date=str(item.get("dataEvento") or ""),
        metadata=metadata,
    )


def normalize_exam_record(item: dict[str, Any]) -> CalendarEvent:
    location = build_location(item.get("edificio"), item.get("aula"))
    start = parse_local_datetime(str(item["dataEvento"]), str(item["oraInizio"]))
    end = parse_local_datetime(str(item["dataEvento"]), str(item["oraFine"]))
    lines: list[str] = []
    append_line(lines, "Course code", item.get("codiceInsegnamento"))
    append_line(lines, "Integrated course code", item.get("codiceCorsoIntegrato"))
    append_line(lines, "Integrated course", item.get("descrizioneCorsoIntegrato"))
    append_line(lines, "Teacher", item.get("docente"))
    append_line(lines, "Exam type", item.get("descTipoAppello"))
    append_line(lines, "Source date", item.get("dataEvento"))
    append_line(lines, "Building", item.get("edificio"))
    append_line(lines, "Room", item.get("aula"))
    append_line(lines, "Duration", item.get("durata"))
    append_line(lines, "Turn", item.get("turno"))
    append_line(lines, "Turn index", item.get("progressivoTurni"))
    append_line(lines, "Reason", item.get("motivo"))
    append_line(lines, "Notes", item.get("note"))
    append_line(lines, "Source", EXAMS_ENDPOINT)
    uid = exam_uid(item, location)
    source_record_id = str(item.get("idCalendarioAula") or item.get("idEsame") or uid)
    metadata = {
        "codiceInsegnamento": item.get("codiceInsegnamento"),
        "codiceCorsoIntegrato": item.get("codiceCorsoIntegrato"),
        "idEsame": item.get("idEsame"),
        "examType": item.get("descTipoAppello"),
    }
    return CalendarEvent(
        uid=uid,
        start=start,
        end=end,
        summary=build_exam_summary(item),
        location=location,
        description="\n".join(lines),
        status=None,
        source_record_id=source_record_id,
        source_kind="exam",
        source_date=str(item.get("dataEvento") or ""),
        metadata=metadata,
    )


def flatten_grouped_records(payload: dict[str, Any], key: str) -> list[dict[str, Any]]:
    grouped = payload.get(key, {})
    if not isinstance(grouped, dict):
        raise ValueError(f"expected dict payload for {key}")
    rows: list[dict[str, Any]] = []
    for items in grouped.values():
        if isinstance(items, list):
            rows.extend(item for item in items if isinstance(item, dict))
    return rows


def merge_lesson_rows(
    base_rows: list[dict[str, Any]],
    room_changes: list[dict[str, Any]],
    extra_rows: list[dict[str, Any]],
    suspension_rows: list[dict[str, Any]],
) -> tuple[list[CalendarEvent], list[dict[str, Any]]]:
    merged = [dict(item) for item in base_rows]
    by_calendar_aula = {
        str(item.get("idCalendarioAula")): item
        for item in merged
        if item.get("idCalendarioAula") is not None
    }
    by_match_key: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for item in merged:
        by_match_key.setdefault(lesson_match_key(item), []).append(item)
    notes: list[dict[str, Any]] = []

    for change in room_changes:
        target = None
        change_id = change.get("idCalendarioAula")
        if change_id is not None:
            target = by_calendar_aula.get(str(change_id))
        if target is None:
            matches = by_match_key.get(lesson_match_key(change), [])
            if len(matches) == 1:
                target = matches[0]
            elif len(matches) > 1:
                notes.append({"type": "ambiguous_room_change", "record": change, "match_count": len(matches)})
                continue
        if target is None:
            notes.append({"type": "unmatched_room_change", "record": change})
            continue
        target["descrizioneAula"] = change.get("aula") or change.get("descrizioneAula") or target.get("descrizioneAula")
        target["descrizioneEdificio"] = change.get("edificio") or change.get("descrizioneEdificio") or target.get("descrizioneEdificio")
        target["flagCambioAula"] = True
        target["motivo"] = merge_reason(target.get("motivo"), change.get("motivo"))

    for extra in extra_rows:
        extra_key = lesson_match_key(extra)
        target = None
        if extra.get("idCalendarioAula") is not None:
            target = by_calendar_aula.get(str(extra["idCalendarioAula"]))
        if target is None:
            matches = by_match_key.get(extra_key, [])
            if len(matches) == 1:
                target = matches[0]
        if target is None:
            candidate = dict(extra)
            candidate["flagStraordinario"] = True
            merged.append(candidate)
            by_match_key.setdefault(lesson_match_key(candidate), []).append(candidate)
            if candidate.get("idCalendarioAula") is not None:
                by_calendar_aula[str(candidate["idCalendarioAula"])] = candidate
            continue
        target["flagStraordinario"] = True

    for suspension in suspension_rows:
        target = None
        if suspension.get("idCalendarioAula") is not None:
            target = by_calendar_aula.get(str(suspension["idCalendarioAula"]))
        if target is None:
            matches = by_match_key.get(lesson_match_key(suspension), [])
            if len(matches) == 1:
                target = matches[0]
            elif len(matches) > 1:
                notes.append({"type": "ambiguous_suspension", "record": suspension, "match_count": len(matches)})
                continue
        if target is None:
            notes.append({"type": "unmatched_suspension", "record": suspension})
            continue
        target["flagSospensione"] = True
        target["motivo"] = merge_reason(target.get("motivo"), suspension.get("motivo"))

    events = [
        normalize_lesson_record(item, "lesson")
        for item in sorted(merged, key=lambda row: (row.get("dataEvento", ""), row.get("oraInizio", ""), row.get("descrizioneInsegnamento", "")))
    ]
    return events, notes


def build_lessons_artifact(spec: FeedSpec, fetcher: Callable[[str, dict[str, str]], Any]) -> FeedArtifact:
    base_payload = fetcher(LESSONS_ENDPOINT, spec.payload)
    room_changes = fetcher(ROOM_CHANGES_ENDPOINT, {k: v for k, v in spec.payload.items() if k != "annoDiCorso"})
    extra_lessons = fetcher(EXTRA_LESSONS_ENDPOINT, {k: v for k, v in spec.payload.items() if k != "annoDiCorso"})
    suspensions = fetcher(SUSPENSIONS_ENDPOINT, {k: v for k, v in spec.payload.items() if k != "annoDiCorso"})
    base_rows = flatten_grouped_records(base_payload, "lezioniCalendario")
    events, merge_notes = merge_lesson_rows(base_rows, room_changes, extra_lessons, suspensions)
    generated = datetime.now(UTC)
    content = render_ics(spec.calendar_name, spec.description, events, timezone_id=spec.timezone_id)
    return build_artifact(
        spec=spec,
        events=events,
        content=content,
        generated=generated,
        source_counts={
            "base_lessons": len(base_rows),
            "room_changes": len(room_changes),
            "extra_lessons": len(extra_lessons),
            "suspensions": len(suspensions),
            "merge_notes": len(merge_notes),
        },
        source_urls=[LESSONS_ENDPOINT, ROOM_CHANGES_ENDPOINT, EXTRA_LESSONS_ENDPOINT, SUSPENSIONS_ENDPOINT],
        cache_mode="fresh",
    )


def build_exams_artifact(spec: FeedSpec, fetcher: Callable[[str, dict[str, str]], Any]) -> FeedArtifact:
    payload = fetcher(EXAMS_ENDPOINT, spec.payload)
    rows = flatten_grouped_records(payload, "appelliCalendario")
    events = [normalize_exam_record(item) for item in sorted(rows, key=lambda row: (row.get("dataEvento", ""), row.get("oraInizio", ""), row.get("descrizioneInsegnamento", "")))]
    generated = datetime.now(UTC)
    content = render_ics(spec.calendar_name, spec.description, events, timezone_id=spec.timezone_id)
    return build_artifact(
        spec=spec,
        events=events,
        content=content,
        generated=generated,
        source_counts={"base_exams": len(rows)},
        source_urls=[EXAMS_ENDPOINT],
        cache_mode="fresh",
    )


def build_artifact(
    spec: FeedSpec,
    events: list[CalendarEvent],
    content: str,
    generated: datetime,
    source_counts: dict[str, int],
    source_urls: list[str],
    cache_mode: str,
) -> FeedArtifact:
    content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
    return FeedArtifact(
        feed_id=spec.feed_id,
        calendar_name=spec.calendar_name,
        generated_at_utc=generated.isoformat(),
        generated_for_local_date=generated.astimezone(ZoneInfo(spec.timezone_id)).date().isoformat(),
        event_count=len(events),
        cancelled_count=sum(1 for event in events if event.status == "CANCELLED"),
        source_counts=source_counts,
        source_payload=dict(spec.payload),
        source_urls=source_urls,
        content_hash=content_hash,
        cache_mode=cache_mode,
        events=[event_to_status_row(event) for event in events],
        ics_text=content,
    )


def build_feed_artifact(spec: FeedSpec, fetcher: Callable[[str, dict[str, str]], Any] = fetch_json) -> FeedArtifact:
    if spec.kind == "holidays":
        return build_holidays_artifact(spec)
    if spec.kind == "lessons":
        return build_lessons_artifact(spec, fetcher)
    if spec.kind == "exams":
        return build_exams_artifact(spec, fetcher)
    if spec.kind == "tartu-lessons":
        return build_tartu_lessons_artifact(spec)
    raise ValueError(f"unsupported feed kind: {spec.kind}")


def build_tartu_lessons_artifact(spec: FeedSpec) -> FeedArtifact:
    try:
        from tartu_y1_station_source import CALENDAR_NAME, SOURCE_URLS, source_events
    except ModuleNotFoundError:
        from medical_curriculum_tooling.tartu_y1_station_source import CALENDAR_NAME, SOURCE_URLS, source_events
    events: list[CalendarEvent] = []
    for row in source_events():
        start = datetime.fromisoformat(str(row["start"]))
        end_text = row.get("end")
        end = datetime.fromisoformat(str(end_text)) if end_text else start
        events.append(CalendarEvent(
            uid=str(row["uid"]),
            start=start,
            end=end,
            summary=str(row["summary"]),
            location=str(row.get("location") or ""),
            description=str(row.get("description") or ""),
            status=row.get("status"),
            source_record_id=str(row.get("source_record_id") or row["uid"]),
            source_kind=str(row.get("source_kind") or "official_ois_exact_date"),
            source_date=str(row.get("source_date") or ""),
            metadata=dict(row.get("metadata") or {}),
        ))
    generated = datetime.now(UTC)
    content = render_ics(CALENDAR_NAME, spec.description, events, timezone_id=spec.timezone_id)
    return build_artifact(
        spec=spec,
        events=events,
        content=content,
        generated=generated,
        source_counts={"dated_source_events": len(events)},
        source_urls=SOURCE_URLS,
        cache_mode="fresh",
    )


def build_holidays_artifact(spec: FeedSpec) -> FeedArtifact:
    from datetime import date
    events = []
    sources = ["https://presidenza.governo.it/ufficio_cerimoniale/cerimoniale/giornate.html", "https://www.normattiva.it/eli/id/2025/10/10/25G00153/ORIGINAL"]
    for year in range(2024, max(2030, datetime.now(ROME_TZ).year + 2) + 1):
        a, b, c = year % 19, year // 100, year % 100
        d, e = b // 4, b % 4
        f = (b + 8) // 25
        g = (b - f + 1) // 3
        h = (19*a + b-d-g+15) % 30
        i, k = c // 4, c % 4
        l = (32+2*e+2*i-h-k) % 7
        m = (a+11*h+22*l) // 451
        easter = date(year, (h+l-7*m+114)//31, (h+l-7*m+114)%31+1)
        days = [(date(year, mo, day), title) for mo, day, title in [(1,1,"New Year's Day / Capodanno"),(1,6,"Epiphany / Epifania"),(4,25,"Liberation Day / Festa della Liberazione"),(5,1,"Labour Day / Festa del Lavoro"),(6,2,"Republic Day / Festa della Repubblica"),(8,15,"Assumption / Ferragosto"),(11,1,"All Saints / Ognissanti"),(12,8,"Immaculate Conception / Immacolata Concezione"),(12,25,"Christmas / Natale"),(12,26,"St Stephen / Santo Stefano")]]
        days += [(easter, "Easter Sunday / Pasqua"), (easter+timedelta(days=1), "Easter Monday / Pasquetta")]
        if year >= 2026:
            days.append((date(year,10,4), "St Francis of Assisi / San Francesco d'Assisi"))
        for day, title in days:
            start = datetime.combine(day, datetime.min.time(), ROME_TZ)
            uid = f"italy-holiday-{day.isoformat()}@ucsc-calendar"
            events.append(CalendarEvent(uid,start,start+timedelta(days=1),title,"Italy", "National public holiday. Sources: " + " ; ".join(sources),None,uid,"holiday",day.isoformat(),{"all_day":True}))
    events.sort(key=lambda event:event.start)
    return build_artifact(spec,events,render_ics(spec.calendar_name,spec.description,events,timezone_id=spec.timezone_id),datetime.now(UTC),{"national_holidays":len(events)},sources,"fresh")


def event_to_status_row(event: CalendarEvent) -> dict[str, Any]:
    return {
        "uid": event.uid,
        "summary": event.summary,
        "start": event.start.isoformat(),
        "end": event.end.isoformat(),
        "location": event.location,
        "description": event.description,
        "status": event.status,
        "source_kind": event.source_kind,
        "source_record_id": event.source_record_id,
        "source_date": event.source_date,
        "metadata": event.metadata,
        "sequence": event.sequence,
        "last_modified_utc": event.last_modified_utc,
    }


def event_from_status_row(row: dict[str, Any]) -> CalendarEvent:
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    description = row.get("description")
    if not description:
        description_lines = ["Retained from previous UCSC calendar feed."]
        append_line(description_lines, "Source kind", row.get("source_kind"))
        append_line(description_lines, "Source record id", row.get("source_record_id"))
        append_line(description_lines, "Source date", row.get("source_date"))
        description = "\n".join(description_lines)
    return CalendarEvent(
        uid=str(row["uid"]),
        start=datetime.fromisoformat(str(row["start"])),
        end=datetime.fromisoformat(str(row["end"])),
        summary=compact_whitespace(str(row.get("summary") or "UCSC event")),
        location=compact_whitespace(str(row.get("location") or "")),
        description=str(description),
        status=row.get("status"),
        source_record_id=str(row.get("source_record_id") or row["uid"]),
        source_kind=str(row.get("source_kind") or "unknown"),
        source_date=str(row.get("source_date") or ""),
        metadata=metadata,
        sequence=int(row.get("sequence") or 0),
        last_modified_utc=row.get("last_modified_utc"),
    )


def event_signature(row: dict[str, Any]) -> str:
    payload = {
        "uid": row.get("uid"),
        "summary": row.get("summary"),
        "start": row.get("start"),
        "end": row.get("end"),
        "location": row.get("location"),
        "description": row.get("description"),
        "status": row.get("status"),
        "source_kind": row.get("source_kind"),
        "source_record_id": row.get("source_record_id"),
        "source_date": row.get("source_date"),
        "metadata": row.get("metadata"),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()


def append_persistence_note(description: str, note: str) -> str:
    line = f"Calendar persistence note: {note}"
    if line in description:
        return description
    return f"{description}\n\n{line}" if description else line


def ledger_cache_key(feed_id: str) -> str:
    return f"feed:{feed_id}:event-ledger:v1"


def initial_ledger_from_seed(feed_id: str) -> dict[str, dict[str, Any]]:
    seed_artifact = load_seed_artifact(feed_id)
    if seed_artifact is None:
        return {}
    observed_at = seed_artifact.generated_at_utc
    ledger: dict[str, dict[str, Any]] = {}
    for row in seed_artifact.events:
        if not isinstance(row, dict) or not row.get("uid") or not row.get("start") or not row.get("end"):
            continue
        event = event_from_status_row(row)
        event.last_modified_utc = event.last_modified_utc or observed_at
        event.sequence = int(event.sequence or 0)
        event_row = event_to_status_row(event)
        ledger[event.uid] = {
            "uid": event.uid,
            "event": event_row,
            "event_signature": event_signature(event_row),
            "first_seen_at_utc": observed_at,
            "last_seen_at_utc": observed_at,
            "last_missing_at_utc": None,
            "seen_count": 1,
            "missing_count": 0,
            "last_snapshot_present": True,
            "retention_state": "seed_bootstrap",
        }
    return ledger


class DurableHistoryError(RuntimeError):
    """Fail closed rather than publish a smaller calendar on archive failure."""


def blob_history_client():
    if not os.environ.get("BLOB_READ_WRITE_TOKEN"):
        if os.environ.get("VERCEL"):
            raise DurableHistoryError("Durable history credential is required in hosted runtime")
        return None
    from vercel.blob import BlobClient
    return BlobClient()


def blob_get_or_none(client: Any, pathname: str) -> Any | None:
    """Return None only for a missing object; preserve provider failures."""
    try:
        return client.get(pathname, access="private")
    except Exception as exc:
        if exc.__class__.__name__ == "BlobNotFoundError":
            return None
        raise


def load_blob_history(feed_id: str, ledger: dict) -> dict:
    client = blob_history_client()
    if client is None:
        return ledger
    try:
        # Read the compact per-feed ledger directly. This avoids listing the
        # immutable archive on every calendar request. The fallback list is a
        # one-time migration path for stores created before the direct ledger
        # index existed.
        ledger_pathname = f"{HISTORY_LEDGER_PREFIX}/{feed_id}.json"
        direct = blob_get_or_none(client, ledger_pathname)
        if direct is not None:
            payload = json.loads(direct.content)
            for record in payload.get("events", []):
                if isinstance(record, dict) and record.get("uid") and isinstance(record.get("event"), dict):
                    uid = str(record["uid"])
                    previous = ledger.get(uid)
                    if previous is None or record["event"].get("last_modified_utc", "") >= previous["event"].get("last_modified_utc", ""):
                        ledger[uid] = record
            return ledger

        # Legacy migration: enumerate the existing immutable snapshots only
        # when the direct ledger has not yet been published.
        cursor = None
        while True:
            page = client.list_objects(prefix=f"{HISTORY_SNAPSHOT_PREFIX}/{feed_id}/", limit=1000, cursor=cursor)
            for blob in page.blobs:
                cached = cache_get("immutable:" + blob.pathname, None)
                if cached is None:
                    result = client.get(blob.url, access="private", use_cache=False)
                    cached = json.loads(result.content)
                    cache_set("immutable:" + blob.pathname, cached, None)
            for record in cached["events"]:
                uid = record["uid"]
                previous = ledger.get(uid)
                previous_event = previous.get("event") if isinstance(previous, dict) else None
                record_event = record.get("event") if isinstance(record, dict) else None
                if not isinstance(record_event, dict):
                    continue
                if previous is None or not isinstance(previous_event, dict) or record_event.get("last_modified_utc", "") >= previous_event.get("last_modified_utc", ""):
                    ledger[uid] = record
            if not page.has_more:
                break
            cursor = page.cursor
        return ledger
    except Exception as exc:
        raise DurableHistoryError("Durable history could not be fully read; refusing a potentially incomplete calendar") from exc
    finally:
        client.close()


def save_blob_history(feed_id: str, payload: dict) -> None:
    client = blob_history_client()
    if client is None:
        return
    digest = hashlib.sha256(json.dumps([event_signature(r["event"]) for r in payload["events"]]).encode()).hexdigest()
    pathname = f"{HISTORY_SNAPSHOT_PREFIX}/{feed_id}/{digest}.json"
    try:
        if blob_get_or_none(client, pathname) is None:
            try:
                client.put(pathname, json_bytes(payload), access="private", content_type="application/json", overwrite=False)
            except Exception:
                # Identical concurrent snapshots may race to create the same immutable path.
                if blob_get_or_none(client, pathname) is None:
                    raise
    except Exception as exc:
        raise DurableHistoryError("New calendar state was not durably archived; refusing publication") from exc
    finally:
        client.close()


def save_blob_ledger(feed_id: str, payload: dict) -> None:
    """Publish a direct read index without replacing immutable snapshots."""
    client = blob_history_client()
    if client is None:
        return
    digest = hashlib.sha256(json.dumps([event_signature(r["event"]) for r in payload["events"]]).encode()).hexdigest()
    pathname = f"{HISTORY_LEDGER_PREFIX}/{feed_id}.json"
    indexed_payload = dict(payload)
    indexed_payload["ledger_digest"] = digest
    try:
        current = blob_get_or_none(client, pathname)
        if current is not None:
            try:
                if json.loads(current.content).get("ledger_digest") == digest:
                    return
            except (TypeError, ValueError, json.JSONDecodeError):
                pass
        client.put(pathname, json_bytes(indexed_payload), access="private", content_type="application/json", overwrite=True)
    except Exception as exc:
        raise DurableHistoryError("Direct durable ledger index could not be published; refusing publication") from exc
    finally:
        client.close()


def load_event_ledger(feed_id: str, runtime_cache: Any | None) -> dict[str, dict[str, Any]]:
    payload = cache_get(ledger_cache_key(feed_id), runtime_cache)
    if isinstance(payload, dict) and payload.get("version") == 1 and isinstance(payload.get("events"), list):
        ledger = initial_ledger_from_seed(feed_id)
        for record in payload["events"]:
            if isinstance(record, dict) and record.get("uid") and isinstance(record.get("event"), dict):
                ledger[str(record["uid"])] = record
        return load_blob_history(feed_id, ledger)
    return load_blob_history(feed_id, initial_ledger_from_seed(feed_id))


def save_event_ledger(
    feed_id: str,
    ledger: dict[str, dict[str, Any]],
    generated_at_utc: str,
    runtime_cache: Any | None,
) -> None:
    payload = {
        "version": 1,
        "feed_id": feed_id,
        "updated_at_utc": generated_at_utc,
        "event_count": len(ledger),
        "events": [
            ledger[uid]
            for uid in sorted(
                ledger,
                key=lambda value: (
                    ledger[value]["event"].get("start", ""),
                    ledger[value]["event"].get("summary", ""),
                    value,
                ),
            )
        ],
    }
    save_blob_history(feed_id, payload)
    save_blob_ledger(feed_id, payload)
    cache_set(ledger_cache_key(feed_id), payload, runtime_cache, ttl_seconds=LEDGER_TTL_SECONDS)


def update_ledger_record(
    event: CalendarEvent,
    previous: dict[str, Any] | None,
    observed_at_utc: str,
    present: bool,
    retention_state: str,
) -> dict[str, Any]:
    event_row = event_to_status_row(event)
    signature = event_signature(event_row)
    if previous is None:
        sequence = int(event.sequence or 0)
        last_modified = event.last_modified_utc or observed_at_utc
        first_seen = observed_at_utc
        seen_count = 1 if present else 0
        missing_count = 0 if present else 1
    else:
        previous_event = previous.get("event") if isinstance(previous.get("event"), dict) else {}
        previous_signature = previous.get("event_signature") or event_signature(previous_event)
        sequence = int(previous_event.get("sequence") or previous.get("sequence") or 0)
        last_modified = previous_event.get("last_modified_utc") or previous.get("last_modified_utc") or observed_at_utc
        if signature != previous_signature:
            sequence += 1
            last_modified = observed_at_utc
        first_seen = previous.get("first_seen_at_utc") or observed_at_utc
        seen_count = int(previous.get("seen_count") or 0) + (1 if present else 0)
        missing_count = 0 if present else int(previous.get("missing_count") or 0) + 1
    event_row["sequence"] = sequence
    event_row["last_modified_utc"] = last_modified
    return {
        "uid": event.uid,
        "event": event_row,
        "event_signature": event_signature(event_row),
        "first_seen_at_utc": first_seen,
        "last_seen_at_utc": observed_at_utc if present else (previous or {}).get("last_seen_at_utc"),
        "last_missing_at_utc": None if present else observed_at_utc,
        "seen_count": seen_count,
        "missing_count": missing_count,
        "last_snapshot_present": present,
        "retention_state": retention_state,
    }


def merge_with_persistent_ledger(
    spec: FeedSpec,
    fresh_artifact: FeedArtifact,
    runtime_cache: Any | None,
) -> FeedArtifact:
    observed_dt = artifact_generated_at(fresh_artifact)
    observed_at = observed_dt.isoformat()
    fresh_events = [event_from_status_row(row) for row in fresh_artifact.events]
    fresh_by_uid = {event.uid: event for event in fresh_events}
    prior_ledger = load_event_ledger(spec.feed_id, runtime_cache)
    ledger = dict(prior_ledger)
    stats = {
        "fresh_events": len(fresh_events),
        "ledger_bootstrap_events": max(0, len(prior_ledger) - len(fresh_by_uid)) if prior_ledger else 0,
        "retained_past_missing": 0,
        "retained_future_missing_tentative": 0,
        "retained_future_missing_cancelled": 0,
    }

    for uid, event in fresh_by_uid.items():
        ledger[uid] = update_ledger_record(
            event=event,
            previous=prior_ledger.get(uid),
            observed_at_utc=observed_at,
            present=True,
            retention_state="present_in_latest_snapshot",
        )

    for uid, previous in prior_ledger.items():
        if uid in fresh_by_uid:
            continue
        previous_event = previous.get("event")
        if not isinstance(previous_event, dict):
            continue
        retained = event_from_status_row(previous_event)
        if retained.end.astimezone(UTC) < observed_dt:
            retained.description = append_persistence_note(
                retained.description,
                "retained because the event is in the past and no longer appears in the upstream feed.",
            )
            retention_state = "retained_past_missing_upstream"
            stats["retained_past_missing"] += 1
        else:
            next_missing_count = int(previous.get("missing_count") or 0) + 1
            if next_missing_count >= 2:
                retained.status = "TENTATIVE"
                retention_state = "retained_future_missing_tentative"
                stats["retained_future_missing_tentative"] += 1
                note = "future event disappeared from repeated upstream snapshots; retained as tentative. Disappearance alone does not establish cancellation."
            else:
                retained.status = "TENTATIVE"
                retention_state = "retained_future_missing_tentative"
                stats["retained_future_missing_tentative"] += 1
                note = "future event disappeared from the latest upstream feed; retained as tentative pending the next successful snapshot."
            retained.description = append_persistence_note(retained.description, note)
        ledger[uid] = update_ledger_record(
            event=retained,
            previous=previous,
            observed_at_utc=observed_at,
            present=False,
            retention_state=retention_state,
        )

    save_event_ledger(spec.feed_id, ledger, observed_at, runtime_cache)
    merged_events = [
        event_from_status_row(record["event"])
        for record in ledger.values()
        if isinstance(record.get("event"), dict)
    ]
    merged_events.sort(key=lambda event: (event.start, event.summary, event.uid))
    content = render_ics(spec.calendar_name, spec.description, merged_events, timezone_id=spec.timezone_id)
    source_counts = dict(fresh_artifact.source_counts)
    source_counts.update(stats)
    source_counts["persistent_ledger_events"] = len(merged_events)
    return build_artifact(
        spec=spec,
        events=merged_events,
        content=content,
        generated=observed_dt,
        source_counts=source_counts,
        source_urls=fresh_artifact.source_urls,
        cache_mode=fresh_artifact.cache_mode,
    )


def ics_escape(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace(";", "\\;")
        .replace(",", "\\,")
        .replace("\n", "\\n")
    )


def fold_ics_line(value: str) -> str:
    limit = 73
    if len(value) <= limit:
        return value
    parts = [value[:limit]]
    remainder = value[limit:]
    while remainder:
        parts.append(f" {remainder[:limit - 1]}")
        remainder = remainder[limit - 1 :]
    return "\r\n".join(parts)


def ics_line(name: str, value: str) -> str:
    return fold_ics_line(f"{name}:{ics_escape(value)}")


def ics_dt_local(dt: datetime, timezone_id: str = "Europe/Rome") -> str:
    return dt.astimezone(ZoneInfo(timezone_id)).strftime("%Y%m%dT%H%M%S")


def render_ics(calendar_name: str, description: str, events: list[CalendarEvent], timezone_id: str = "Europe/Rome") -> str:
    generated_dt = datetime.now(UTC)
    generated = ics_dt_utc(generated_dt)
    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Codex Automation//UCSC Unicatt Calendar Broadcast//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        ics_line("X-WR-CALNAME", calendar_name),
        ics_line("X-WR-CALDESC", description + " " + RETENTION_POLICY),
        f"X-WR-TIMEZONE:{timezone_id}",
        "BEGIN:VTIMEZONE",
        f"TZID:{timezone_id}",
        f"X-LIC-LOCATION:{timezone_id}",
        "BEGIN:DAYLIGHT",
        "TZOFFSETFROM:+0200" if timezone_id == "Europe/Tallinn" else "TZOFFSETFROM:+0100",
        "TZOFFSETTO:+0300" if timezone_id == "Europe/Tallinn" else "TZOFFSETTO:+0200",
        "TZNAME:EEST" if timezone_id == "Europe/Tallinn" else "TZNAME:CEST",
        "DTSTART:19700329T030000" if timezone_id == "Europe/Tallinn" else "DTSTART:19700329T020000",
        "RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU",
        "END:DAYLIGHT",
        "BEGIN:STANDARD",
        "TZOFFSETFROM:+0300" if timezone_id == "Europe/Tallinn" else "TZOFFSETFROM:+0200",
        "TZOFFSETTO:+0200" if timezone_id == "Europe/Tallinn" else "TZOFFSETTO:+0100",
        "TZNAME:EET" if timezone_id == "Europe/Tallinn" else "TZNAME:CET",
        "DTSTART:19701025T040000" if timezone_id == "Europe/Tallinn" else "DTSTART:19701025T030000",
        "RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU",
        "END:STANDARD",
        "END:VTIMEZONE",
    ]
    for event in events:
        lines.extend(
            [
                "BEGIN:VEVENT",
                ics_line("UID", event.uid),
                f"DTSTAMP:{generated}",
                f"LAST-MODIFIED:{ics_utc_from_iso(event.last_modified_utc, generated_dt)}",
                f"SEQUENCE:{max(0, int(event.sequence or 0))}",
                f"DTSTART;VALUE=DATE:{event.start:%Y%m%d}" if event.metadata.get("all_day") else f"DTSTART;TZID={timezone_id}:{ics_dt_local(event.start, timezone_id)}",
                *([] if event.metadata.get("end_unknown") else [f"DTEND;VALUE=DATE:{event.end:%Y%m%d}" if event.metadata.get("all_day") else f"DTEND;TZID={timezone_id}:{ics_dt_local(event.end, timezone_id)}"]),
                ics_line("SUMMARY", event.summary),
                ics_line("LOCATION", event.location),
                ics_line("DESCRIPTION", event.description + "\n\n" + RETENTION_POLICY),
                ics_line("STATUS", event.status or "CONFIRMED"),
                "TRANSP:OPAQUE",
                "END:VEVENT",
            ]
        )
    lines.append("END:VCALENDAR")
    return "\r\n".join(lines) + "\r\n"


def ics_dt_utc(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def ics_utc_from_iso(value: str | None, fallback: datetime) -> str:
    if value:
        try:
            return ics_dt_utc(datetime.fromisoformat(value))
        except ValueError:
            pass
    return ics_dt_utc(fallback)


def artifact_to_payload(artifact: FeedArtifact) -> dict[str, Any]:
    payload = asdict(artifact)
    return payload


def payload_to_artifact(payload: dict[str, Any]) -> FeedArtifact:
    allowed = {item.name for item in fields(FeedArtifact)}
    return FeedArtifact(**{key: value for key, value in payload.items() if key in allowed})


def seed_status_path(feed_id: str) -> Path:
    return ensure_seed_dir() / f"{feed_id}-status.json"


def seed_ics_path(feed_id: str) -> Path:
    return ensure_seed_dir() / f"{feed_id}.ics"


def load_seed_artifact(feed_id: str) -> FeedArtifact | None:
    status_path = seed_status_path(feed_id)
    ics_path = seed_ics_path(feed_id)
    if not status_path.exists() or not ics_path.exists():
        return None
    payload = json.loads(status_path.read_text(encoding="utf-8"))
    payload["ics_text"] = ics_path.read_text(encoding="utf-8")
    artifact = payload_to_artifact(payload)
    artifact.cache_mode = "seed-fallback"
    return artifact


def artifact_generated_at(artifact: FeedArtifact) -> datetime:
    return datetime.fromisoformat(artifact.generated_at_utc)


def artifact_age_seconds(artifact: FeedArtifact, now_utc: datetime | None = None) -> int:
    reference = now_utc or datetime.now(UTC)
    age = reference - artifact_generated_at(artifact)
    return max(0, int(age.total_seconds()))


def refresh_feed(feed_id: str, runtime_cache: Any | None, force: bool = False) -> FeedArtifact:
    spec = FEED_SPECS[feed_id]
    cache_key = f"feed:{feed_id}:latest"
    cached = cache_get(cache_key, runtime_cache)
    cached_artifact = payload_to_artifact(cached) if cached is not None else None
    seed_artifact = load_seed_artifact(feed_id)
    try:
        artifact = build_feed_artifact(spec)
        artifact = merge_with_persistent_ledger(spec, artifact, runtime_cache)
        cache_set(cache_key, artifact_to_payload(artifact), runtime_cache)
        return artifact
    except (HTTPError, URLError, TimeoutError, ValueError):
        fallback = cached_artifact or seed_artifact
        if fallback is None:
            raise
        ledger = load_event_ledger(feed_id, runtime_cache)
        rows = {e["uid"]: e for e in fallback.events}
        rows.update({uid: record["event"] for uid, record in ledger.items()})
        events = sorted((event_from_status_row(row) for row in rows.values()), key=lambda e:(e.start,e.uid))
        return build_artifact(spec, events, render_ics(spec.calendar_name,spec.description,events,timezone_id=spec.timezone_id), artifact_generated_at(fallback), fallback.source_counts, fallback.source_urls, "stale-cache")


def latest_feed(feed_id: str, runtime_cache: Any | None) -> FeedArtifact:
    return refresh_feed(feed_id, runtime_cache=runtime_cache, force=False)


def json_bytes(payload: dict[str, Any]) -> bytes:
    return (json.dumps(payload, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def response(status: int, content_type: str, body: bytes, extra_headers: dict[str, str] | None = None) -> HttpResponse:
    headers = {
        "Content-Type": content_type,
        "Content-Length": str(len(body)),
        "Cache-Control": "public, max-age=300",
    }
    if extra_headers:
        headers.update(extra_headers)
    return HttpResponse(status=status, headers=headers, body=body)


def status_payload(artifact: FeedArtifact) -> dict[str, Any]:
    payload = artifact_to_payload(artifact)
    payload.pop("ics_text", None)
    age_seconds = artifact_age_seconds(artifact)
    payload["generated_age_seconds"] = age_seconds
    payload["generated_age_hours"] = round(age_seconds / 3600, 2)
    payload["refresh_local_hours"] = list(REFRESH_LOCAL_HOURS)
    payload["stale"] = artifact.cache_mode != "fresh"
    payload["seed_fallback"] = artifact.cache_mode == "seed-fallback"
    payload["healthy"] = age_seconds <= HEALTHY_MAX_AGE_SECONDS
    payload["retention_policy"] = RETENTION_POLICY
    payload["coverage_complete"] = False if artifact.feed_id in {"year1-lessons", "year2-lessons", "tartu-y1-lessons"} else None
    return payload


def maybe_refresh_all(runtime_cache: Any | None, force: bool = False) -> dict[str, Any]:
    now_local = datetime.now(ROME_TZ)
    marker_key = "refresh-marker"
    marker = cache_get(marker_key, runtime_cache)
    current_date = now_local.date().isoformat()
    if now_local.hour not in REFRESH_LOCAL_HOURS and not force:
        return {
            "ok": True,
            "refreshed": False,
            "reason": "outside_refresh_window",
            "local_time": now_local.isoformat(),
            "refresh_local_hours": list(REFRESH_LOCAL_HOURS),
        }
    if isinstance(marker, dict) and marker.get("local_date") == current_date and marker.get("fresh_completed") and not force:
        return {
            "ok": True,
            "refreshed": False,
            "reason": "already_refreshed_today",
            "local_time": now_local.isoformat(),
            "local_date": current_date,
        }
    attempt_hours = set()
    if isinstance(marker, dict) and marker.get("local_date") == current_date:
        attempt_hours = {int(value) for value in marker.get("attempt_hours", [])}
    if now_local.hour in attempt_hours and not force:
        return {
            "ok": True,
            "refreshed": False,
            "reason": "already_attempted_this_window",
            "local_time": now_local.isoformat(),
            "local_date": current_date,
            "refresh_local_hours": list(REFRESH_LOCAL_HOURS),
        }
    results = {}
    all_fresh = True
    for feed_id in FEED_SPECS:
        artifact = refresh_feed(feed_id, runtime_cache=runtime_cache, force=True)
        all_fresh = all_fresh and artifact.cache_mode == "fresh"
        results[feed_id] = {
            "generated_at_utc": artifact.generated_at_utc,
            "event_count": artifact.event_count,
            "content_hash": artifact.content_hash,
            "cache_mode": artifact.cache_mode,
        }
    marker_payload = {
        "local_date": current_date,
        "local_time": now_local.isoformat(),
        "attempt_hours": sorted(attempt_hours | {now_local.hour}),
        "fresh_completed": all_fresh,
    }
    cache_set(marker_key, marker_payload, runtime_cache, ttl_seconds=172800)
    return {
        "ok": True,
        "refreshed": all_fresh,
        "local_time": now_local.isoformat(),
        "local_date": current_date,
        "refresh_local_hours": list(REFRESH_LOCAL_HOURS),
        "all_fresh": all_fresh,
        "results": results,
    }


def handle_route(path: str, query_string: str = "", runtime_cache: Any | None = None) -> HttpResponse:
    query = parse_qs(query_string, keep_blank_values=True)
    if path == "/api/refresh":
        try:
            payload = maybe_refresh_all(runtime_cache, force=query.get("force") == ["1"])
            return response(200, "application/json; charset=utf-8", json_bytes(payload))
        except Exception as exc:
            # A refresh provider outage is an operational failure, not a
            # generic platform crash. Keep the old static feed untouched and
            # return a machine-readable signal for the watchdog/backstop.
            return response(
                503,
                "application/json; charset=utf-8",
                json_bytes(
                    {
                        "ok": False,
                        "refreshed": False,
                        "failure_visible": True,
                        "error": type(exc).__name__,
                        "message": str(exc),
                    }
                ),
            )
    if path in ESN_ROME_SOCIAL_ROUTE_PATHS:
        if handle_esn_rome_social_route is None:
            body = json_bytes(
                {
                    "ok": False,
                    "path": path,
                    "message": "ESN Rome social calendar module could not be imported",
                    "error": type(ESN_ROME_SOCIAL_IMPORT_ERROR).__name__ if ESN_ROME_SOCIAL_IMPORT_ERROR else "UnknownError",
                }
            )
            return response(502, "application/json; charset=utf-8", body)
        social_response = handle_esn_rome_social_route(path, query_string, runtime_cache=runtime_cache)
        return HttpResponse(
            status=social_response.status,
            headers=dict(social_response.headers),
            body=social_response.body,
        )

    feed_id = ""
    output = ""
    if path == "/api/index":
        feed_id = query.get("feed", [""])[0]
        output = query.get("format", [""])[0]
    else:
        for candidate in FEED_SPECS.values():
            if path == candidate.route_path:
                feed_id = candidate.feed_id
                output = "ics"
                break
            if path == candidate.route_path.replace(".ics", "-status.json"):
                feed_id = candidate.feed_id
                output = "status"
                break
    if feed_id not in FEED_SPECS or output not in {"ics", "status"}:
        body = json_bytes(
            {
                "ok": False,
                "message": "unknown route",
                "path": path,
            }
        )
        return response(404, "application/json; charset=utf-8", body)

    try:
        artifact = latest_feed(feed_id, runtime_cache=runtime_cache)
    except Exception as exc:
        body = json_bytes(
            {
                "ok": False,
                "feed_id": feed_id,
                "error": type(exc).__name__,
                "message": str(exc),
            }
        )
        return response(502, "application/json; charset=utf-8", body)

    etag = f"\"{artifact.content_hash}\""
    if output == "ics":
        return response(
            200,
            "text/calendar; charset=utf-8",
            artifact.ics_text.encode("utf-8"),
            extra_headers={"ETag": etag},
        )
    return response(
        200,
        "application/json; charset=utf-8",
        json_bytes(status_payload(artifact)),
        extra_headers={"ETag": etag},
    )


def write_local_outputs(output_dir: Path, runtime_cache: Any | None = None) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for spec in FEED_SPECS.values():
        artifact = latest_feed(spec.feed_id, runtime_cache=runtime_cache)
        ics_path = output_dir / f"{spec.feed_id}.ics"
        status_path = output_dir / f"{spec.feed_id}-status.json"
        ics_path.write_text(artifact.ics_text, encoding="utf-8")
        status_path.write_text(json.dumps(status_payload(artifact), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        written.extend([ics_path, status_path])
    return written


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build UCSC Rome subscription calendar feeds from the public didatticaweb2 JSON APIs.")
    parser.add_argument("--feed", choices=["all", *sorted(FEED_SPECS.keys())], default="all")
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--print-status", action="store_true")
    parser.add_argument("--force-refresh", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runtime_cache = load_runtime_cache()
    if args.output_dir:
        output_dir = Path(args.output_dir).expanduser().resolve()
        if args.feed == "all":
            written = write_local_outputs(output_dir, runtime_cache=runtime_cache)
            if args.print_status:
                print(json.dumps({"written": [str(path) for path in written]}, indent=2))
            return 0
        artifact = refresh_feed(args.feed, runtime_cache=runtime_cache, force=args.force_refresh)
        ics_path = output_dir / f"{args.feed}.ics"
        status_path = output_dir / f"{args.feed}-status.json"
        output_dir.mkdir(parents=True, exist_ok=True)
        ics_path.write_text(artifact.ics_text, encoding="utf-8")
        status_path.write_text(json.dumps(status_payload(artifact), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        if args.print_status:
            print(json.dumps(status_payload(artifact), indent=2, ensure_ascii=False))
        return 0

    if args.feed == "all":
        payload = maybe_refresh_all(runtime_cache, force=args.force_refresh)
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0

    artifact = refresh_feed(args.feed, runtime_cache=runtime_cache, force=args.force_refresh)
    if args.print_status:
        print(json.dumps(status_payload(artifact), indent=2, ensure_ascii=False))
    else:
        print(artifact.ics_text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
