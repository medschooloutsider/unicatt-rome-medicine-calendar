"""Heavy failure-mode smoke tests for the provider-free calendar pipeline."""

from __future__ import annotations

import copy
import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from medical_curriculum_tooling import unicatt_calendar_broadcast as source
from medical_curriculum_tooling.calendar_reliability import pipeline


NOW = datetime(2026, 10, 26, 12, 0, tzinfo=UTC)


def lesson(uid: str, day: str, *, suspended: bool = False) -> dict:
    return {
        "idCalendarioAula": uid,
        "idLezione": f"lesson-{uid}",
        "codiceInsegnamento": f"MR{uid}",
        "codiceCorsoIntegrato": "MR000016",
        "descrizioneInsegnamento": f"Test lesson {uid}",
        "descrizioneCorsoIntegrato": "TEST COURSE",
        "docente": "TEST TEACHER",
        "dataEvento": day,
        "oraInizio": "08:30",
        "oraFine": "10:00",
        "descrizioneEdificio": "Giovanni XXIII",
        "descrizioneAula": "Lazzati",
        "durata": "01:30",
        "flagSospensione": suspended,
    }


def fetcher_for(rows: list[dict] | Exception):
    def fetch(url: str, payload: dict[str, str]):
        if isinstance(rows, Exception):
            raise rows
        if url == source.LESSONS_ENDPOINT:
            return {"lezioniCalendario": {"test": copy.deepcopy(rows)}}
        return []

    return fetch


def run(temp: Path, rows: list[dict] | Exception, now: datetime = NOW):
    paths = pipeline.PipelinePaths.from_root(temp, backup=temp / "backup")
    return paths, pipeline.run_feed(
        "year3-lessons",
        paths,
        now=now,
        fetcher=fetcher_for(rows),
        enable_alerts=False,
    )


class CalendarPipelineTests(unittest.TestCase):
    def test_success_publishes_primary_and_backup_with_matching_hashes(self):
      with tempfile.TemporaryDirectory() as directory:
        paths, result = run(Path(directory), [lesson("keep", "01/11/2026"), lesson("drop", "02/11/2026")])
        self.assertTrue(result["ok"])
        self.assertTrue(pipeline.verify_publication(paths, ["year3-lessons"])["ok"])
        primary_ics, primary_status = pipeline.feed_public_paths(paths, "year3-lessons")
        backup_ics = paths.backup / source.FEED_SPECS["year3-lessons"].route_path.lstrip("/")
        self.assertEqual(primary_ics.read_bytes(), backup_ics.read_bytes())
        status = json.loads(primary_status.read_text(encoding="utf-8"))
        self.assertTrue(status["healthy"])
        self.assertEqual(status["event_count"], 2)
        self.assertEqual(status["content_hash"], pipeline.sha256_text(primary_ics.read_bytes().decode("utf-8")))

    def test_one_valid_missing_snapshot_is_tentative_then_second_is_cancelled(self):
      with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        paths, first = run(root, [lesson("keep", "01/11/2026"), lesson("drop", "02/11/2026")])
        self.assertTrue(first["ok"])
        second = pipeline.run_feed(
            "year3-lessons",
            paths,
            now=NOW + timedelta(minutes=15),
            fetcher=fetcher_for([lesson("keep", "01/11/2026")]),
            enable_alerts=False,
        )
        self.assertTrue(second["ok"])
        self.assertEqual({item["type"] for item in second["alerts"]}, {"possible_cancellation"})
        status_path = pipeline.feed_public_paths(paths, "year3-lessons")[1]
        status = json.loads(status_path.read_text(encoding="utf-8"))
        dropped = next(row for row in status["events"] if row["uid"].endswith("drop@codex"))
        self.assertEqual(dropped["status"], "TENTATIVE")

        third = pipeline.run_feed(
            "year3-lessons",
            paths,
            now=NOW + timedelta(minutes=30),
            fetcher=fetcher_for([lesson("keep", "01/11/2026")]),
            enable_alerts=False,
        )
        self.assertTrue(third["ok"])
        self.assertEqual({item["type"] for item in third["alerts"]}, {"confirmed_cancellation"})
        status = json.loads(status_path.read_text(encoding="utf-8"))
        dropped = next(row for row in status["events"] if row["uid"].endswith("drop@codex"))
        self.assertEqual(dropped["status"], "CANCELLED")
        self.assertIn("two consecutive valid upstream snapshots", dropped["description"])

    def test_explicit_suspension_cancels_immediately(self):
      with tempfile.TemporaryDirectory() as directory:
        paths, result = run(Path(directory), [lesson("cancel", "01/11/2026", suspended=True), lesson("keep", "02/11/2026")])
        self.assertTrue(result["ok"])
        self.assertEqual({item["type"] for item in result["alerts"]}, {"explicit_cancellation"})
        status = json.loads(pipeline.feed_public_paths(paths, "year3-lessons")[1].read_text(encoding="utf-8"))
        cancelled = next(row for row in status["events"] if row["uid"].endswith("cancel@codex"))
        self.assertEqual(cancelled["status"], "CANCELLED")

    def test_source_failure_keeps_old_ics_and_marks_health_degraded(self):
      with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        paths, first = run(root, [lesson("keep", "01/11/2026")])
        self.assertTrue(first["ok"])
        ics_path, status_path = pipeline.feed_public_paths(paths, "year3-lessons")
        old_ics = ics_path.read_bytes()
        failed = pipeline.run_feed(
            "year3-lessons",
            paths,
            now=NOW + timedelta(hours=1),
            fetcher=fetcher_for(RuntimeError("synthetic upstream HTTP 500")),
            enable_alerts=False,
        )
        self.assertFalse(failed["ok"])
        self.assertEqual(failed["error"], "UnsafeSnapshotError")
        self.assertEqual(ics_path.read_bytes(), old_ics)
        status = json.loads(status_path.read_text(encoding="utf-8"))
        self.assertFalse(status["healthy"])
        self.assertTrue(status["stale"])
        self.assertEqual(status["consecutive_failures"], 1)
        self.assertIn("synthetic upstream HTTP 500", status["last_error"]["message"])
        self.assertFalse(pipeline.watchdog(paths, now=NOW + timedelta(hours=1), feed_ids=["year3-lessons"])["ok"])

    def test_empty_after_history_and_duplicate_uid_fail_closed(self):
      with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        paths, first = run(root, [lesson("keep", "01/11/2026")])
        self.assertTrue(first["ok"])
        ics_path, _ = pipeline.feed_public_paths(paths, "year3-lessons")
        old_ics = ics_path.read_bytes()

        empty = pipeline.run_feed(
            "year3-lessons",
            paths,
            now=NOW + timedelta(minutes=15),
            fetcher=fetcher_for([]),
            enable_alerts=False,
        )
        self.assertFalse(empty["ok"])
        self.assertIn("zero events", empty["message"])
        self.assertEqual(ics_path.read_bytes(), old_ics)

        duplicate = pipeline.run_feed(
            "year3-lessons",
            paths,
            now=NOW + timedelta(minutes=30),
            fetcher=fetcher_for([lesson("keep", "01/11/2026"), lesson("keep", "01/11/2026")]),
            enable_alerts=False,
        )
        self.assertFalse(duplicate["ok"])
        self.assertIn("duplicate event UIDs", duplicate["message"])
        self.assertEqual(ics_path.read_bytes(), old_ics)

    def test_watchdog_detects_publication_mismatch_and_health_json_is_written(self):
      with tempfile.TemporaryDirectory() as directory:
        paths, result = run(Path(directory), [lesson("keep", "01/11/2026")])
        self.assertTrue(result["ok"])
        ics_path, _ = pipeline.feed_public_paths(paths, "year3-lessons")
        ics_path.write_bytes(ics_path.read_bytes() + b"\n")
        verification = pipeline.verify_publication(paths, ["year3-lessons"])
        self.assertFalse(verification["ok"])
        self.assertTrue(any("hash mismatch" in error for error in verification["errors"]))

