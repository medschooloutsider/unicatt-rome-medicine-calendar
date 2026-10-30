"""Failure-mode tests for secret-safe Telegram alert delivery."""

from __future__ import annotations

import json
import tempfile
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread

from medical_curriculum_tooling.calendar_reliability import telegram_notifier


class Response:
    def __init__(self, payload: dict):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


class TelegramNotifierTests(unittest.TestCase):
    def test_human_cancellation_message_has_schedule_details_not_machine_fields(self):
        message = telegram_notifier.render_message(
            {
                "type": "confirmed_cancellation",
                "feed_id": "year3-lessons",
                "summary": "FIRST AID AND NURSING PRACTICE PROFESSIONAL TRAINING (MECHANISMS AND MANIFESTATIONS OF DISEASES)",
                "timezone_id": "Europe/Rome",
                "event": {
                    "uid": "unicatt-lesson-occurrence-122054170@codex",
                    "summary": "FIRST AID AND NURSING PRACTICE PROFESSIONAL TRAINING (MECHANISMS AND MANIFESTATIONS OF DISEASES)",
                    "start": "2026-11-03T14:15:00+01:00",
                    "end": "2026-11-03T19:15:00+01:00",
                    "location": "Policlinico - Gemelli Training Center",
                    "description": "Teacher: SCAPIGLIATI ANDREA\nSource date: 03/11/2026",
                    "status": "CANCELLED",
                },
            },
            "internal-fingerprint",
        )
        self.assertIn("cancellation confirmed", message)
        self.assertIn("Tuesday 03 November 2026, 14:15-19:15 (Europe/Rome)", message)
        self.assertIn("Policlinico - Gemelli Training Center", message)
        self.assertIn("Teacher: Scapigliati Andrea", message)
        self.assertNotIn("UID:", message)
        self.assertNotIn("Fingerprint:", message)
        self.assertNotIn("Observed:", message)

    def test_grouped_occurrences_are_one_human_message(self):
        message = telegram_notifier.render_message(
            {
                "type": "confirmed_cancellation",
                "feed_id": "year3-lessons",
                "summary": "FIRST AID (MECHANISMS AND MANIFESTATIONS OF DISEASES)",
                "timezone_id": "Europe/Rome",
                "events": [
                    {
                        "summary": "FIRST AID (MECHANISMS AND MANIFESTATIONS OF DISEASES)",
                        "start": "2026-11-03T14:15:00+01:00",
                        "end": "2026-11-03T19:15:00+01:00",
                        "location": "Policlinico - Gemelli Training Center",
                    },
                    {
                        "summary": "FIRST AID (MECHANISMS AND MANIFESTATIONS OF DISEASES)",
                        "start": "2026-11-09T14:15:00+01:00",
                        "end": "2026-11-09T19:15:00+01:00",
                        "location": "Policlinico - Gemelli Training Center",
                    },
                ],
            },
            "internal-fingerprint",
        )
        self.assertIn("Occurrences: 2", message)
        self.assertIn("Tuesday 03 November 2026", message)
        self.assertIn("Monday 09 November 2026", message)

    def test_provisional_cancellation_is_suppressed(self):
        def opener(*_args, **_kwargs):
            raise AssertionError("provisional cancellation must not reach Telegram")

        with tempfile.TemporaryDirectory() as directory:
            result = telegram_notifier.notify_alert(
                {
                    "type": "possible_cancellation",
                    "feed_id": "year3-lessons",
                    "summary": "Test class",
                    "event": {
                        "summary": "Test class",
                        "start": "2026-11-03T14:15:00+01:00",
                        "end": "2026-11-03T19:15:00+01:00",
                    },
                },
                Path(directory) / "ledger.json",
                token="token",
                chat_id="123",
                opener=opener,
            )
            self.assertEqual(result["status"], "suppressed_provisional")
            self.assertFalse((Path(directory) / "ledger.json").exists())

    def test_success_is_recorded_and_duplicate_is_suppressed(self):
        calls = []

        def opener(request, timeout):
            calls.append((request, timeout))
            return Response({"ok": True, "result": {"message_id": 41}})

        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory) / "alerts" / "telegram-delivery.json"
            event = {"type": "confirmed_cancellation", "feed_id": "year3-lessons", "uid": "u1", "summary": "Test class"}
            first = telegram_notifier.notify_alert(event, receipt, token="secret-token", chat_id="123", opener=opener)
            second = telegram_notifier.notify_alert(event, receipt, token="secret-token", chat_id="123", opener=opener)
            self.assertEqual(first["status"], "sent")
            self.assertEqual(second["status"], "duplicate")
            self.assertEqual(len(calls), 1)
            self.assertNotIn("secret-token", receipt.read_text())
            self.assertEqual(json.loads(receipt.read_text())["sent"][first["fingerprint"]]["message_id"], 41)

    def test_transient_failures_retry_then_succeed(self):
        calls = []

        def opener(request, timeout):
            calls.append(1)
            if len(calls) < 3:
                return Response({"ok": False, "error_code": 500})
            return Response({"ok": True, "result": {"message_id": 42}})

        with tempfile.TemporaryDirectory() as directory:
            result = telegram_notifier.notify_alert(
                {"type": "heartbeat", "feed_id": "all", "fingerprint": "heartbeat:test"},
                Path(directory) / "ledger.json",
                token="token",
                chat_id="123",
                opener=opener,
                sleep=lambda _seconds: None,
            )
            self.assertEqual(result["status"], "sent")
            self.assertEqual(result["attempts"], 3)
            self.assertEqual(len(calls), 3)

    def test_failure_is_recorded_without_raising(self):
        def opener(request, timeout):
            return Response({"ok": False, "error_code": 403})

        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory) / "ledger.json"
            result = telegram_notifier.notify_alert(
                {"type": "watchdog_failure", "feed_id": "all", "message": "synthetic failure"},
                receipt,
                token="token",
                chat_id="123",
                opener=opener,
                sleep=lambda _seconds: None,
            )
            self.assertEqual(result["status"], "failed")
            self.assertIn(result["fingerprint"], json.loads(receipt.read_text())["failures"])

    def test_missing_configuration_is_explicit_but_nonfatal(self):
        with tempfile.TemporaryDirectory() as directory:
            result = telegram_notifier.notify_alert(
                {"type": "heartbeat", "feed_id": "all", "fingerprint": "heartbeat:missing"},
                Path(directory) / "ledger.json",
                token="",
                chat_id="",
            )
            self.assertEqual(result["status"], "not_configured")

    def test_real_local_http_transport_retries_and_records_message_id(self):
        class Handler(BaseHTTPRequestHandler):
            attempts = 0
            bodies = []

            def do_POST(self):  # noqa: N802 - stdlib handler API.
                Handler.attempts += 1
                length = int(self.headers.get("Content-Length", "0"))
                Handler.bodies.append(self.rfile.read(length).decode("utf-8"))
                if Handler.attempts == 1:
                    self.send_response(500)
                    payload = {"ok": False, "error_code": 500}
                else:
                    self.send_response(200)
                    payload = {"ok": True, "result": {"message_id": 77}}
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(payload).encode("utf-8"))

            def log_message(self, *_args):
                return

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                result = telegram_notifier.notify_alert(
                    {"type": "heartbeat", "feed_id": "all", "fingerprint": "http-transport-test"},
                    Path(directory) / "ledger.json",
                    token="token",
                    chat_id="123",
                    sleep=lambda _seconds: None,
                    api_base=f"http://127.0.0.1:{server.server_port}",
                )
                self.assertEqual(result["status"], "sent")
                self.assertEqual(result["message_id"], 77)
                self.assertEqual(Handler.attempts, 2)
                self.assertTrue(any("chat_id=123" in body for body in Handler.bodies))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
