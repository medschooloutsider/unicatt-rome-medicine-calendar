"""Failure-mode tests for secret-safe Telegram alert delivery."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

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


if __name__ == "__main__":
    unittest.main()
