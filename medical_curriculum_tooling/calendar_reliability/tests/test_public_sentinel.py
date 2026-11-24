from __future__ import annotations

import hashlib
import json
import unittest
from datetime import UTC, datetime, timedelta
from urllib.error import HTTPError
from unittest import mock

from medical_curriculum_tooling.calendar_reliability import check_public_calendar


class PublicSentinelTests(unittest.TestCase):
    class _Response:
        def __init__(self, payload: bytes):
            self.payload = payload

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return self.payload

    def test_fetch_retries_transient_503_then_succeeds(self):
        transient = HTTPError("https://example.test/feed", 503, "busy", {}, None)
        with mock.patch.object(
            check_public_calendar,
            "urlopen",
            side_effect=[transient, self._Response(b"ok")],
        ) as opener, mock.patch.object(check_public_calendar.time, "sleep") as sleeper:
            self.assertEqual(
                check_public_calendar.fetch(
                    "https://example.test/feed",
                    attempts=3,
                    retry_delays_seconds=(0.25, 0.5),
                ),
                b"ok",
            )
        self.assertEqual(opener.call_count, 2)
        sleeper.assert_called_once_with(0.25)

    def test_fetch_does_not_retry_non_transient_404(self):
        missing = HTTPError("https://example.test/feed", 404, "missing", {}, None)
        with mock.patch.object(check_public_calendar, "urlopen", side_effect=missing) as opener:
            with self.assertRaises(HTTPError):
                check_public_calendar.fetch("https://example.test/feed", attempts=3)
        self.assertEqual(opener.call_count, 1)

    def _responses(self, expires_at: str) -> dict[str, bytes]:
        ics = "BEGIN:VCALENDAR\nUID:fixture-uid\nEND:VCALENDAR\n"
        status = {
            "events": [{"uid": "fixture-uid"}],
            "healthy": True,
            "stale": False,
            "last_error": None,
            "last_publication_at_utc": datetime.now(UTC).isoformat(),
            "content_hash": hashlib.sha256(ics.encode("utf-8")).hexdigest(),
        }
        responses = {"https://example.test/health.json": json.dumps({
            "ok": True,
            "freshness_lease": {"expires_at_utc": expires_at},
        }).encode("utf-8")}
        for route in check_public_calendar.FEED_ROUTES:
            responses[f"https://example.test/{route}-status.json"] = json.dumps(status).encode("utf-8")
            responses[f"https://example.test/{route}.ics"] = ics.encode("utf-8")
        return responses

    def test_freshness_lease_is_required(self):
        future = (datetime.now(UTC) + timedelta(minutes=10)).isoformat()
        responses = self._responses(future)
        with mock.patch.object(check_public_calendar, "fetch", side_effect=responses.__getitem__):
            result = check_public_calendar.check("https://example.test", max_age_seconds=3600)
        self.assertTrue(result["ok"])

        expired = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
        responses = self._responses(expired)
        with mock.patch.object(check_public_calendar, "fetch", side_effect=responses.__getitem__):
            result = check_public_calendar.check("https://example.test", max_age_seconds=3600)
        self.assertFalse(result["ok"])
        self.assertIn("health.json: freshness lease expired or missing", result["failures"])


if __name__ == "__main__":
    unittest.main()
