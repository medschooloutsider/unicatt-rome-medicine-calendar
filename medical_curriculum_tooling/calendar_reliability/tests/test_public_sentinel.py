from __future__ import annotations

import hashlib
import json
import unittest
from datetime import UTC, datetime, timedelta
from unittest import mock

from medical_curriculum_tooling.calendar_reliability import check_public_calendar


class PublicSentinelTests(unittest.TestCase):
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
