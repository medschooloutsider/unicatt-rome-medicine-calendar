"""Regression test: the legacy refresh route must fail visibly, not HTTP 500."""

import unittest

from medical_curriculum_tooling.calendar_station_deploy import unicatt_calendar_broadcast as deployed


class LegacyRouteTests(unittest.TestCase):
  def test_refresh_provider_failure_returns_machine_readable_503(self):
    def fail(*_args, **_kwargs):
        raise RuntimeError("synthetic RuntimeCache/provider failure")

    original = deployed.maybe_refresh_all
    deployed.maybe_refresh_all = fail
    try:
        result = deployed.handle_route("/api/refresh", "force=1", runtime_cache=None)
    finally:
        deployed.maybe_refresh_all = original
    self.assertEqual(result.status, 503)
    self.assertIn(b'"failure_visible": true', result.body)
    self.assertIn(b"synthetic RuntimeCache/provider failure", result.body)

