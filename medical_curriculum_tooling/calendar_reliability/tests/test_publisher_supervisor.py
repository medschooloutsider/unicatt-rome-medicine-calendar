from __future__ import annotations

import json
import subprocess
import unittest
from unittest import mock

from medical_curriculum_tooling.calendar_reliability import publisher_supervisor


class PublisherSupervisorTests(unittest.TestCase):
    def test_parse_run_id_from_workflow_dispatch_output(self):
        self.assertEqual(
            publisher_supervisor.parse_run_id(
                "https://github.com/medschooloutsider/unicatt-rome-medicine-calendar/actions/runs/12345"
            ),
            "12345",
        )

    def test_wait_treats_missing_run_as_pending_visibility(self):
        snapshots = iter(
            [
                None,
                {"status": "in_progress", "conclusion": None},
                {"status": "completed", "conclusion": "success"},
            ]
        )
        sleeper = mock.Mock()
        with mock.patch.object(publisher_supervisor, "_run_snapshot", side_effect=lambda *_args: next(snapshots)):
            result = publisher_supervisor.wait_for_run("123", "repo", timeout_seconds=10, poll_seconds=2, sleeper=sleeper)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["conclusion"], "success")
        self.assertEqual(sleeper.call_count, 2)

    def test_recovery_uses_active_run_instead_of_dispatching_duplicate(self):
        active = [{"databaseId": 456, "createdAt": "2026-11-24T09:00:00Z", "status": "in_progress", "conclusion": None}]
        with mock.patch.object(publisher_supervisor, "_workflow_dispatches", return_value=active), mock.patch.object(
            publisher_supervisor, "wait_for_run", return_value={"status": "completed", "conclusion": "success"}
        ) as waiter, mock.patch.object(publisher_supervisor, "run_gh") as runner:
            result = publisher_supervisor.recover(
                repo="repo",
                workflow="calendar-publish.yml",
                now=publisher_supervisor._parse_iso("2026-11-24T09:01:00Z"),
            )
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "existing-active")
        waiter.assert_called_once_with("456", "repo", timeout_seconds=240)
        runner.assert_not_called()

    def test_recovery_dispatches_and_waits_for_exact_run(self):
        dispatch = subprocess.CompletedProcess(
            ["gh"], 0,
            stdout="https://github.com/medschooloutsider/unicatt-rome-medicine-calendar/actions/runs/789\n",
            stderr="",
        )
        with mock.patch.object(publisher_supervisor, "_workflow_dispatches", return_value=[]), mock.patch.object(
            publisher_supervisor, "run_gh", return_value=dispatch
        ) as runner, mock.patch.object(
            publisher_supervisor, "wait_for_run", return_value={"status": "completed", "conclusion": "success"}
        ) as waiter:
            result = publisher_supervisor.recover(repo="repo", now=publisher_supervisor._parse_iso("2026-11-24T09:01:00Z"))
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "dispatched")
        self.assertEqual(result["run_id"], "789")
        self.assertEqual(runner.call_count, 1)
        waiter.assert_called_once_with("789", "repo", timeout_seconds=240)

    def test_recovery_resolves_run_when_dispatch_output_has_no_url(self):
        dispatch = subprocess.CompletedProcess(["gh"], 0, stdout="Created", stderr="")
        discovered = [{"databaseId": 999, "createdAt": "2026-11-24T09:01:01Z", "status": "queued", "conclusion": None}]
        with mock.patch.object(publisher_supervisor, "_workflow_dispatches", side_effect=[[], discovered]), mock.patch.object(
            publisher_supervisor, "run_gh", return_value=dispatch
        ) as runner, mock.patch.object(
            publisher_supervisor, "wait_for_run", return_value={"status": "completed", "conclusion": "success"}
        ) as waiter:
            result = publisher_supervisor.recover(
                repo="repo",
                now=publisher_supervisor._parse_iso("2026-11-24T09:01:00Z"),
                clock=lambda: 0.0,
                sleeper=lambda _delay: None,
            )
        self.assertTrue(result["ok"])
        self.assertEqual(result["run_id"], "999")
        runner.assert_called_once()
        waiter.assert_called_once_with("999", "repo", timeout_seconds=240)


if __name__ == "__main__":
    unittest.main()
