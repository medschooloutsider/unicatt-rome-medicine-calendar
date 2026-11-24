#!/usr/bin/env python3
"""Single-flight GitHub publisher recovery with bounded API-race tolerance."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime
from typing import Any, Callable


RUN_URL_RE = re.compile(r"/actions/runs/(\d+)")
DEFAULT_REPO = "medschooloutsider/unicatt-rome-medicine-calendar"
DEFAULT_WORKFLOW = "calendar-publish.yml"


def gh_path() -> str:
    return os.environ.get("GH_BIN") or shutil.which("gh") or "/opt/homebrew/bin/gh"


def run_gh(args: list[str], *, timeout: int = 30) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [gh_path(), *args],
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def parse_run_id(output: str) -> str | None:
    match = RUN_URL_RE.search(output)
    return match.group(1) if match else None


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _run_snapshot(run_id: str, repo: str) -> dict[str, Any] | None:
    result = run_gh(["run", "view", run_id, "-R", repo, "--json", "status,conclusion,updatedAt"])
    if result.returncode != 0:
        # GitHub's run index is eventually consistent after workflow_dispatch;
        # a 404 or connection error is pending visibility, not failure.
        return None
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def wait_for_run(
    run_id: str,
    repo: str,
    *,
    timeout_seconds: int = 240,
    poll_seconds: float = 5.0,
    clock: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    deadline = clock() + timeout_seconds
    last_snapshot: dict[str, Any] | None = None
    while clock() <= deadline:
        snapshot = _run_snapshot(run_id, repo)
        if snapshot is not None:
            last_snapshot = snapshot
            if snapshot.get("status") == "completed":
                return {"run_id": run_id, **snapshot}
        sleeper(poll_seconds)
    return {
        "run_id": run_id,
        "status": "timeout",
        "conclusion": None,
        "last_snapshot": last_snapshot,
    }


def _workflow_dispatches(repo: str, workflow: str) -> list[dict[str, Any]]:
    result = run_gh(
        [
            "run",
            "list",
            "-R",
            repo,
            "--workflow",
            workflow,
            "--event",
            "workflow_dispatch",
            "--limit",
            "20",
            "--json",
            "databaseId,createdAt,status,conclusion",
        ]
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "unable to list publisher runs")
    payload = json.loads(result.stdout or "[]")
    return payload if isinstance(payload, list) else []


def _wait_for_new_dispatch_run(
    repo: str,
    workflow: str,
    before_ids: set[str],
    *,
    created_after: datetime,
    timeout_seconds: int,
    poll_seconds: float,
    clock: Callable[[], float],
    sleeper: Callable[[float], None],
) -> str | None:
    """Resolve a dispatch to its exact run when gh cannot return a URL."""
    deadline = clock() + timeout_seconds
    while clock() <= deadline:
        for run in _workflow_dispatches(repo, workflow):
            run_id = str(run.get("databaseId", ""))
            created_at = run.get("createdAt")
            if run_id and run_id not in before_ids and created_at:
                if _parse_iso(str(created_at)) >= created_after:
                    return run_id
        sleeper(poll_seconds)
    return None


def recover(
    *,
    repo: str = DEFAULT_REPO,
    workflow: str = DEFAULT_WORKFLOW,
    reason: str = "sentinel-stale-publication",
    cooldown_seconds: int = 1800,
    timeout_seconds: int = 240,
    now: datetime | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    observed = now or datetime.now(UTC)
    runs = _workflow_dispatches(repo, workflow)
    active = [item for item in runs if item.get("status") in {"queued", "in_progress"}]
    selected: dict[str, Any] | None = active[0] if active else None
    mode = "existing-active"
    if selected is None and runs:
        latest = runs[0]
        created = _parse_iso(str(latest["createdAt"]))
        age = max(0, int((observed - created).total_seconds()))
        if age < cooldown_seconds:
            selected = latest
            mode = "cooldown"
    if selected is None:
        before_ids = {str(item.get("databaseId")) for item in runs}
        dispatch_started = observed
        result = run_gh(
            [
                "workflow",
                "run",
                workflow,
                "-R",
                repo,
                "--ref",
                "main",
                "-f",
                f"recovery_reason={reason}",
            ]
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "publisher dispatch failed")
        run_id = parse_run_id(result.stdout + result.stderr)
        if run_id is None:
            run_id = _wait_for_new_dispatch_run(
                repo,
                workflow,
                before_ids,
                created_after=dispatch_started,
                timeout_seconds=min(60, timeout_seconds),
                poll_seconds=2,
                clock=clock,
                sleeper=sleeper,
            )
        if run_id is None:
            raise RuntimeError("publisher dispatch returned no resolvable workflow run")
        mode = "dispatched"
    else:
        run_id = str(selected.get("databaseId"))
    snapshot = wait_for_run(run_id, repo, timeout_seconds=timeout_seconds)
    return {
        "ok": snapshot.get("status") == "completed" and snapshot.get("conclusion") == "success",
        "mode": mode,
        "run_id": run_id,
        "status": snapshot.get("status"),
        "conclusion": snapshot.get("conclusion"),
        "last_snapshot": snapshot.get("last_snapshot"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("recover", nargs="?")
    parser.add_argument("--repo", default=DEFAULT_REPO)
    parser.add_argument("--workflow", default=DEFAULT_WORKFLOW)
    parser.add_argument("--reason", default="sentinel-stale-publication")
    parser.add_argument("--cooldown-seconds", type=int, default=1800)
    parser.add_argument("--timeout-seconds", type=int, default=240)
    args = parser.parse_args()
    if args.recover != "recover":
        parser.error("the only command is: recover")
    try:
        result = recover(
            repo=args.repo,
            workflow=args.workflow,
            reason=args.reason,
            cooldown_seconds=args.cooldown_seconds,
            timeout_seconds=args.timeout_seconds,
        )
    except Exception as exc:
        print(json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"}))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
