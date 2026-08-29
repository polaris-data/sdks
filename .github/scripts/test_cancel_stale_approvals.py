"""Tests for the release approval timeout watchdog."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone

from cancel_stale_approvals import (
    CancellationConflict,
    collect_candidates,
    expire_stale_approvals,
)

CUTOFF = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)


def run(run_id: int, name: str, status: str = "waiting") -> dict[str, object]:
    return {"id": run_id, "name": name, "status": status}


def job(
    name: str = "publish",
    status: str = "waiting",
    created_at: str = "2026-08-29T11:49:00Z",
) -> dict[str, str]:
    return {"name": name, "status": status, "created_at": created_at}


class FakeClient:
    def __init__(self) -> None:
        self.runs = []
        self.jobs = {}
        self.current_runs = {}
        self.current_jobs = {}
        self.conflicts = set()
        self.cancelled = []

    def list_waiting_runs(self, repository: str) -> list[dict[str, object]]:
        return self.runs

    def list_jobs(self, repository: str, run_id: int) -> list[dict[str, str]]:
        return self.current_jobs.get(run_id, self.jobs[run_id])

    def get_run(self, repository: str, run_id: int) -> dict[str, object]:
        return self.current_runs.get(run_id, run(run_id, "Release Rust"))

    def cancel_run(self, repository: str, run_id: int) -> None:
        if run_id in self.conflicts:
            raise CancellationConflict
        self.cancelled.append(run_id)


class WatchdogTests(unittest.TestCase):
    def test_selects_only_stale_waiting_release_publish_jobs(self) -> None:
        client = FakeClient()
        client.runs = [
            run(1, "Release Rust"),
            run(2, "Release Python"),
            run(3, "Release TypeScript"),
            run(4, "CI"),
            run(5, "Release Rust"),
            run(6, "Release Python"),
        ]
        client.jobs = {
            1: [job()],
            2: [job(created_at="2026-08-29T12:05:00Z")],
            3: [job(status="in_progress")],
            4: [job()],
            5: [job(name="validate")],
            6: [job(status="completed")],
        }

        candidates = collect_candidates(client, "polaris-data/sdks", CUTOFF)

        self.assertEqual([candidate.run_id for candidate in candidates], [1])

    def test_rechecks_state_before_cancelling(self) -> None:
        client = FakeClient()
        client.runs = [run(1, "Release Rust")]
        client.jobs = {1: [job()]}
        client.current_runs = {1: run(1, "Release Rust", "in_progress")}

        outcomes = expire_stale_approvals(
            client, "polaris-data/sdks", CUTOFF, dry_run=False
        )

        self.assertEqual(outcomes[0].action, "skipped: state changed")
        self.assertEqual(client.cancelled, [])

    def test_dry_run_does_not_cancel(self) -> None:
        client = FakeClient()
        client.runs = [run(1, "Release Python")]
        client.jobs = {1: [job()]}

        outcomes = expire_stale_approvals(
            client, "polaris-data/sdks", CUTOFF, dry_run=True
        )

        self.assertEqual(outcomes[0].action, "would cancel")
        self.assertEqual(client.cancelled, [])

    def test_cancels_a_stale_approval(self) -> None:
        client = FakeClient()
        client.runs = [run(1, "Release Rust")]
        client.jobs = {1: [job()]}

        outcomes = expire_stale_approvals(
            client, "polaris-data/sdks", CUTOFF, dry_run=False
        )

        self.assertEqual(outcomes[0].action, "cancelled")
        self.assertEqual(client.cancelled, [1])

    def test_cancellation_conflict_is_a_no_op(self) -> None:
        client = FakeClient()
        client.runs = [run(1, "Release TypeScript")]
        client.jobs = {1: [job()]}
        client.conflicts = {1}

        outcomes = expire_stale_approvals(
            client, "polaris-data/sdks", CUTOFF, dry_run=False
        )

        self.assertEqual(outcomes[0].action, "skipped: state changed")
        self.assertEqual(client.cancelled, [])


if __name__ == "__main__":
    unittest.main()
