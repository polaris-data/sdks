"""Cancel release runs that have waited too long for environment approval."""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

RELEASE_WORKFLOWS = frozenset({"Release Python", "Release Rust", "Release TypeScript"})
API_URL = "https://api.github.com"


class CancellationConflict(Exception):
    """The run changed state before cancellation completed."""


class GitHubClient:
    def __init__(self, token: str) -> None:
        self.token = token

    def _request(self, method: str, path: str) -> dict[str, Any] | None:
        request = urllib.request.Request(
            f"{API_URL}{path}",
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "User-Agent": "polaris-approval-timeout",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urllib.request.urlopen(request) as response:
                body = response.read()
        except urllib.error.HTTPError as error:
            if method == "POST" and error.code == 409:
                raise CancellationConflict from error
            detail = error.read().decode(errors="replace")
            raise RuntimeError(
                f"GitHub API {method} {path} failed with {error.code}: {detail}"
            ) from error
        return json.loads(body) if body else None

    def _list(self, path: str, key: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        page = 1
        separator = "&" if "?" in path else "?"
        while True:
            response = self._request(
                "GET", f"{path}{separator}per_page=100&page={page}"
            )
            assert response is not None
            batch = response[key]
            items.extend(batch)
            if len(batch) < 100:
                return items
            page += 1

    def list_waiting_runs(self, repository: str) -> list[dict[str, Any]]:
        return self._list(
            f"/repos/{repository}/actions/runs?status=waiting", "workflow_runs"
        )

    def list_jobs(self, repository: str, run_id: int) -> list[dict[str, Any]]:
        return self._list(
            f"/repos/{repository}/actions/runs/{run_id}/jobs?filter=all", "jobs"
        )

    def get_run(self, repository: str, run_id: int) -> dict[str, Any]:
        response = self._request("GET", f"/repos/{repository}/actions/runs/{run_id}")
        assert response is not None
        return response

    def cancel_run(self, repository: str, run_id: int) -> None:
        self._request("POST", f"/repos/{repository}/actions/runs/{run_id}/cancel")


@dataclass(frozen=True)
class Candidate:
    run_id: int
    workflow: str
    waiting_since: datetime


@dataclass(frozen=True)
class Outcome:
    candidate: Candidate
    action: str


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def stale_publish_job(
    jobs: list[dict[str, Any]], cutoff: datetime
) -> dict[str, Any] | None:
    for job in jobs:
        created_at = job.get("created_at")
        if (
            job.get("name") == "publish"
            and job.get("status") == "waiting"
            and isinstance(created_at, str)
            and parse_timestamp(created_at) <= cutoff
        ):
            return job
    return None


def collect_candidates(
    client: Any, repository: str, cutoff: datetime
) -> list[Candidate]:
    candidates = []
    for run in client.list_waiting_runs(repository):
        if run.get("name") not in RELEASE_WORKFLOWS:
            continue
        run_id = run.get("id")
        if not isinstance(run_id, int):
            continue
        job = stale_publish_job(client.list_jobs(repository, run_id), cutoff)
        if job is not None:
            candidates.append(
                Candidate(run_id, run["name"], parse_timestamp(job["created_at"]))
            )
    return candidates


def expire_stale_approvals(
    client: Any, repository: str, cutoff: datetime, dry_run: bool
) -> list[Outcome]:
    outcomes = []
    for candidate in collect_candidates(client, repository, cutoff):
        run = client.get_run(repository, candidate.run_id)
        job = stale_publish_job(client.list_jobs(repository, candidate.run_id), cutoff)
        if run.get("status") != "waiting" or job is None:
            outcomes.append(Outcome(candidate, "skipped: state changed"))
            continue
        if dry_run:
            outcomes.append(Outcome(candidate, "would cancel"))
            continue
        try:
            client.cancel_run(repository, candidate.run_id)
        except CancellationConflict:
            outcomes.append(Outcome(candidate, "skipped: state changed"))
        else:
            outcomes.append(Outcome(candidate, "cancelled"))
    return outcomes


def write_summary(outcomes: list[Outcome], dry_run: bool) -> None:
    heading = (
        "Approval timeout watchdog (dry run)"
        if dry_run
        else "Approval timeout watchdog"
    )
    lines = [f"## {heading}", ""]
    if not outcomes:
        lines.append("No stale release approvals found.")
    else:
        lines.extend(
            f"- `{outcome.candidate.workflow}` run {outcome.candidate.run_id}: "
            f"{outcome.action} (waiting since "
            f"{outcome.candidate.waiting_since.isoformat()})"
            for outcome in outcomes
        )
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with Path(summary_path).open("a") as summary:
            summary.write("\n".join(lines) + "\n")
    print("\n".join(lines))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", required=True)
    parser.add_argument("--cutoff-minutes", type=int, default=10)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.cutoff_minutes < 1:
        parser.error("--cutoff-minutes must be positive")

    token = os.environ.get("GH_TOKEN")
    if not token:
        print("GH_TOKEN is required", file=sys.stderr)
        return 2

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=args.cutoff_minutes)
    outcomes = expire_stale_approvals(
        GitHubClient(token), args.repository, cutoff, args.dry_run
    )
    write_summary(outcomes, args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
