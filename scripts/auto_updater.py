"""Auto-update worker that polls the remote repo every 10 minutes.

When a new commit lands on the tracked branch of the upstream release repo
(``awp-worknet/mine-skill``), this updater performs a fast-forward pull and
signals the running worker to stop cleanly. The host agent's supervisor will
then restart the worker, picking up the new code on the next launch.

Disabled by setting ``MINE_AUTO_UPDATE=0``. By default it is on whenever the
project directory is a git checkout.
"""
from __future__ import annotations

import logging
import os
import subprocess
import threading
from pathlib import Path
from typing import Callable

log = logging.getLogger("agent.auto_update")

UPSTREAM_REPO = "https://github.com/awp-worknet/mine-skill.git"
DEFAULT_BRANCH = "main"
CHECK_INTERVAL_SECONDS = 600  # 10 minutes


class AutoUpdater:
    """Background thread that periodically checks for upstream updates."""

    def __init__(
        self,
        project_root: Path,
        *,
        on_update_applied: Callable[[], None] | None = None,
        upstream_url: str = UPSTREAM_REPO,
        branch: str = DEFAULT_BRANCH,
        check_interval: int = CHECK_INTERVAL_SECONDS,
    ) -> None:
        self._project_root = Path(project_root).resolve()
        self._upstream_url = upstream_url
        self._branch = branch
        self._check_interval = check_interval
        self._on_update_applied = on_update_applied
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    # ── Public API ──

    def start(self) -> bool:
        """Start the auto-update thread. Returns True if started, False otherwise."""
        if os.environ.get("MINE_AUTO_UPDATE", "1").strip() == "0":
            log.info("Auto-update disabled via MINE_AUTO_UPDATE=0")
            return False
        if not self._is_git_repo():
            log.info("Auto-update skipped: not a git checkout at %s", self._project_root)
            return False
        if self._thread is not None and self._thread.is_alive():
            return True
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run, name="auto-updater", daemon=True,
        )
        self._thread.start()
        log.info(
            "Auto-update thread started (upstream=%s branch=%s interval=%ds)",
            self._upstream_url, self._branch, self._check_interval,
        )
        return True

    def stop(self) -> None:
        self._stop_event.set()
        t = self._thread
        # Don't join if called from the auto-updater thread itself (e.g.
        # on_update_applied → stop() → _stop_auto_updater → here).
        # Thread.join() on the current thread raises RuntimeError.
        if t is not None and t is not threading.current_thread():
            t.join(timeout=5)

    # ── Internal ──

    def _run(self) -> None:
        while not self._stop_event.is_set():
            # Wait first — don't check immediately on startup (let worker warm up)
            if self._stop_event.wait(timeout=self._check_interval):
                return
            try:
                self._check_and_update()
            except Exception as exc:
                log.warning("Auto-update check failed: %s", exc)

    def _is_git_repo(self) -> bool:
        return (self._project_root / ".git").exists()

    def _git(self, *args: str, timeout: float = 30) -> tuple[int, str, str]:
        """Run a git command in the project root. Returns (returncode, stdout, stderr)."""
        try:
            result = subprocess.run(
                ["git", *args],
                cwd=self._project_root,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            return result.returncode, result.stdout.strip(), result.stderr.strip()
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as exc:
            return 1, "", str(exc)

    def _get_local_head(self) -> str:
        rc, out, _ = self._git("rev-parse", "HEAD")
        return out if rc == 0 else ""

    def _get_local_branch(self) -> str:
        rc, out, _ = self._git("rev-parse", "--abbrev-ref", "HEAD")
        return out if rc == 0 else ""

    def _get_remote_head(self) -> str:
        """Query remote for the latest commit SHA on the tracked branch."""
        rc, out, err = self._git(
            "ls-remote", self._upstream_url, f"refs/heads/{self._branch}",
        )
        if rc != 0 or not out:
            log.debug("ls-remote failed: %s", err or "no output")
            return ""
        # Format: "<sha>\trefs/heads/<branch>"
        return out.split()[0] if out else ""

    def _check_and_update(self) -> None:
        # Only auto-update on the tracked branch — don't touch feature/detached state
        local_branch = self._get_local_branch()
        if local_branch != self._branch:
            log.debug(
                "Auto-update skipped: on branch %s, expected %s",
                local_branch, self._branch,
            )
            return

        local_head = self._get_local_head()
        remote_head = self._get_remote_head()
        if not local_head or not remote_head:
            return
        if local_head == remote_head:
            log.debug("Up to date (HEAD=%s)", local_head[:8])
            return

        log.info(
            "Update available: local=%s → remote=%s. Pulling...",
            local_head[:8], remote_head[:8],
        )

        # Fetch from the upstream URL directly — don't rely on existing remotes
        rc, _, err = self._git("fetch", self._upstream_url, self._branch, timeout=60)
        if rc != 0:
            log.warning("Fetch failed: %s", err)
            return

        # Try fast-forward first; fall back to reset for release-repo consumers
        # where the upstream has merge commits (sync workflow creates PRs with
        # merge commits, making ff-only impossible from the dev repo history).
        rc, _, err = self._git("merge", "--ff-only", "FETCH_HEAD", timeout=30)
        if rc != 0:
            # Check if local has unpushed commits that should be preserved
            rc2, local_only, _ = self._git(
                "rev-list", "FETCH_HEAD..HEAD", "--count",
            )
            local_count = int(local_only.strip()) if rc2 == 0 and local_only.strip().isdigit() else 0
            if local_count > 0:
                log.warning(
                    "Cannot auto-update: %d local commit(s) not in upstream. "
                    "Push or discard them first.",
                    local_count,
                )
                return
            # No local-only commits — safe to reset to upstream
            log.info("Fast-forward not possible (upstream has merge commits); resetting to FETCH_HEAD")
            rc, _, err = self._git("reset", "--hard", "FETCH_HEAD", timeout=30)
            if rc != 0:
                log.warning("Reset to FETCH_HEAD failed: %s", err)
                return

        new_head = self._get_local_head()
        log.info(
            "✓ Auto-update applied: %s → %s. Signaling worker to restart.",
            local_head[:8], new_head[:8],
        )

        if self._on_update_applied is not None:
            try:
                self._on_update_applied()
            except Exception as exc:
                log.warning("Update callback failed: %s", exc)
