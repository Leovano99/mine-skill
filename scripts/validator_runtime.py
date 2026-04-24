"""ValidatorRuntime – main event loop for the validator agent.

Optimized with patterns from example-worker.py:
- Consecutive failure tracking with alerting (#1)
- Status file for external monitoring (#4)
- JSONL history logging (#5)
- Hot-reloadable config file (#6)
- Auto-restart on crash (#7)
- Notification system via openclaw message (#8)
- Stats persistence across restarts (#9)
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from common import (
    resolve_eval_timeout,
    resolve_validator_id,
)
from evaluation_engine import EvaluationEngine, EvaluationResult
import httpx
from lib.platform_client import PlatformApiError
from ws_client import ValidatorWSClient, WSDisconnected, WSMessage

_HTTPStatusError = httpx.HTTPStatusError

log = logging.getLogger("validator.runtime")

HEARTBEAT_INTERVAL = 55
WS_RECEIVE_TIMEOUT = 30.0
FALLBACK_ALERT_THRESHOLD = 5


class ValidatorRuntime:
    """Orchestrates the validator lifecycle: connect, heartbeat, evaluate, report."""

    def __init__(
        self,
        *,
        platform_client: Any,
        ws_client: ValidatorWSClient,
        engine: EvaluationEngine | None = None,
        validator_id: str = "",
        heartbeat_interval: int = HEARTBEAT_INTERVAL,
        state_dir: str = "",
    ) -> None:
        self._platform = platform_client
        self._ws = ws_client
        self._engine = engine or EvaluationEngine(timeout=resolve_eval_timeout())
        self._validator_id = validator_id or resolve_validator_id()
        self._heartbeat_interval = heartbeat_interval

        self._running = False
        self._paused = False
        self._lock = threading.Lock()
        self._platform_lock = threading.Lock()
        self._heartbeat_thread: threading.Thread | None = None
        self._main_thread: threading.Thread | None = None
        self._auto_updater: Any = None
        self._stop_event = threading.Event()

        self._stats_lock = threading.Lock()
        self._stats: dict[str, int] = {
            "tasks_received": 0,
            "tasks_evaluated": 0,
            # match/mismatch are the validator's verdict on the miner data
            # — NOT "accepted/rejected by the platform". Both verdicts are
            # reported to the platform via report_evaluation and count as
            # valid evaluations. Previously these were named tasks_accepted /
            # tasks_rejected, which made the host LLM consistently hallucinate
            # that "rejected" meant the platform refused the submission.
            "tasks_match": 0,
            "tasks_mismatch": 0,
            "errors": 0,
            "consecutive_failures": 0,
        }
        self._start_time = time.monotonic()
        # Dynamically updated from heartbeat response
        self._eligible = True
        self._min_task_interval = 30
        self._in_ready_pool = False
        self._last_action = ""
        self._last_action_at = ""
        self._recent_actions: list[dict[str, str]] = []
        # Real-time phase for external status readers (host LLM).
        # Values: "starting", "waiting_for_task", "evaluating",
        #         "cooldown", "stopped".
        self._phase = "starting"
        self._phase_detail = ""  # e.g. "28s remaining" for cooldown

        # File paths for persistence
        suffix = f"-{self._validator_id}" if self._validator_id else ""
        if state_dir:
            base = Path(state_dir)
        else:
            from common import resolve_validator_output_root
            base = resolve_validator_output_root()
        base.mkdir(parents=True, exist_ok=True)
        self._status_file = base / f"validator{suffix}-status.json"
        self._history_file = base / f"validator{suffix}-history.jsonl"
        self._config_file = base / f"validator{suffix}-config.json"

    # ------------------------------------------------------------------
    # Persistence (#4, #5, #9)
    # ------------------------------------------------------------------

    def _snapshot_stats(self) -> dict[str, int]:
        """Return a thread-safe snapshot of the stats dict."""
        with self._stats_lock:
            return dict(self._stats)

    def _inc_stat(self, key: str, delta: int = 1) -> None:
        """Thread-safe stat increment."""
        with self._stats_lock:
            self._stats[key] = self._stats.get(key, 0) + delta

    def _set_stat(self, key: str, value: int) -> None:
        """Thread-safe stat set."""
        with self._stats_lock:
            self._stats[key] = value

    def _get_stat(self, key: str) -> int:
        """Thread-safe stat read."""
        with self._stats_lock:
            return self._stats.get(key, 0)

    def _write_status(self) -> None:
        """Write current status to JSON file for external monitoring."""
        with self._lock:
            eligible = self._eligible
            min_interval = self._min_task_interval
            in_pool = self._in_ready_pool
        status = {
            "running": self._running,
            "pid": os.getpid(),
            "uptime_seconds": int(time.monotonic() - self._start_time),
            "validator_id": self._validator_id,
            "eligible": eligible,
            "in_ready_pool": in_pool,
            "ws_connected": self._ws.connected,
            "phase": self._phase,
            "phase_detail": self._phase_detail,
            "stats": self._snapshot_stats(),
            "last_action": self._last_action,
            "last_action_at": self._last_action_at,
            "recent_actions": self._recent_actions[-30:],
            "min_task_interval": min_interval,
        }
        tmp = str(self._status_file) + f".tmp-{os.getpid()}-{threading.get_ident()}"
        try:
            self._status_file.parent.mkdir(parents=True, exist_ok=True)
            with open(tmp, "w") as f:
                json.dump(status, f, indent=2)
            os.replace(tmp, str(self._status_file))
        except OSError as e:
            log.warning("Failed to write status file: %s", e)
            try:
                os.unlink(tmp)
            except OSError:
                pass

    def _restore_stats(self) -> None:
        """Restore stats from previous run so counters survive restarts."""
        try:
            data = json.loads(self._status_file.read_text(encoding="utf-8"))
            prev = data.get("stats", {})
            # Backward-compat: old status files used tasks_accepted/tasks_rejected
            # before the rename to tasks_match/tasks_mismatch. Map them so a
            # validator upgraded mid-epoch doesn't lose its running totals.
            if "tasks_match" not in prev and "tasks_accepted" in prev:
                prev["tasks_match"] = prev.get("tasks_accepted", 0)
            if "tasks_mismatch" not in prev and "tasks_rejected" in prev:
                prev["tasks_mismatch"] = prev.get("tasks_rejected", 0)
            for key in self._stats:
                if key == "consecutive_failures":
                    continue  # reset on fresh start
                if key in prev and isinstance(prev[key], int):
                    self._stats[key] = prev[key]
            self._last_action = data.get("last_action", "")
            self._last_action_at = data.get("last_action_at", "")
            actions = data.get("recent_actions", [])
            if isinstance(actions, list):
                self._recent_actions = actions[-30:]
            log.info("Restored stats from previous run: %s", self._stats)
        except (OSError, json.JSONDecodeError, KeyError):
            log.info("No previous stats to restore, starting fresh")

    def _log_history(self, entry: dict[str, Any]) -> None:
        """Append evaluation record to JSONL history file."""
        entry["time"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            with open(self._history_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except OSError:
            pass

    def _record_action(self, action: str, detail: dict[str, Any] | None = None) -> None:
        """Record an action for status reporting."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self._last_action = action
        self._last_action_at = now
        entry: dict[str, Any] = {"time": now, "action": action}
        if detail:
            entry.update(detail)
        self._recent_actions.append(entry)
        if len(self._recent_actions) > 60:
            del self._recent_actions[:len(self._recent_actions) - 30]

    # ------------------------------------------------------------------
    # Config (#6)
    # ------------------------------------------------------------------

    def _read_config(self) -> dict[str, Any]:
        """Read hot-reloadable config. Edit the file to change behavior without restart."""
        defaults: dict[str, Any] = {
            "cli_timeout": 120,
            "notify_enabled": False,
            "notify_interval": 300,
        }
        try:
            data = json.loads(self._config_file.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                for key in defaults:
                    if key in data:
                        defaults[key] = data[key]
        except (OSError, json.JSONDecodeError):
            pass
        return defaults

    def _write_default_config(self) -> None:
        """Write default config file if it does not exist."""
        if self._config_file.exists():
            return
        try:
            self._config_file.write_text(json.dumps({
                "cli_timeout": 120,
                "notify_enabled": False,
                "notify_interval": 300,
            }, indent=2), encoding="utf-8")
            log.info("Config file created: %s", self._config_file)
        except OSError:
            pass

    # ------------------------------------------------------------------
    # Notification (#8)
    # ------------------------------------------------------------------

    def _send_notification(self, message: str) -> None:
        """Send notification via openclaw message send (if configured)."""
        cfg = self._read_config()
        if not cfg.get("notify_enabled"):
            return
        try:
            import subprocess
            import shutil
            openclaw_bin = shutil.which("openclaw") or "openclaw"
            subprocess.run(
                [openclaw_bin, "message", "send", "--message", message],
                capture_output=True, text=True, timeout=30,
            )
        except (subprocess.TimeoutExpired, FileNotFoundError):
            log.warning("Failed to send notification")

    # ------------------------------------------------------------------
    # Public control API
    # ------------------------------------------------------------------

    def start(self) -> dict[str, Any]:
        """Start the validator runtime (WS loop + heartbeat)."""
        with self._lock:
            if self._running:
                return self.status()
            self._running = True
            self._paused = False
            self._stop_event.clear()

        log.info("ValidatorRuntime starting (id=%s)", self._validator_id)

        # Optionally initialize OpenClaw agent workspace. This is only needed
        # when the CLI path is actually available — the evaluation engine
        # routes through llm_enrich (CLI → gateway → API), so on hosts without
        # openclaw we fall through silently instead of spamming warnings.
        try:
            from crawler.enrich.generative.openclaw_agent import openclaw_cli_available
            if openclaw_cli_available():
                import openclaw_llm
                agent_id = openclaw_llm.init(instance_id=self._validator_id)
                log.info("OpenClaw agent initialized: %s", agent_id)
            else:
                log.info("OpenClaw CLI not installed — using llm_enrich gateway/API routing for evaluation")
        except Exception as exc:
            log.warning("OpenClaw init skipped: %s", exc)

        # Restore stats from previous run (#9)
        self._restore_stats()
        self._write_default_config()

        # Check validator application status
        try:
            with self._platform_lock:
                app = self._platform.get_my_validator_application()
            app_status = str(app.get("status") or "")
            if app_status == "pending_review":
                log.warning("Validator application is pending review, cannot start yet")
                with self._lock:
                    self._running = False
                return self.status()
            if app_status == "rejected":
                log.warning("Validator application was rejected")
                with self._lock:
                    self._running = False
                return self.status()
            if not app_status:
                log.info("No validator application found, submitting one")
                with self._platform_lock:
                    self._platform.submit_validator_application()
                log.info("Validator application submitted, waiting for approval")
                with self._lock:
                    self._running = False
                return self.status()
        except (PlatformApiError, _HTTPStatusError) as err:
            status_code = err.status_code if isinstance(err, PlatformApiError) else err.response.status_code
            error_code = err.code if isinstance(err, PlatformApiError) else ""
            error_msg = str(err)
            if status_code == 403:
                log.warning("Validator application check got 403: %s (proceeding — will retry via heartbeat)", error_msg)
                # Don't stop — 403 on application check may be transient or a permission issue
                # that resolves after heartbeat. Only join_ready_pool 403 is fatal.
            else:
                log.warning("Validator application check failed: %s (proceeding anyway)", err)
        except Exception as exc:
            log.warning("Validator application check failed: %s (proceeding anyway)", exc)

        try:
            self._ws.connect()
        except WSDisconnected:
            log.warning("Initial WS connect failed; will retry in main loop")

        try:
            with self._platform_lock:
                self._platform.join_ready_pool()
            with self._lock:
                self._in_ready_pool = True
            log.info("Joined validator ready pool")
        except (PlatformApiError, _HTTPStatusError) as err:
            status_code = err.status_code if isinstance(err, PlatformApiError) else err.response.status_code
            error_msg = str(err)
            if status_code == 403:
                log.error(
                    "Failed to join validator ready pool (403 Forbidden): %s. "
                    "This may indicate insufficient stake (minimum 10,000 AWP on Mine Worknet, "
                    "may increase as more validators join). Stake must remain allocated "
                    "continuously — withdrawal causes eviction. Will retry on next heartbeat.",
                    error_msg,
                )
            else:
                log.warning("join_ready_pool failed: %s", err)
        except Exception as exc:
            log.warning("join_ready_pool failed: %s", exc)

        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, name="validator-heartbeat", daemon=True
        )
        self._heartbeat_thread.start()

        self._main_thread = threading.Thread(
            target=self._main_loop, name="validator-main", daemon=True
        )
        self._main_thread.start()

        # Auto-updater — pulls from upstream on new commits, triggers graceful stop
        try:
            from auto_updater import AutoUpdater
            from pathlib import Path as _Path
            project_root = _Path(__file__).resolve().parents[1]
            self._auto_updater = AutoUpdater(
                project_root,
                on_update_applied=self._on_auto_update_applied,
            )
            self._auto_updater.start()
        except Exception as exc:
            log.warning("Auto-updater start failed: %s", exc)
            self._auto_updater = None

        self._record_action("started")
        self._write_status()
        return self.status()

    def _on_auto_update_applied(self) -> None:
        """Called by AutoUpdater after a successful pull. Triggers graceful stop."""
        log.info("Auto-update applied; stopping validator for restart")
        self._record_action("auto_update_applied")
        self.stop()

    def stop(self) -> dict[str, Any]:
        """Gracefully stop the validator runtime."""
        with self._lock:
            if not self._running:
                return self.status()
            self._running = False
        self._stop_event.set()

        self._phase = "stopped"
        self._phase_detail = ""
        log.info("ValidatorRuntime stopping")

        # Stop auto-updater thread
        if getattr(self, "_auto_updater", None) is not None:
            try:
                self._auto_updater.stop()
            except Exception:
                pass

        try:
            with self._platform_lock:
                self._platform.leave_ready_pool()
        except Exception as exc:
            log.warning("leave_ready_pool failed: %s", exc)

        self._ws.close()

        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=10)
        if self._main_thread and self._main_thread.is_alive():
            self._main_thread.join(timeout=10)

        self._record_action("stopped")
        self._write_status()
        return self.status()

    def pause(self) -> dict[str, Any]:
        """Pause evaluation processing (heartbeat continues)."""
        with self._lock:
            self._paused = True
        log.info("ValidatorRuntime paused")
        self._record_action("paused")
        self._write_status()
        return self.status()

    def resume(self) -> dict[str, Any]:
        """Resume evaluation processing."""
        with self._lock:
            self._paused = False
        log.info("ValidatorRuntime resumed")
        self._record_action("resumed")
        self._write_status()
        return self.status()

    def status(self) -> dict[str, Any]:
        """Return current runtime status."""
        with self._lock:
            state = "stopped"
            if self._running and self._paused:
                state = "paused"
            elif self._running:
                state = "running"
        return {
            "state": state,
            "validator_id": self._validator_id,
            "ws_connected": self._ws.connected,
            "eligible": self._eligible,
            "uptime_seconds": int(time.monotonic() - self._start_time),
            "stats": self._snapshot_stats(),
            "last_action": self._last_action,
            "last_action_at": self._last_action_at,
            "status_file": str(self._status_file),
            "history_file": str(self._history_file),
            "config_file": str(self._config_file),
        }

    # ------------------------------------------------------------------
    # Internal loops
    # ------------------------------------------------------------------

    def _set_phase(self, phase: str, detail: str = "") -> None:
        """Update the externally visible phase and write status (skip if unchanged)."""
        if self._phase == phase and self._phase_detail == detail:
            return
        self._phase = phase
        self._phase_detail = detail
        self._write_status()

    def _main_loop(self) -> None:
        """WS receive loop with HTTP polling fallback."""
        self._set_phase("waiting_for_task")
        consecutive_ws_failures = 0
        while self._running:
            if not self._ws.connected:
                try:
                    self._ws.reconnect_with_backoff()
                except Exception as exc:
                    log.error("Reconnect error: %s", exc)
                # Update counter based on reconnect result
                if self._ws.connected:
                    consecutive_ws_failures = 0
                else:
                    consecutive_ws_failures += 1
                    # Fall back to HTTP polling after consecutive WS failures
                    if consecutive_ws_failures >= 3:
                        self._poll_evaluation_task_http()
                        consecutive_ws_failures = 3  # cap to prevent overflow, keep polling
                    if self._stop_event.wait(timeout=5):
                        break
                    continue

            try:
                msg = self._ws.receive(timeout=WS_RECEIVE_TIMEOUT)
            except WSDisconnected:
                log.warning("WS disconnected during receive")
                consecutive_ws_failures += 1
                continue

            if msg is None:
                continue

            # Successful receive — reset WS failure counter
            consecutive_ws_failures = 0

            if msg.type == "evaluation_task":
                self._inc_stat("tasks_received")
                with self._lock:
                    eligible = self._eligible
                    paused = self._paused
                if not eligible:
                    log.info("Not eligible — ignoring evaluation_task %s", msg.assignment_id)
                    continue
                if paused:
                    log.info("Paused — ignoring evaluation_task %s", msg.assignment_id)
                    continue
                try:
                    self._set_phase("evaluating", f"task {msg.task_id}")
                    self._handle_evaluation_task(msg)
                except Exception as exc:
                    self._inc_stat("errors")
                    self._inc_stat("consecutive_failures")
                    log.error("Error handling evaluation task %s: %s", msg.assignment_id, exc)
                    self._record_action(f"error: {exc}", {"task_id": msg.task_id})
                    # Alert on consecutive failures (#1)
                    consec = self._get_stat("consecutive_failures")
                    if consec >= FALLBACK_ALERT_THRESHOLD:
                        if consec % FALLBACK_ALERT_THRESHOLD == 0:
                            alert = f"WARNING: {consec} consecutive evaluation failures!"
                            log.warning(alert)
                            self._send_notification(alert)
                finally:
                    self._set_phase("waiting_for_task")

            elif msg.type == "cooldown":
                # Platform signals post-task cooldown with a precise sleep
                # duration — respect it instead of using min_task_interval.
                retry_after = int(msg.data.get("retry_after_seconds", 30))
                cooldown_msg = msg.data.get("message", "")
                log.info("Cooldown received via WS: %ds (%s)", retry_after, cooldown_msg)
                self._record_action(f"cooldown {retry_after}s", {"message": cooldown_msg})
                self._set_phase("cooldown", f"{retry_after}s")
                self._stop_event.wait(timeout=retry_after)
                self._set_phase("waiting_for_task")

            elif msg.type == "error":
                # Platform reports a claim/ack/reject failure or cooldown via
                # WS error message. Log it and handle validator_cooldown by
                # sleeping the precise retry_after_seconds.
                error_code = str(msg.raw.get("code") or "")
                error_msg = str(msg.raw.get("message") or "")
                retry_after = msg.raw.get("retry_after_seconds")
                log.warning("WS error: code=%s message=%s", error_code, error_msg)
                self._record_action(f"ws_error: {error_code}", {"message": error_msg})
                if error_code == "validator_cooldown" and isinstance(retry_after, (int, float)) and retry_after > 0:
                    log.info("Validator cooldown via WS error: sleeping %ds", int(retry_after))
                    self._set_phase("cooldown", f"{int(retry_after)}s")
                    self._stop_event.wait(timeout=int(retry_after))
                    self._set_phase("waiting_for_task")

            else:
                log.debug("Ignoring message type=%s", msg.type)

        log.info("Main loop exited")
        self._write_status()

    def _poll_evaluation_task_http(self) -> None:
        """HTTP polling fallback when WS is unavailable."""
        with self._lock:
            eligible = self._eligible
            paused = self._paused
        if not eligible or paused:
            return
        # Iterative claim loop with PoW retry (max 3 attempts to avoid infinite loop)
        for _attempt in range(3):
            try:
                with self._platform_lock:
                    claim_data = self._platform.claim_evaluation_task()
                if not claim_data:
                    return
                if isinstance(claim_data, dict) and claim_data.get("_cooldown"):
                    retry_after = int(claim_data.get("retry_after_seconds", 30))
                    log.info("Validator cooldown via HTTP: sleeping %ds", retry_after)
                    self._stop_event.wait(timeout=retry_after)
                    return
                if isinstance(claim_data, dict) and claim_data.get("_pow_required"):
                    if self._handle_pow_challenge(claim_data):
                        log.info("PoW passed — retrying claim")
                        continue  # retry claim in next loop iteration
                    return  # PoW failed — next claim will return 409 cooldown
                # Normal claim success
                msg = WSMessage({"type": "evaluation_task", "data": claim_data})
                self._inc_stat("tasks_received")
                try:
                    self._handle_evaluation_task(msg, via_http=True)
                except Exception as eval_exc:
                    self._inc_stat("errors")
                    self._inc_stat("consecutive_failures")
                    log.error("HTTP fallback eval failed: %s", eval_exc)
                    self._write_status()
                return
            except Exception as exc:
                error_str = str(exc)
                if "404" not in error_str and "409" not in error_str:
                    log.warning("HTTP poll claim failed: %s", exc)
                return
        log.warning("HTTP poll: PoW retry limit reached (3 attempts) without claiming a task")

    def _handle_evaluation_task(self, msg: WSMessage, *, via_http: bool = False) -> None:
        """Process a single evaluation task.

        Per API spec, the complete flow is:
          1. WS push: {"type":"evaluation_task","data":{"task_id":"evt_xxx"}} (task_id only)
          2. HTTP POST /evaluation-tasks/claim → returns assignment_id + full data
          3. Evaluate
          4. HTTP POST /evaluation-tasks/{id}/report with assignment_id

        The assignment_id comes from the claim response, NOT from WS push.
        WS ack_eval is no longer required — claim via HTTP is sufficient.

        HTTP polling flow (WS unavailable):
          1. HTTP POST /evaluation-tasks/claim → directly returns full data
          2. Evaluate + report
        """
        task_id = msg.task_id

        # HTTP POST /evaluation-tasks/claim to get assignment_id + full data.
        # WS push only carries task_id. The claim response is the authoritative
        # source for assignment_id (REQUIRED in report) and all evaluation data.
        if via_http:
            # HTTP polling path — msg.data already contains full claim response
            claim_data = msg.data
        else:
            # WS path — must call HTTP claim to get assignment_id + data
            try:
                with self._platform_lock:
                    claim_data = self._platform.claim_evaluation_task()
                if not claim_data:
                    log.warning("Claim returned no data for task %s", task_id)
                    return
                if claim_data.get("_cooldown"):
                    retry_after = int(claim_data.get("retry_after_seconds", 30))
                    log.info("Validator cooldown on WS claim: sleeping %ds", retry_after)
                    self._stop_event.wait(timeout=retry_after)
                    return
                # 428 PoW required — solve and retry claim
                if claim_data.get("_pow_required"):
                    if self._handle_pow_challenge(claim_data):
                        log.info("PoW passed — retrying claim for task %s", task_id)
                        with self._platform_lock:
                            claim_data = self._platform.claim_evaluation_task()
                        if not claim_data:
                            log.info("No task available after PoW pass")
                            return
                        if claim_data.get("_cooldown"):
                            retry = int(claim_data.get("retry_after_seconds", 30))
                            log.info("Cooldown after PoW retry: %ds", retry)
                            self._stop_event.wait(timeout=retry)
                            return
                        if claim_data.get("_pow_required"):
                            log.warning("Second PoW challenge on retry — dropping")
                            return
                    else:
                        return
            except Exception as exc:
                log.warning("HTTP claim for task %s failed: %s", task_id, exc)
                return

        assignment_id = str(claim_data.get("assignment_id") or "")
        task_id = str(claim_data.get("task_id") or task_id)
        dataset_id = str(claim_data.get("dataset_id") or "")
        cleaned_data = str(claim_data.get("cleaned_data") or "")
        repeat_cleaned_data = str(claim_data.get("repeat_cleaned_data") or "")
        structured_data = claim_data.get("structured_data") or {}
        schema_fields = claim_data.get("schema_fields") or []
        dataset_schema = claim_data.get("dataset_schema") or {}

        if not assignment_id:
            log.warning("No assignment_id from claim for task %s — cannot report", task_id)
            return

        log.info("Task claimed: task=%s assignment=%s dataset=%s", task_id, assignment_id, dataset_id)

        if not isinstance(structured_data, dict):
            structured_data = {}
        if not isinstance(schema_fields, list):
            schema_fields = list(schema_fields) if schema_fields else []
        if not isinstance(dataset_schema, dict):
            dataset_schema = {}

        # Step 3: Evaluate (M0 vs M1 comparison + quality scoring)
        eval_result: EvaluationResult = self._engine.evaluate(
            cleaned_data, structured_data, schema_fields,
            repeat_cleaned_data=repeat_cleaned_data,
            dataset_schema=dataset_schema,
        )
        self._inc_stat("tasks_evaluated")

        # Step 4: Report with result (match/mismatch) and score
        with self._platform_lock:
            self._platform.report_evaluation(
                task_id, eval_result.score,
                assignment_id=assignment_id,
                result=eval_result.result,
            )

        # Reset consecutive failures on success (#1)
        self._set_stat("consecutive_failures", 0)

        if eval_result.result == "match":
            self._inc_stat("tasks_match")
            action = f"match score={eval_result.score} task={task_id}"
            log.info("Evaluation reported to platform: %s", action)
        else:
            self._inc_stat("tasks_mismatch")
            action = f"mismatch task={task_id}"
            log.info("Evaluation reported to platform: %s", action)

        self._record_action(action, {
            "type": "evaluation",
            "task_id": task_id,
            "assignment_id": assignment_id,
            "result": eval_result.result,
            "score": eval_result.score,
        })
        self._log_history({
            "type": "evaluation",
            "task_id": task_id,
            "assignment_id": assignment_id,
            "dataset_id": dataset_id,
            "result": eval_result.result,
            "score": eval_result.score,
        })
        self._write_status()

        # Step 5: Wait min_task_interval before accepting next task.
        # The platform may also send a WS "cooldown" message with a precise
        # retry_after_seconds — that is handled in _main_loop and overrides
        # this local interval. This sleep is the fallback when no WS cooldown
        # message arrives.
        with self._lock:
            wait_seconds = self._min_task_interval
        if wait_seconds > 0:
            log.info("Waiting %ds (min_task_interval) before next task", wait_seconds)
            self._set_phase("cooldown", f"{wait_seconds}s (min_task_interval)")
            self._stop_event.wait(timeout=wait_seconds)

    # ------------------------------------------------------------------
    # PoW (Proof of Work) challenge handling
    # ------------------------------------------------------------------

    def _handle_pow_challenge(self, pow_data: dict[str, Any]) -> bool:
        """Handle a 428 PoW challenge. Returns True if passed.

        Per doc: after passing, caller should retry claim immediately.
        After failing, next claim returns 409 cooldown (handled upstream).
        """
        challenge = pow_data.get("challenge") or {}
        challenge_id = str(challenge.get("id") or pow_data.get("challenge_id") or "")
        prompt = str(challenge.get("prompt") or "")

        if not challenge_id or not prompt:
            log.warning("PoW challenge missing id or prompt: %s", pow_data)
            return False

        log.info("PoW challenge: id=%s type=%s", challenge_id, challenge.get("question_type"))
        self._record_action("pow_challenge", {"challenge_id": challenge_id})
        self._set_phase("solving_pow", f"challenge {challenge_id}")

        answer = self._solve_logic_puzzle(prompt)
        if not answer:
            log.warning("Failed to solve PoW puzzle")
            self._record_action("pow_failed", {"challenge_id": challenge_id, "reason": "no_answer"})
            self._set_phase("waiting_for_task")
            return False

        log.info("Submitting PoW answer: %s", answer)
        try:
            with self._platform_lock:
                result = self._platform.answer_pow_challenge(challenge_id, answer)
            data = result.get("data") if isinstance(result, dict) else {}
            passed = data.get("passed", False) if isinstance(data, dict) else False

            if passed:
                log.info("PoW challenge passed!")
                self._record_action("pow_passed", {"challenge_id": challenge_id})
                self._set_phase("waiting_for_task")
                return True
            else:
                log.warning("PoW challenge failed (wrong answer)")
                self._record_action("pow_failed", {"challenge_id": challenge_id, "reason": "wrong_answer"})
                self._inc_stat("errors")
                self._set_phase("waiting_for_task")
                return False
        except Exception as exc:
            log.error("PoW answer submit failed: %s", exc)
            self._record_action("pow_error", {"challenge_id": challenge_id, "error": str(exc)})
            self._inc_stat("errors")
            self._set_phase("waiting_for_task")
            return False

    def _solve_logic_puzzle(self, prompt: str) -> str:
        """Use the LLM to solve a logic grid puzzle.

        Calls enrich_with_llm directly (not via self._engine.llm_call) so the
        system_prompt is passed as a separate role — important for models that
        distinguish system vs user turns.

        Returns ONLY the answer value (e.g. "bird", "coffee", "red").
        """
        import asyncio
        try:
            from crawler.enrich.generative.llm_enrich import enrich_with_llm

            system_prompt = (
                "You are solving a logic puzzle. Read the clues carefully, "
                "use elimination to deduce the answer. "
                "Respond with ONLY the answer value (a single word), nothing else. "
                "No explanation, no punctuation."
            )

            async def _run() -> str:
                result = await enrich_with_llm(
                    prompt,
                    model_config=getattr(self._engine, "model_config", None),
                    system_prompt=system_prompt,
                    timeout=60.0,
                )
                if not result.success:
                    raise RuntimeError(result.error or "LLM call failed")
                return result.content

            from evaluation_engine import _LLM_EXECUTOR
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    raw = _LLM_EXECUTOR.submit(lambda: asyncio.run(_run())).result()
                else:
                    raw = asyncio.run(_run())
            except RuntimeError:
                raw = asyncio.run(_run())

            if raw:
                raw = raw.strip().split("\n")[0].strip().strip(".").strip('"').strip("'").lower()
            return raw or ""
        except Exception as exc:
            log.error("LLM solve failed: %s", exc)
            return ""

    def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats to the platform.

        WS 连接时降频发 HTTP 心跳（每 5 个周期发一次）——仍需从
        heartbeat 响应更新 _eligible 和 _min_task_interval。
        完全跳过会导致这些状态永久过期。
        """
        ws_skip_counter = 0
        while self._running:
            if self._ws.connected:
                ws_skip_counter += 1
                # WS 连接时每 5 个心跳周期发一次 HTTP 心跳刷新状态
                if ws_skip_counter >= 5:
                    self._send_heartbeat()
                    ws_skip_counter = 0
            else:
                self._send_heartbeat()
                ws_skip_counter = 0
            # Retry joining ready pool if not yet in it
            with self._lock:
                in_pool = self._in_ready_pool
            if not in_pool:
                try:
                    with self._platform_lock:
                        self._platform.join_ready_pool()
                    with self._lock:
                        self._in_ready_pool = True
                    log.info("Successfully joined ready pool on retry")
                except Exception as exc:
                    log.warning("Ready pool retry failed: %s", exc)
            self._write_status()
            if self._stop_event.wait(timeout=self._heartbeat_interval):
                break
        log.info("Heartbeat loop exited")

    def _send_heartbeat(self) -> None:
        """Send a single heartbeat and update runtime state from response."""
        try:
            with self._platform_lock:
                resp = self._platform.send_unified_heartbeat(client_name=f"validator-{self._validator_id}")
            data = resp.get("data") if isinstance(resp, dict) else None
            if isinstance(data, dict):
                validator_info = data.get("validator")
                if isinstance(validator_info, dict):
                    with self._lock:
                        self._eligible = validator_info.get("eligible", True)
                        interval = validator_info.get("min_task_interval_seconds")
                        if isinstance(interval, (int, float)) and interval > 0:
                            self._min_task_interval = int(interval)
                    if not self._eligible:
                        log.warning("Validator not eligible (evicted or suspended)")
            log.debug("Heartbeat sent")
        except Exception as exc:
            log.warning("Heartbeat failed: %s", exc)

