from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx

if TYPE_CHECKING:
    from signer import WalletSigner

from auth_orchestrator import AUTH_ERROR_CODES, AuthOrchestrator
try:
    from canonicalize import normalize_url
except ImportError:
    # Fallback if PYTHONPATH resolves lib/canonicalize instead of scripts/canonicalize
    from lib.canonicalize import canonicalize_url as _canon
    def normalize_url(url: str, regex_pattern: str | None = None) -> str:  # type: ignore[misc]
        return _canon(url)
from common import (
    DEFAULT_EIP712_CHAIN_ID,
    DEFAULT_EIP712_DOMAIN_NAME,
    DEFAULT_EIP712_VERIFYING_CONTRACT,
    WALLET_SESSION_DURATION_SECONDS,
    WALLET_SESSION_RENEW_THRESHOLD_SECONDS,
    inject_crawler_root,
    resolve_awp_registration,
    resolve_miner_id,
    resolve_platform_base_url,
    resolve_signature_config,
    resolve_wallet_config,
    resolve_ws_url,
)
from crawl_mode_planner import CrawlModePlanner
from lib.platform_client import PlatformApiError, PlatformClient
from mine_gateway import resolve_mine_gateway_model_config, write_model_config
from pow_solver import UnsupportedChallenge, solve_challenge
from run_artifacts import RunArtifactWriter
from run_models import CrawlerRunResult, WorkItem, WorkerConfig, WorkerIterationSummary
from task_sources import (
    BackendClaimSource,
    DatasetDiscoverySource,
    ResumeQueueSource,
    SkipClaimedTask,
    build_follow_up_items_from_discovery,
    build_report_payload,
    claimed_task_from_payload,
    local_task_from_payload,
    optional_string,
    task_to_work_item,
)
from worker_state import WorkerStateStore

CRAWLER_ROOT = inject_crawler_root()

from crawler.output import read_json_file, read_jsonl_file  # noqa: E402
from crawler.submission_export import build_submission_request  # noqa: E402


class SkipItemError(RuntimeError):
    pass


class CrawlerRunner:
    def __init__(self, config: WorkerConfig) -> None:
        self.config = config
        self.output_root = config.output_root
        self.default_backend = config.default_backend

    # repeat_crawl only does fetch+extract (no enrich). 180s covers the
    # slowest path (Amazon Playwright: browser launch + page load + wait
    # strategy ≈ 60-90s) with headroom for network jitter, while staying
    # well within the platform's 6-minute reporting deadline.
    REPEAT_CRAWL_TIMEOUT = 180

    # Sentinel value for --field-group to skip enrichment entirely.
    SKIP_ENRICH_SENTINEL = "none"

    # Cached openclaw CLI availability — PATH doesn't change mid-run.
    _openclaw_available: bool | None = None

    def run_item(self, item: WorkItem, command: str) -> CrawlerRunResult:
        output_dir = resolve_item_output_dir(item, output_root=self.output_root)
        output_dir.mkdir(parents=True, exist_ok=True)
        input_path = output_dir / "task-input.jsonl"
        input_path.write_text(json.dumps(item.record, ensure_ascii=False) + "\n", encoding="utf-8")
        argv = [self.config.python_bin, "-m", "crawler", command, "--input", str(input_path), "--output", str(output_dir), "--auto-login"]
        self._append_enrich_argv(argv, command=command, output_dir=output_dir)
        if item.resume:
            argv.append("--resume")
        if command == "discover-crawl":
            argv.extend(["--max-depth", str(self.config.discovery_max_depth), "--max-pages", str(self.config.discovery_max_pages)])
        if self.default_backend:
            argv.extend(["--preferred-backend", self.default_backend])
        # repeat_crawl = fetch+extract only (no enrich), so use a short timeout
        timeout = (
            self.REPEAT_CRAWL_TIMEOUT
            if item.claim_task_type == "repeat_crawl"
            else getattr(self.config, "crawl_timeout_seconds", 600)
        )
        try:
            completed = subprocess.run(
                argv,
                cwd=self.config.crawler_root,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            raise SkipItemError(f"crawler subprocess timed out for {item.url}")
        records_path = output_dir / "records.jsonl"
        errors_path = output_dir / "errors.jsonl"
        records = read_jsonl_file(records_path) if records_path.exists() else []
        errors = read_jsonl_file(errors_path) if errors_path.exists() else []
        summary_path = output_dir / "summary.json"
        summary = read_json_file(summary_path) if summary_path.exists() else {}
        if not isinstance(summary, dict):
            summary = {}
        return CrawlerRunResult(
            output_dir=output_dir,
            records=records,
            errors=errors,
            summary=summary,
            exit_code=completed.returncode,
            argv=argv,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )

    def _append_enrich_argv(self, argv: list[str], *, command: str, output_dir: Path) -> None:
        """Attach LLM enrich args for run/enrich: prefer OpenClaw CLI, then gateway config.

        When no LLM backend is available (no openclaw, no gateway config),
        enrichment is explicitly skipped via ``--field-group none``. Without
        this the crawler falls through to the platform's full enrich plan
        (e.g. arXiv has 42 field groups, many requiring LLM calls). Each
        group times out individually → the subprocess blocks for many
        minutes and appears hung.  Skipping enrichment is safe: the base
        structured_data (title, abstract, authors, etc.) is already extracted
        and sufficient for platform submission and scoring.
        """
        if command not in {"run", "enrich"}:
            return
        if os.environ.get("MINE_SKIP_ENRICH", "").strip() == "1":
            argv.extend(["--field-group", self.SKIP_ENRICH_SENTINEL])
            return
        # Cache openclaw CLI check — PATH doesn't change mid-run
        if CrawlerRunner._openclaw_available is None:
            from crawler.enrich.generative.openclaw_agent import openclaw_cli_available
            CrawlerRunner._openclaw_available = openclaw_cli_available()
        if CrawlerRunner._openclaw_available:
            argv.append("--use-openclaw")
            return
        if self.config.gateway_model_config:
            config_path = write_model_config(
                output_dir / "_runtime" / "mine-model-config.json",
                self.config.gateway_model_config,
            )
            argv.extend(["--model-config", str(config_path)])
            return
        argv.extend(["--field-group", self.SKIP_ENRICH_SENTINEL])


def _build_test_config(root: Path) -> WorkerConfig:
    return WorkerConfig(
        base_url="http://example.test",
        token="",
        miner_id="miner-test",
        output_root=root / "outputs",
        crawler_root=CRAWLER_ROOT,
        python_bin="python",
        state_root=root / "state",
        gateway_model_config={},
    )


def resolve_item_output_dir(item: WorkItem, *, output_root: Path) -> Path:
    return Path(item.output_dir) if item.output_dir else (output_root / item.source / _safe_path_segment(item.item_id))


class AgentWorker:
    def __init__(
        self,
        *,
        client: PlatformClient,
        runner: CrawlerRunner,
        config: WorkerConfig,
        ws_client: Any | None = None,
    ) -> None:
        self.client = client
        self.runner = runner
        self.config = config
        self.state_store = WorkerStateStore(config.state_root)
        self.resume_source = ResumeQueueSource(self.state_store)
        self.backend_source = BackendClaimSource(self.client)
        self.ws_source: Any | None = None
        if ws_client is not None:
            from task_sources import WebSocketClaimSource
            self.ws_source = WebSocketClaimSource(ws_client)
        self.dataset_source = DatasetDiscoverySource(self.client, self.state_store)
        self.crawl_mode_planner = CrawlModePlanner()
        self.auth_orchestrator = AuthOrchestrator(
            self.state_store,
            retry_after_seconds=config.auth_retry_interval_seconds,
        )
        # ── Submit queue: crawl threads produce, submit thread consumes ──
        # Separating crawl (parallel) from submit (sequential) prevents
        # multiple threads from hitting 429 simultaneously. The submit
        # thread handles rate limiting with backoff.
        import queue as _queue_mod
        self._submit_queue: _queue_mod.Queue[tuple[WorkItem, dict[str, Any], dict[str, Any] | None] | None] = _queue_mod.Queue()
        self._submit_thread: threading.Thread | None = None
        self._submit_stop = threading.Event()
        # Auto-updater — polls upstream every 10min, pulls + requests stop on update
        self._auto_updater: Any = None
        # WS heartbeat skip counter — send HTTP heartbeat every 5th iteration
        # when WS is connected to keep session state (credit_score, epoch etc.)
        self._ws_heartbeat_skip = 0

        self._submit_stats_lock = threading.Lock()
        self._submit_stats = {"submitted": 0, "deferred": 0, "discarded": 0}
        # Track last-seen submit count so each iteration can report the delta.
        # Without this, the async submit path never updates session_totals
        # and status keeps showing submitted=0 despite successful submissions.
        self._submit_stats_baseline = {"submitted": 0, "discarded": 0}
        self._submit_retries: dict[str, int] = {}  # item_id → retry count
        self._MAX_SUBMIT_RETRIES = 5

        # Dedicated repeat_crawl processing thread — runs independently
        # from the main iteration loop so a slow discovery task can never
        # block a repeat_crawl with a 6-minute platform deadline.
        self._repeat_queue: list[WorkItem] = []
        self._repeat_lock = threading.Lock()
        self._repeat_thread: threading.Thread | None = None
        self._repeat_stop = threading.Event()

        seed: dict[str, Any] = {}
        token_expires_at = os.environ.get("AWP_WALLET_TOKEN_EXPIRES_AT", "").strip()
        if token_expires_at.isdigit():
            seed["token_expires_at"] = int(token_expires_at)
        self.state_store.save_session(seed)

    def start_working(self, *, selected_dataset_ids: list[str] | None = None) -> dict[str, Any]:
        datasets = self.client.list_datasets()
        dataset_ids = [str(dataset.get("dataset_id") or dataset.get("id") or "").strip() for dataset in datasets if str(dataset.get("dataset_id") or dataset.get("id") or "").strip()]
        session = self.state_store.load_session()
        current_selected = [str(dataset_id) for dataset_id in (session.get("selected_dataset_ids") or []) if str(dataset_id).strip()]
        requested_selected = (
            [dataset_id for dataset_id in (selected_dataset_ids or []) if dataset_id in dataset_ids]
            if selected_dataset_ids is not None
            else current_selected
        )

        unified_ok = False
        miner_ok = False
        heartbeat_errors: list[str] = []
        try:
            unified = self.client.send_unified_heartbeat(client_name=self.config.client_name)
            unified_ok = True
            self._update_session_from_heartbeat(unified)
        except Exception as exc:
            heartbeat_errors.append(f"unified heartbeat failed: {exc}")
        try:
            self.client.send_miner_heartbeat(client_name=self.config.client_name)
            miner_ok = True
            self.state_store.save_session({"last_heartbeat_at": int(time.time())})
        except Exception as exc:
            heartbeat_errors.append(f"miner heartbeat failed: {exc}")

        if not requested_selected and len(dataset_ids) == 1:
            requested_selected = [dataset_ids[0]]

        if not requested_selected and len(dataset_ids) > 1:
            self.state_store.save_session({
                "mining_state": "idle",
                "last_control_action": "start-working",
                "last_state_change_at": int(time.time()),
            })
            return {
                "mining_state": "idle",
                "selection_required": True,
                "selected_dataset_ids": [],
                "datasets": self.list_datasets()["datasets"],
                "heartbeat": {
                    "unified_ok": unified_ok,
                    "miner_ok": miner_ok,
                    "errors": heartbeat_errors,
                },
                "status": self.check_status(),
                "message": "dataset selection required",
            }

        # Start WebSocket receive thread for push-based task claiming
        if self.ws_source is not None:
            try:
                self.ws_source.start()
            except Exception as exc:
                heartbeat_errors.append(f"ws start failed (falling back to HTTP polling): {exc}")

        # Start dedicated repeat_crawl processing thread
        self._start_repeat_crawl_thread()

        # Start submit thread — sequential submission with rate limit backoff
        self._start_submit_thread()

        # Start auto-update thread — pulls from upstream and signals stop on update
        self._start_auto_updater()

        session_update: dict[str, Any] = {
            "mining_state": "running",
            "selected_dataset_ids": requested_selected,
            "last_control_action": "start-working",
            "last_state_change_at": int(time.time()),
            "run_started_at": int(time.time()),
            "stop_reason": None,
        }
        session = self.state_store.save_session(session_update)
        return {
            "mining_state": session["mining_state"],
            "selection_required": False,
            "selected_dataset_ids": list(session.get("selected_dataset_ids") or []),
            "datasets": self.list_datasets()["datasets"],
            "heartbeat": {
                "unified_ok": unified_ok,
                "miner_ok": miner_ok,
                "errors": heartbeat_errors,
            },
            "status": self.check_status(),
            "message": "start working confirmed",
        }

    def list_datasets(self) -> dict[str, Any]:
        session = self.state_store.load_session()
        datasets = self.client.list_datasets()
        selected = set(session.get("selected_dataset_ids") or [])
        cooldowns = self.state_store.active_dataset_cooldowns()
        annotated: list[dict[str, Any]] = []
        for dataset in datasets:
            dataset_id = optional_string(dataset.get("dataset_id")) or optional_string(dataset.get("id")) or ""
            entry = dict(dataset)
            entry["selected"] = dataset_id in selected if dataset_id else False
            if dataset_id in cooldowns:
                entry["cooldown"] = cooldowns[dataset_id]
            annotated.append(entry)
        return {"datasets": annotated, "selected_dataset_ids": list(selected)}

    def pause(self) -> dict[str, Any]:
        session = self.state_store.save_session({
            "mining_state": "paused",
            "last_control_action": "pause",
            "last_state_change_at": int(time.time()),
        })
        return self.check_status() | {"message": "Mining paused.", "mining_state": session["mining_state"]}

    def resume(self) -> dict[str, Any]:
        session = self.state_store.save_session({
            "mining_state": "running",
            "last_control_action": "resume",
            "last_state_change_at": int(time.time()),
            "run_started_at": int(time.time()),
            "stop_reason": None,
        })
        return self.check_status() | {"message": "Mining resumed.", "mining_state": session["mining_state"]}

    def stop(self) -> dict[str, Any]:
        self._stop_auto_updater()
        self._stop_submit_thread()
        self._stop_repeat_crawl_thread()
        if self.ws_source is not None:
            self.ws_source.stop()
        session = self.state_store.save_session({
            "mining_state": "stopped",
            "last_control_action": "stop",
            "last_state_change_at": int(time.time()),
        })
        return self.check_status() | {"message": "Mining session ended.", "mining_state": session["mining_state"]}

    def run_once(self) -> str:
        summary = self.run_iteration(1)
        if summary["errors"]:
            return "; ".join(summary["errors"])
        if summary["messages"]:
            return "; ".join(summary["messages"])
        if summary["auth_pending"]:
            first = summary["auth_pending"][0]
            return json.dumps(first, ensure_ascii=False)
        return "no task available"

    def check_status(self) -> dict[str, Any]:
        session = self.state_store.load_session()
        # Enrich with unified profile from platform (replaces multiple API calls)
        try:
            signer_addr = self.client.get_signer_address()
            if signer_addr:
                profile = self.client.fetch_profile(signer_addr)
                if profile:
                    session["profile"] = profile
                    # Extract miner stats from profile
                    miner_info = profile.get("miner") or {}
                    if miner_info:
                        session["credit_score"] = miner_info.get("credit", session.get("credit_score"))
                        session["credit_tier"] = miner_info.get("credit_tier", session.get("credit_tier"))
                    miner_summary = profile.get("miner_summary") or {}
                    if miner_summary:
                        session["miner_stats"] = miner_summary
                    current_epoch = profile.get("current_epoch") or {}
                    if current_epoch:
                        session["current_epoch"] = current_epoch
                        epoch_id = current_epoch.get("epoch_id")
                        if epoch_id:
                            session["epoch_id"] = epoch_id
                        epoch_miner = current_epoch.get("miner") or {}
                        if epoch_miner:
                            # Only advance epoch_submitted, never regress (profile can lag behind heartbeat)
                            profile_count = epoch_miner.get("task_count")
                            if profile_count is not None:
                                session["epoch_submitted"] = max(int(profile_count), int(session.get("epoch_submitted") or 0))
                            session["epoch_avg_score"] = epoch_miner.get("avg_score")
        except Exception:
            pass
        # Fallback: try older APIs if profile didn't populate
        if "miner_stats" not in session:
            try:
                my_stats = self.client.fetch_my_miner_stats()
                if my_stats:
                    session["miner_stats"] = my_stats
            except Exception:
                pass
        if "current_epoch" not in session:
            try:
                current_epoch = self.client.fetch_current_epoch()
                if current_epoch:
                    session["current_epoch"] = current_epoch
                    if not session.get("epoch_id"):
                        session["epoch_id"] = current_epoch.get("epoch_id")
            except Exception:
                pass
        epoch_submitted = int(session.get("epoch_submitted") or 0)
        epoch_target = int(session.get("epoch_target") or 80)
        epoch_remaining = max(0, epoch_target - epoch_submitted)
        epoch_completion_percent = 0.0 if epoch_target <= 0 else round(min(100.0, (epoch_submitted / epoch_target) * 100), 2)
        session_totals = dict(session.get("session_totals") or {})
        reward = self._reward_status_from_session(session)
        credit = self._credit_status_from_session(session)
        current_batch = self.state_store.get_current_batch()
        if not isinstance(current_batch, dict) or not current_batch:
            current_batch = dict(session.get("current_batch") or {}) if isinstance(session.get("current_batch"), dict) else {}

        # Calculate estimated completion time
        estimated_completion = self._estimate_completion_time(session, session_totals, epoch_remaining)

        # Build earnings summary for easy consumption
        earnings_summary = {
            "epoch_id": session.get("epoch_id"),
            "submitted": epoch_submitted,
            "target": epoch_target,
            "progress_percent": epoch_completion_percent,
            "remaining": epoch_remaining,
            "credit_score": session.get("credit_score"),
            "credit_tier": session.get("credit_tier"),
            "estimated_completion": estimated_completion,
        }

        return {
            "mining_state": session.get("mining_state", "idle"),
            "credit_score": session.get("credit_score"),
            "credit_tier": session.get("credit_tier"),
            "credit": credit,
            "epoch_id": session.get("epoch_id"),
            "epoch_submitted": epoch_submitted,
            "epoch_target": epoch_target,
            "selected_dataset_ids": list(session.get("selected_dataset_ids") or []),
            "reward": reward,
            "settlement": dict(session.get("settlement") or {}),
            "phase": self._resolve_phase(session=session, current_batch=current_batch, reward=reward),
            "current_batch": current_batch,
            "last_control_action": session.get("last_control_action"),
            "last_state_change_at": session.get("last_state_change_at"),
            "last_activity_at": session.get("last_activity_at"),
            "last_iteration": session.get("last_iteration", 0),
            "last_wait_seconds": session.get("last_wait_seconds", 0),
            "stop_reason": optional_string(session.get("stop_reason")),
            "queues": {
                "backlog": len(self.state_store.load_backlog()),
                "auth_pending": len(self.state_store.load_auth_pending()),
                "submit_pending": len(self.state_store.load_submit_pending()),
            },
            "cooldowns": self.state_store.active_dataset_cooldowns(),
            "last_summary": dict(session.get("last_summary") or {}),
            "session_totals": session_totals,
            "earnings_summary": earnings_summary,
            "progress": {
                "epoch_completion_percent": epoch_completion_percent,
                "epoch_remaining": epoch_remaining,
                "estimated_completion": estimated_completion,
                "session_processed_items": int(session_totals.get("processed_items") or 0),
                "session_submitted_items": int(session_totals.get("submitted_items") or 0),
                "session_failed_items": int(session_totals.get("failed_items") or 0),
            },
        }

    def _estimate_completion_time(
        self,
        session: dict[str, Any],
        session_totals: dict[str, Any],
        epoch_remaining: int,
    ) -> str | None:
        """Estimate time to reach epoch target based on current submission rate."""
        if epoch_remaining <= 0:
            return "complete"

        run_started_at = session.get("run_started_at")
        if not isinstance(run_started_at, int):
            return None

        submitted_in_session = int(session_totals.get("submitted_items") or 0)
        if submitted_in_session <= 0:
            return None

        elapsed_seconds = max(1, int(time.time()) - run_started_at)
        rate_per_second = submitted_in_session / elapsed_seconds

        if rate_per_second <= 0:
            return None

        remaining_seconds = int(epoch_remaining / rate_per_second)

        # Format as human-readable duration
        if remaining_seconds < 60:
            return f"{remaining_seconds}s"
        elif remaining_seconds < 3600:
            minutes = remaining_seconds // 60
            return f"{minutes}m"
        else:
            hours = remaining_seconds // 3600
            minutes = (remaining_seconds % 3600) // 60
            return f"{hours}h {minutes}m"

    def process_task_payload(self, task_type: str, payload: dict[str, Any]) -> str:
        item = self._work_item_from_payload(task_type, payload)
        summary = WorkerIterationSummary(iteration=1)
        self._process_items([item], summary)
        result = summary.to_dict()
        if result["messages"]:
            return "; ".join(result["messages"])
        if result["errors"]:
            return "; ".join(result["errors"])
        return json.dumps(result, ensure_ascii=False)

    def _work_item_from_payload(self, task_type: str, payload: dict[str, Any]) -> WorkItem:
        if task_type.startswith("local_") or task_type == "local_file":
            return task_to_work_item(local_task_from_payload({"task_type": task_type, **payload}))
        try:
            return task_to_work_item(claimed_task_from_payload(task_type, payload, client=self.client))
        except SkipClaimedTask as exc:
            raise ValueError(str(exc)) from exc

    def run_iteration(self, iteration: int) -> dict[str, Any]:
        summary = WorkerIterationSummary(iteration=iteration)
        session = self.state_store.load_session()
        mining_state = str(session.get("mining_state") or "idle")
        if mining_state in {"paused", "stopped"}:
            summary.messages.append(f"worker {mining_state}")
            batch_state = self._save_current_batch(iteration=iteration, items=[], state=mining_state, summary=summary)
            return self._finalize_iteration(summary, current_batch=batch_state)
        self._send_heartbeats(summary)
        stop_reason = self._active_stop_reason()
        if stop_reason:
            summary.messages.append(f"stop condition reached: {stop_reason}")
            self._mark_stop_condition(stop_reason)
            batch_state = self._save_current_batch(iteration=iteration, items=[], state="settled", summary=summary, stop_reason=stop_reason)
            return self._finalize_iteration(summary, current_batch=batch_state, stop_reason=stop_reason)
        self._drain_submit_pending(summary)
        work_items = self._collect_work_items(summary)
        if not work_items:
            summary.messages.append("no task available")
            summary.retry_pending = len(self.state_store.load_backlog()) + len(self.state_store.load_auth_pending())
            batch_state = self._save_current_batch(iteration=iteration, items=[], state="idle", summary=summary)
            return self._finalize_iteration(summary, current_batch=batch_state)
        self._save_current_batch(iteration=iteration, items=work_items, state="running")
        self._process_items(work_items, summary)
        summary.retry_pending = len(self.state_store.load_backlog()) + len(self.state_store.load_auth_pending())
        summary_payload = summary.to_dict()
        stop_reason = self._active_stop_reason(payload=summary_payload)
        if stop_reason:
            summary.messages.append(f"current batch settled; stop condition reached: {stop_reason}")
            self._mark_stop_condition(stop_reason)
        batch_state = self._save_current_batch(
            iteration=iteration,
            items=work_items,
            state="settled",
            summary=summary,
            stop_reason=stop_reason,
        )
        return self._finalize_iteration(summary, current_batch=batch_state, stop_reason=stop_reason)

    def run_loop(self, *, interval: int = 60, max_iterations: int = 0) -> str:
        """Run continuous mining loop.

        Args:
            interval: Seconds between iterations (default 60)
            max_iterations: Stop after N iterations, 0 = infinite
        """
        iteration = 0
        consecutive_empty = 0
        wait = interval
        while max_iterations == 0 or iteration < max_iterations:
            iteration += 1
            self._proactive_session_renew()
            try:
                summary = self.run_iteration(iteration)
                result = json.dumps(summary, ensure_ascii=False)
                current_state = str(self.state_store.load_session().get("mining_state") or "idle")
                if current_state == "stopped":
                    print(f"[worker] stopped after {iteration} iterations")
                    return f"stopped after {iteration} iterations"
                if not summary["processed_items"] and not summary["discovery_items"] and not summary["claimed_items"] and not summary["resumed_items"]:
                    consecutive_empty += 1
                    wait = min(interval * (2 ** min(consecutive_empty, 3)), 300)
                else:
                    consecutive_empty = 0
                    wait = interval
                self.state_store.save_session({"last_wait_seconds": wait})
                print(f"[worker] iteration {iteration}: {result}")
            except KeyboardInterrupt:
                print(f"[worker] stopped after {iteration} iterations")
                return f"stopped after {iteration} iterations"
            except Exception as e:
                print(f"[worker] iteration {iteration} error: {e}")
                # Don't reset consecutive_empty — let back-off accumulate on errors
                consecutive_empty += 1
                wait = min(interval * (2 ** min(consecutive_empty, 4)), 300)
                self.state_store.save_session({"last_wait_seconds": wait})

            if max_iterations != 0 and iteration >= max_iterations:
                break

            try:
                time.sleep(wait)
            except KeyboardInterrupt:
                print(f"[worker] stopped after {iteration} iterations")
                return f"stopped after {iteration} iterations"

        self.stop()
        return f"completed {iteration} iterations"

    def run_worker(self, *, interval: int = 60, max_iterations: int = 1) -> dict[str, Any]:
        """Run the mining iteration loop.

        With ``max_iterations=0`` this is the long-lived background worker
        path — it never returns until the session is marked ``stopped``. The
        per-iteration print/log output is what makes the session log file
        non-empty; without it the log stays at 0 bytes and users think the
        worker is hung.
        """
        log = logging.getLogger("agent.worker")
        log.info(
            "worker starting: interval=%ds max_iterations=%s",
            interval,
            "infinite" if max_iterations == 0 else max_iterations,
        )
        iterations: list[dict[str, Any]] = []
        iteration = 0
        consecutive_empty = 0
        while max_iterations == 0 or iteration < max_iterations:
            iteration += 1
            self._proactive_session_renew()
            log.info("iteration %d: starting", iteration)
            summary = self.run_iteration(iteration)
            iterations.append(summary)

            # Summary line — always emitted so the log file has a heartbeat
            # even when there's no work. This is what proves the worker is
            # alive and pulling; without it a user sees 0 bytes and assumes
            # the process is stuck.
            processed = summary.get("processed_items", 0)
            submitted = summary.get("submitted_items", 0)
            discovered = summary.get("discovery_items", 0)
            claimed = summary.get("claimed_items", 0)
            resumed = summary.get("resumed_items", 0)
            errors = summary.get("errors") or []
            log.info(
                "iteration %d: done processed=%d submitted=%d discovery=%d claimed=%d resumed=%d errors=%d",
                iteration, processed, submitted, discovered, claimed, resumed, len(errors),
            )
            if errors:
                for err in errors[:5]:
                    log.warning("iteration %d error: %s", iteration, err)

            if str(self.state_store.load_session().get("mining_state") or "idle") == "stopped":
                log.info("worker stopping: mining_state=stopped")
                break
            if max_iterations == 1 or (max_iterations != 0 and iteration >= max_iterations):
                break

            # Back off when consecutive iterations find nothing to do. This
            # mirrors run_loop's behavior and keeps the log file readable
            # (long sleeps are logged before they start).
            if not processed and not discovered and not claimed and not resumed:
                consecutive_empty += 1
                wait = min(interval * (2 ** min(consecutive_empty, 3)), 300)
            else:
                consecutive_empty = 0
                wait = interval
            self.state_store.save_session({"last_wait_seconds": wait})
            log.info("iteration %d: sleeping %ds", iteration, wait)
            time.sleep(wait)
        log.info("worker finished: total_iterations=%d", iteration)
        # Ensure daemon threads are stopped and pending items persisted.
        self.stop()
        return {
            "completed_iterations": iteration,
            "iterations": iterations,
            "status": self.check_status(),
            "state": {
                "backlog": len(self.state_store.load_backlog()),
                "auth_pending": self.state_store.load_auth_pending(),
                "submit_pending": len(self.state_store.load_submit_pending()),
            },
        }

    def _proactive_session_renew(self) -> None:
        """Check wallet session before each iteration; renew if expired or near expiry."""
        session = self.state_store.load_session()
        expires_at = session.get("token_expires_at")
        if not isinstance(expires_at, int):
            return
        remaining = expires_at - int(time.time())
        if remaining > WALLET_SESSION_RENEW_THRESHOLD_SECONDS:
            return
        signer = getattr(self.client, "_signer", None)
        renew_session = getattr(signer, "renew_session", None)
        if not callable(renew_session):
            return
        try:
            payload = renew_session(duration_seconds=WALLET_SESSION_DURATION_SECONDS)
            self._sync_wallet_refresh_state(payload)
        except Exception:
            pass

    def _send_heartbeats(self, summary: WorkerIterationSummary) -> None:
        self._ensure_wallet_session(summary)
        # WS 连接时跳过 HTTP 心跳请求（WS 维持在线），但仍然执行
        # ready pool 维护和 wallet refresh 等非心跳逻辑。
        ws_connected = (
            self.ws_source is not None
            and hasattr(self.ws_source, "ws_client")
            and getattr(self.ws_source.ws_client, "connected", False)
        )
        # WS 连接时降频发心跳（每 5 轮发一次），保持 credit/epoch 等状态更新
        send_hb = True
        if ws_connected:
            self._ws_heartbeat_skip += 1
            send_hb = self._ws_heartbeat_skip >= 5
            if send_hb:
                self._ws_heartbeat_skip = 0
        else:
            self._ws_heartbeat_skip = 0
        if send_hb:
            try:
                unified = self.client.send_unified_heartbeat(client_name=self.config.client_name)
                summary.unified_heartbeat_sent = True
                self._update_session_from_heartbeat(unified)
            except Exception as exc:
                summary.errors.append(f"unified heartbeat failed: {exc}")
            try:
                self.client.send_miner_heartbeat(client_name=self.config.client_name)
                summary.heartbeat_sent = True
                self.state_store.save_session({"last_heartbeat_at": int(time.time())})
            except Exception as exc:
                summary.errors.append(f"miner heartbeat failed: {exc}")
        else:
            summary.heartbeat_sent = True
        # Join miner ready pool (with backoff on persistent failure)
        pool_failures = getattr(self, "_pool_join_failures", 0)
        if not getattr(self, "_miner_ready_pool_joined", False) and pool_failures < 10:
            try:
                self.client.join_miner_ready_pool()
                self._miner_ready_pool_joined = True
                self._pool_join_failures = 0
            except Exception as exc:
                self._pool_join_failures = pool_failures + 1
                if self._pool_join_failures <= 3:
                    summary.errors.append(f"join miner ready pool failed: {exc}")
        self._sync_wallet_refresh_state()

    # ------------------------------------------------------------------
    # Submit thread: sequential submission with rate limit backoff
    # ------------------------------------------------------------------

    def _start_submit_thread(self) -> None:
        if self._submit_thread is not None and self._submit_thread.is_alive():
            return
        self._submit_stop.clear()
        self._submit_thread = threading.Thread(
            target=self._submit_loop, name="submit", daemon=True,
        )
        self._submit_thread.start()

    def _stop_submit_thread(self) -> None:
        self._submit_stop.set()
        self._submit_queue.put(None)  # sentinel to unblock get()
        if self._submit_thread is not None:
            # Join timeout must exceed httpx client timeout (30s) so the
            # in-flight network call can finish before we give up.
            self._submit_thread.join(timeout=45)
        # Persist any items still in the queue (not the in-flight one,
        # which was already popped by the thread).
        self._persist_remaining_queue()

    # ------------------------------------------------------------------
    # Auto-updater
    # ------------------------------------------------------------------

    def _start_auto_updater(self) -> None:
        try:
            from auto_updater import AutoUpdater
        except ImportError:
            return
        if self._auto_updater is not None:
            return
        project_root = Path(__file__).resolve().parents[1]
        self._auto_updater = AutoUpdater(
            project_root,
            on_update_applied=self._on_auto_update_applied,
        )
        self._auto_updater.start()

    def _stop_auto_updater(self) -> None:
        if self._auto_updater is not None:
            try:
                self._auto_updater.stop()
            except Exception:
                pass  # Safe to ignore — daemon thread dies with process

    def _on_auto_update_applied(self) -> None:
        """Called by AutoUpdater after a successful fast-forward pull.

        Calls stop() directly to shut down all threads (submit, repeat_crawl,
        WS) cleanly — same approach as ValidatorRuntime. Merely setting
        mining_state=stopped was insufficient because it only takes effect
        at the next iteration boundary, leaving old code running.
        """
        log = logging.getLogger("agent.auto_update")
        log.info("Auto-update applied — stopping worker for restart")
        self.state_store.save_session({
            "mining_state": "stopped",
            "stop_reason": "auto_update",
            "last_control_action": "auto-update",
            "last_state_change_at": int(time.time()),
        })
        self.stop()

    def _persist_remaining_queue(self) -> None:
        """Persist any items remaining in the submit queue to disk.

        Called after the submit thread stops (or fails to stop) so items
        survive across restarts. Safe to call from any thread.
        """
        log = logging.getLogger("agent.submit")
        count = 0
        while True:
            try:
                entry = self._submit_queue.get_nowait()
            except Exception:
                break
            if entry is None:
                continue
            item, record, report_result = entry
            self.state_store.enqueue_submit_pending(item, {"record": record, "report_result": report_result})
            count += 1
        if count:
            log.info("Persisted %d remaining queue item(s) to submit_pending", count)

    def _enqueue_submission(
        self,
        item: WorkItem,
        record: dict[str, Any],
        report_result: dict[str, Any] | None,
    ) -> None:
        """Put a crawled item into the submit queue. Auto-starts the thread."""
        # Don't revive a stopped submit thread — persist directly instead.
        if self._submit_stop.is_set():
            self.state_store.enqueue_submit_pending(item, {"record": record, "report_result": report_result})
            return
        self._submit_queue.put((item, record, report_result))
        if self._submit_thread is None or not self._submit_thread.is_alive():
            self._start_submit_thread()

    def _submit_loop(self) -> None:
        """Sequential submission with rate limit backoff.

        Takes items from the submit queue one at a time. On 429, backs off
        for retry_after seconds before continuing. This prevents multiple
        threads from hammering the platform simultaneously.
        """
        log = logging.getLogger("agent.submit")
        log.info("Submit thread started")
        while not self._submit_stop.is_set():
            try:
                entry = self._submit_queue.get(timeout=5)
            except Exception:
                continue
            if entry is None:
                break  # stop sentinel
            item, record, report_result = entry
            self._submit_single(item, record, report_result, log)
        # Remaining queue items are persisted by _stop_submit_thread →
        # _persist_remaining_queue after this thread exits.
        log.info("Submit thread stopped")

    def _submit_single(
        self,
        item: WorkItem,
        record: dict[str, Any],
        report_result: dict[str, Any] | None,
        log: logging.Logger,
    ) -> None:
        """Submit one item to the platform. Handles 429 with backoff."""
        if not item.dataset_id:
            return
        try:
            export_path, _ = _export_and_submit_core_submissions_for_task(
                self.client,
                Path(item.output_dir) if item.output_dir else (self.runner.output_root / item.source / _safe_path_segment(item.item_id)),
                record,
                item,
                report_result=report_result,
            )
            self.state_store.clear_submit_pending(item.item_id)
            self._submit_retries.pop(item.item_id, None)
            with self._submit_stats_lock:
                self._submit_stats["submitted"] += 1
            log.info("Submitted %s", item.item_id)
        except PlatformApiError as api_exc:
            if api_exc.code == "address_not_registered":
                log.error("Wallet not registered — cannot submit")
                self.state_store.clear_submit_pending(item.item_id)
                return
            if api_exc.code in ("dedup_hash_conflict", "dedup_hash_in_cooldown",
                                "url_pattern_mismatch", "duplicate",
                                "submission_not_found", "dataset_not_found"):
                with self._submit_stats_lock:
                    self._submit_stats["discarded"] += 1
                self.state_store.clear_submit_pending(item.item_id)
                log.info("Discarded %s: %s", item.item_id, api_exc.code)
                return
            # Unknown API error — defer
            self.state_store.enqueue_submit_pending(item, {"record": record, "report_result": report_result})
            with self._submit_stats_lock:
                self._submit_stats["deferred"] += 1
            log.warning("Deferred %s: %s", item.item_id, api_exc)
        except httpx.HTTPStatusError as http_exc:
            status = http_exc.response.status_code
            if status == 429:
                retry_after = _extract_retry_after_seconds(http_exc, default=60)
                retries = self._submit_retries.get(item.item_id, 0) + 1
                self._submit_retries[item.item_id] = retries
                if retries > self._MAX_SUBMIT_RETRIES:
                    # Give up after too many retries — persist to disk
                    log.warning("429 Rate Limited — giving up on %s after %d retries", item.item_id, retries)
                    self.state_store.enqueue_submit_pending(item, {"record": record, "report_result": report_result})
                    with self._submit_stats_lock:
                        self._submit_stats["deferred"] += 1
                    self._submit_retries.pop(item.item_id, None)
                    return
                log.warning("429 Rate Limited — backing off %ds then retrying %s (attempt %d/%d)",
                            retry_after, item.item_id, retries, self._MAX_SUBMIT_RETRIES)
                if item.dataset_id:
                    self.state_store.mark_dataset_cooldown(
                        item.dataset_id,
                        retry_after_seconds=retry_after,
                        reason="429 Rate Limited",
                    )
                if self._submit_stop.wait(timeout=retry_after):
                    self.state_store.enqueue_submit_pending(item, {"record": record, "report_result": report_result})
                    return
                self._submit_queue.put((item, record, report_result))
                return
            if 400 <= status < 500:
                with self._submit_stats_lock:
                    self._submit_stats["discarded"] += 1
                self.state_store.clear_submit_pending(item.item_id)
                log.info("Discarded %s: HTTP %d", item.item_id, status)
                return
            # 5xx — defer
            self.state_store.enqueue_submit_pending(item, {"record": record, "report_result": report_result})
            with self._submit_stats_lock:
                self._submit_stats["deferred"] += 1
            log.warning("Deferred %s: HTTP %d", item.item_id, status)
        except Exception as exc:
            self.state_store.enqueue_submit_pending(item, {"record": record, "report_result": report_result})
            with self._submit_stats_lock:
                self._submit_stats["deferred"] += 1
            log.warning("Deferred %s: %s", item.item_id, exc)

    def get_submit_stats(self) -> dict[str, int]:
        with self._submit_stats_lock:
            return dict(self._submit_stats)

    # ------------------------------------------------------------------
    # Dedicated repeat_crawl thread
    # ------------------------------------------------------------------

    def _start_repeat_crawl_thread(self) -> None:
        if self._repeat_thread is not None and self._repeat_thread.is_alive():
            return
        self._repeat_stop.clear()
        self._repeat_thread = threading.Thread(
            target=self._repeat_crawl_loop,
            name="repeat-crawl",
            daemon=True,
        )
        self._repeat_thread.start()

    def _stop_repeat_crawl_thread(self) -> None:
        self._repeat_stop.set()
        if self._repeat_thread is not None:
            self._repeat_thread.join(timeout=10)

    def _enqueue_repeat_crawl(self, items: list[WorkItem]) -> None:
        """Put repeat_crawl items into the dedicated queue, auto-starting the thread."""
        with self._repeat_lock:
            self._repeat_queue.extend(items)
        # Lazy start: ensure the processing thread is running
        if self._repeat_thread is None or not self._repeat_thread.is_alive():
            self._start_repeat_crawl_thread()

    def _repeat_crawl_loop(self) -> None:
        """Process repeat_crawl items independently from the main iteration.

        Runs in its own thread so a slow discovery/enrich task can never
        block a repeat_crawl that has a 6-minute platform deadline.
        Each item is processed synchronously (fetch+extract, no enrich)
        with a 120s timeout.
        """
        log = logging.getLogger("agent.repeat_crawl")
        log.info("Repeat-crawl thread started")
        while not self._repeat_stop.is_set():
            # Drain queue
            with self._repeat_lock:
                batch = list(self._repeat_queue)
                self._repeat_queue.clear()
            if not batch:
                self._repeat_stop.wait(timeout=5)
                continue
            for item in batch:
                if self._repeat_stop.is_set():
                    break
                self._process_single_repeat_crawl(item, log)
        log.info("Repeat-crawl thread stopped")

    def _process_single_repeat_crawl(
        self,
        item: WorkItem,
        log: logging.Logger,
    ) -> None:
        """Process one repeat_crawl item: fetch+extract → report cleaned_data."""
        try:
            command = self.crawl_mode_planner.choose_command(item)
            result = self.runner.run_item(item, command)
        except SkipItemError as exc:
            log.warning("repeat_crawl skipped %s: %s", item.claim_task_id, exc)
            self._report_repeat_crawl_failure(item, "crawl_timeout")
            return
        except Exception as exc:
            log.error("repeat_crawl failed %s: %s", item.claim_task_id, exc)
            self._report_repeat_crawl_failure(item, "crawl_failed")
            return

        if not result.records:
            fail_reason = self._classify_crawl_fail_reason(result.errors)
            self._report_repeat_crawl_failure(item, fail_reason)
            return

        record = result.records[0]
        report_payload = build_report_payload(item, record)
        try:
            self.client.report_repeat_crawl_task_result(item.claim_task_id, report_payload)
            log.info("repeat_crawl %s reported successfully", item.claim_task_id)
        except Exception as exc:
            log.error("report repeat_crawl result failed for %s: %s", item.claim_task_id, exc)

    def _collect_work_items(self, summary: WorkerIterationSummary) -> list[WorkItem]:
        session = self.state_store.load_session()
        selected_dataset_ids = {
            str(dataset_id)
            for dataset_id in (session.get("selected_dataset_ids") or [])
            if str(dataset_id).strip()
        }
        items: list[WorkItem] = []
        resumed = self.resume_source.collect(limit=self.config.max_parallel)
        summary.resumed_items = len(resumed)
        items.extend(resumed)
        # WebSocket push (preferred) — drain any tasks received via WS
        ws_claimed: list[WorkItem] = []
        if self.ws_source is not None:
            try:
                ws_claimed = self.ws_source.collect()
            except Exception as exc:
                ws_claimed = []
                summary.errors.append(f"ws claim source failed: {exc}")
            summary.errors.extend(getattr(self.ws_source, "last_errors", []))
            summary.messages.extend(getattr(self.ws_source, "last_skips", []))
            items.extend(ws_claimed)

        # HTTP polling fallback — only poll if WS is absent or returned nothing
        ws_delivered = bool(self.ws_source is not None and ws_claimed)
        if not ws_delivered:
            try:
                claimed = self.backend_source.collect()
            except Exception as exc:
                claimed = []
                summary.errors.append(f"claim source failed: {exc}")
            summary.errors.extend(getattr(self.backend_source, "last_errors", []))
            summary.messages.extend(getattr(self.backend_source, "last_skips", []))
            items.extend(claimed)

        # Single-pass partition: route repeat_crawl to dedicated thread
        repeat_items: list[WorkItem] = []
        non_repeat: list[WorkItem] = []
        for i in items:
            (repeat_items if i.claim_task_type == "repeat_crawl" else non_repeat).append(i)
        items = non_repeat
        if repeat_items:
            self._enqueue_repeat_crawl(repeat_items)
            summary.claimed_items = len(repeat_items)
            summary.messages.append(
                f"{len(repeat_items)} repeat_crawl task(s) routed to dedicated thread"
            )

        summary.claimed_items += len([item for item in items if item.source == "backend_claim"])
        try:
            discoveries = self.dataset_source.collect(min_interval_seconds=self.config.dataset_refresh_seconds)
        except Exception as exc:
            discoveries = []
            summary.errors.append(f"dataset discovery failed: {exc}")
        summary.discovery_items = len(discoveries)
        items.extend(discoveries)
        merged: dict[str, WorkItem] = {}
        for item in items:
            allowed = self._filter_collectible_item(item, selected_dataset_ids=selected_dataset_ids, summary=summary)
            if allowed is not None:
                merged[allowed.item_id] = allowed
        refill_attempts = 0
        while len(merged) < self.config.max_parallel and refill_attempts < max(1, self.config.max_parallel * 2):
            remaining = self.config.max_parallel - len(merged)
            extra = self.resume_source.collect(limit=remaining)
            if not extra:
                break
            refill_attempts += 1
            summary.resumed_items += len(extra)
            for item in extra:
                allowed = self._filter_collectible_item(item, selected_dataset_ids=selected_dataset_ids, summary=summary)
                if allowed is not None:
                    merged[allowed.item_id] = allowed
        filtered = list(merged.values())[: self.config.max_parallel]
        # Final counts from the filtered set only. Reset pre-filter counts
        # to avoid double-counting (they were set during collection above).
        repeat_claimed = summary.claimed_items  # preserve repeat_crawl count
        summary.discovery_items = 0
        summary.resumed_items = 0
        summary.claimed_items = repeat_claimed
        for item in filtered:
            if item.source == "dataset_discovery":
                summary.discovery_items += 1
            elif item.source in ("backlog", "resume", "auth_pending"):
                summary.resumed_items += 1
            elif item.source == "backend_claim":
                summary.claimed_items += 1
        return filtered

    def _process_items(self, items: list[WorkItem], summary: WorkerIterationSummary) -> None:
        if self.config.per_dataset_parallel:
            self._process_items_per_dataset(items, summary)
        else:
            self._process_items_mixed(items, summary)

    def _process_items_mixed(self, items: list[WorkItem], summary: WorkerIterationSummary) -> None:
        """Process all items in a single mixed pool (legacy behavior)."""
        with ThreadPoolExecutor(max_workers=max(1, self.config.max_parallel)) as executor:
            futures = {executor.submit(self._run_item, item): item for item in items}
            for future in as_completed(futures):
                item = futures[future]
                try:
                    result = future.result()
                except SkipItemError as exc:
                    summary.skipped_items += 1
                    summary.messages.append(f"skipped {item.item_id}: {exc}")
                    # Extract actual reason from SkipItemError message
                    reason = "occupancy_blocked" if "occupancy_blocked" in str(exc) else "crawl_timeout"
                    self._handle_item_failure(item, reason, summary)
                    continue
                except Exception as exc:
                    summary.errors.append(f"{item.item_id}: {exc}")
                    self._handle_item_failure(item, "crawl_failed", summary)
                    continue
                try:
                    self._handle_result(item, result, summary)
                except Exception as handle_exc:
                    summary.errors.append(f"handle_result failed for {item.item_id}: {handle_exc}")
                    self.state_store.enqueue_backlog([_clone_item(item, resume=True, output_dir=result.output_dir)])

    def _process_items_per_dataset(self, items: list[WorkItem], summary: WorkerIterationSummary) -> None:
        """Process items grouped by dataset_id, each group with independent concurrency."""
        # Group items by dataset_id
        grouped: dict[str, list[WorkItem]] = {}
        for item in items:
            key = item.dataset_id or "_no_dataset"
            grouped.setdefault(key, []).append(item)

        # Process all dataset groups in parallel, each with its own executor
        def process_group(group_items: list[WorkItem]) -> list[tuple[WorkItem, CrawlerRunResult | Exception]]:
            results: list[tuple[WorkItem, CrawlerRunResult | Exception]] = []
            with ThreadPoolExecutor(max_workers=max(1, min(len(group_items), self.config.max_parallel))) as executor:
                futures = {executor.submit(self._run_item, item): item for item in group_items}
                for future in as_completed(futures):
                    item = futures[future]
                    try:
                        result = future.result()
                        results.append((item, result))
                    except Exception as exc:
                        results.append((item, exc))
            return results

        # Run all groups concurrently
        with ThreadPoolExecutor(max_workers=len(grouped)) as group_executor:
            group_futures = {
                group_executor.submit(process_group, group_items): dataset_id
                for dataset_id, group_items in grouped.items()
            }
            for group_future in as_completed(group_futures):
                dataset_id = group_futures[group_future]
                try:
                    group_results = group_future.result()
                except Exception as exc:
                    summary.errors.append(f"dataset group {dataset_id} failed: {exc}")
                    continue
                for item, result in group_results:
                    if isinstance(result, SkipItemError):
                        summary.skipped_items += 1
                        summary.messages.append(f"skipped {item.item_id}: {result}")
                        reason = "occupancy_blocked" if "occupancy_blocked" in str(result) else "crawl_timeout"
                        self._handle_item_failure(item, reason, summary)
                    elif isinstance(result, Exception):
                        summary.errors.append(f"{item.item_id}: {result}")
                        self._handle_item_failure(item, "crawl_failed", summary)
                    else:
                        try:
                            self._handle_result(item, result, summary)
                        except Exception as handle_exc:
                            summary.errors.append(f"handle_result failed for {item.item_id}: {handle_exc}")
                            self.state_store.enqueue_backlog([_clone_item(item, resume=True, output_dir=getattr(result, 'output_dir', None))])

    def _run_item(self, item: WorkItem) -> CrawlerRunResult:
        command = self.crawl_mode_planner.choose_command(item)
        writer = self._artifact_writer_for_item(item)
        self._preflight_item(item, command=command, writer=writer)
        result = self.runner.run_item(item, command)
        writer.write_json(
            "crawler/result.json",
            {
                "exit_code": result.exit_code,
                "argv": result.argv,
                "records_count": len(result.records),
                "errors_count": len(result.errors),
            },
        )
        return result

    def _preflight_item(self, item: WorkItem, *, command: str, writer: RunArtifactWriter | None = None) -> None:
        terminal_state = self._handle_preflight_common(item, writer=writer, command=command)
        if terminal_state is not None:
            raise SkipItemError(f"preflight blocked ({terminal_state}) for {item.url}")

    def _handle_result(self, item: WorkItem, result: CrawlerRunResult, summary: WorkerIterationSummary) -> None:
        auth_pending = self.auth_orchestrator.handle_errors(item, result.errors)
        if auth_pending:
            summary.auth_pending.extend(auth_pending)

        retryable_errors = [
            error for error in result.errors
            if bool(error.get("retryable")) and str(error.get("error_code") or "") not in AUTH_ERROR_CODES
        ]
        command = self.crawl_mode_planner.choose_command(item)

        # Only retry non-discovery items on retryable errors (discovery is idempotent via follow-ups)
        if retryable_errors and command != "discover-crawl":
            self.state_store.enqueue_backlog([_clone_item(item, resume=True, output_dir=result.output_dir)])

        progress_message = self._build_progress_message(item, result)
        if progress_message:
            summary.messages.append(progress_message)
        if command == "discover-crawl":
            followups = build_follow_up_items_from_discovery(item, result.records)
            if followups:
                self.state_store.enqueue_backlog(followups)
            summary.discovered_followups += len(followups)
            summary.processed_items += 1
            summary.messages.append(f"discovered {len(followups)} follow-up URLs from {item.url}")
            # If discover-crawl produced no followups, cooldown the dataset to prevent
            # immediate retry (covers both error cases and empty 200 OK responses)
            if not followups and item.dataset_id:
                self.state_store.mark_dataset_cooldown(
                    item.dataset_id,
                    retry_after_seconds=300,
                    reason="discover-crawl produced no results",
                )
                summary.messages.append(f"dataset {item.dataset_id} cooled down 5min after failed discover-crawl")
            return

        if not result.records:
            for error in result.errors:
                summary.errors.append(f"{item.item_id}: {error.get('error_code') or 'UNKNOWN'}")
            # Report failure to server so it can reassign immediately
            if item.claim_task_id and item.claim_task_type == "repeat_crawl":
                fail_reason = self._classify_crawl_fail_reason(result.errors)
                self._report_repeat_crawl_failure(item, fail_reason)
                summary.messages.append(f"repeat_crawl {item.claim_task_id} failed ({fail_reason})")
            return

        record = result.records[0]
        self.auth_orchestrator.clear_if_recovered(item)
        summary.processed_items += 1

        report_result: dict[str, Any] | None = None
        if item.claim_task_id and item.claim_task_type:
            report_payload = build_report_payload(item, record)
            try:
                if item.claim_task_type == "repeat_crawl":
                    report_result = self.client.report_repeat_crawl_task_result(item.claim_task_id, report_payload)
                elif item.claim_task_type == "refresh":
                    report_result = self.client.report_refresh_task_result(item.claim_task_id, report_payload)
                else:
                    summary.errors.append(f"unknown claim_task_type: {item.claim_task_type} for {item.claim_task_id}")
                    return
            except PlatformApiError as api_exc:
                if api_exc.code == "address_not_registered":
                    summary.errors.append("Wallet address not registered. Please install and use the AWP Skill to complete on-chain registration, then retry.")
                    return
                summary.errors.append(f"report failed for {item.item_id}: {api_exc}")
                return
            except httpx.HTTPStatusError as exc:
                if self._maybe_handle_rate_limit(item, exc, summary, output_dir=result.output_dir):
                    return
                summary.errors.append(f"HTTP error for {item.item_id}: {exc.response.status_code}")
                self.state_store.enqueue_backlog([_clone_item(item, resume=True, output_dir=result.output_dir)])
                return

        # repeat_crawl tasks only report cleaned_data — no structured data submission needed
        if item.claim_task_type == "repeat_crawl":
            summary.messages.append(f"repeat_crawl reported for {item.item_id}")
            return

        if item.dataset_id:
            # Enqueue to the dedicated submit thread — submissions are
            # sequential with rate limit backoff. Crawl threads keep working
            # while the submit thread waits out 429 cooldowns.
            # Ensure output_dir is set so the submit thread can find the files
            if not item.output_dir:
                item = _clone_item(item, resume=False, output_dir=result.output_dir)
            self._enqueue_submission(item, record, report_result)
            summary.messages.append(f"processed {item.item_id}, queued for submission")
        else:
            summary.messages.append(f"processed {item.item_id} in {result.output_dir}")

    def _drain_submit_pending(self, summary: WorkerIterationSummary) -> None:
        """Re-enqueue persisted submit_pending items to the submit thread.

        Each item is cleared from the persistent store BEFORE enqueuing to
        the submit thread. If the submission fails, the submit thread
        re-persists it via enqueue_submit_pending. This prevents the same
        item from being drained again on the next iteration (double submit).
        """
        pending = self.state_store.load_submit_pending()
        if not pending:
            return
        count = 0
        for entry in pending:
            item_payload = entry.get("item")
            payload = entry.get("payload")
            if not isinstance(item_payload, dict) or not isinstance(payload, dict):
                continue
            item = WorkItem.from_dict(item_payload)
            record = payload.get("record")
            report_result = payload.get("report_result")
            if not isinstance(record, dict):
                continue
            # Clear first, then enqueue. If we crash between clear and enqueue,
            # the item is lost — but this is the only way to prevent double
            # submission (drain seeing the same item every iteration). The submit
            # thread re-persists on failure, so the window is tiny.
            self.state_store.clear_submit_pending(item.item_id)
            self._enqueue_submission(
                item, record,
                report_result if isinstance(report_result, dict) else None,
            )
            count += 1
        if count:
            summary.messages.append(f"re-enqueued {count} pending submission(s) to submit thread")

    def _process_single_item_for_test(self, item: WorkItem, writer: RunArtifactWriter) -> str:
        command = self.crawl_mode_planner.choose_command(item)
        writer.write_json("task/item.json", item.to_dict())
        terminal_state = self._handle_preflight_common(item, writer=writer, command=command)
        if terminal_state is not None:
            return terminal_state
        result = self.runner.run_item(item, command)
        writer.write_json(
            "crawler/result.json",
            {
                "exit_code": result.exit_code,
                "argv": result.argv,
                "records_count": len(result.records),
                "errors_count": len(result.errors),
            },
        )
        return "processed"

    @staticmethod
    def _classify_crawl_fail_reason(errors: list[dict[str, Any]]) -> str:
        """Classify crawl errors into a platform fail_reason code."""
        for error in errors:
            code = str(error.get("error_code") or "")
            if "CAPTCHA" in code:
                return "captcha_detected"
            if "AUTH" in code:
                return "auth_required"
            if "CONTENT_EMPTY" in code:
                return "content_empty"
        return "crawl_failed"

    def _report_repeat_crawl_failure(
        self,
        item: WorkItem,
        fail_reason: str,
    ) -> None:
        """Report repeat_crawl failure to platform so it can reassign.

        Per API spec, report body is {"cleaned_data": "...", "failed": bool}.
        fail_reason is logged locally but NOT sent to platform (not in spec).
        """
        log = logging.getLogger("agent.repeat_crawl")
        try:
            self.client.report_repeat_crawl_task_result(
                item.claim_task_id,
                {"cleaned_data": "", "failed": True},
            )
            log.info("repeat_crawl %s reported as failed (%s)", item.claim_task_id, fail_reason)
        except Exception as exc:
            log.error("report repeat_crawl failure failed: %s", exc)

    # Discovery items 最多重试 3 次后丢弃——防止坏 URL（502/timeout）
    # 反复进 backlog 占住 max_parallel 槽位，挤掉新 discovery。
    _MAX_ITEM_RETRIES = 3

    # 不可恢复的失败——永远不重试，立即丢弃
    _NON_RETRYABLE_REASONS = frozenset({
        "occupancy_blocked",  # URL 被其他 miner 占用，epoch 内不会释放
    })

    def _handle_item_failure(
        self,
        item: WorkItem,
        fail_reason: str,
        summary: WorkerIterationSummary,
    ) -> None:
        """Route a failed item: repeat_crawl → report to platform, others → re-queue with retry limit."""
        if item.claim_task_id and item.claim_task_type == "repeat_crawl":
            self._report_repeat_crawl_failure(item, fail_reason)
            summary.messages.append(f"repeat_crawl {item.claim_task_id} failed ({fail_reason})")
            return
        # Non-retryable failures — discard immediately, don't waste slots
        if fail_reason in self._NON_RETRYABLE_REASONS:
            summary.messages.append(f"discarded {item.item_id}: {fail_reason} (non-retryable)")
            return
        # Track retry count in metadata to prevent infinite re-queue loops
        retries = int(item.metadata.get("_retries", 0)) + 1
        if retries > self._MAX_ITEM_RETRIES:
            summary.messages.append(
                f"discarded {item.item_id} after {retries - 1} retries ({fail_reason})"
            )
            return
        cloned = _clone_item(item, resume=True)
        updated = WorkItem(
            item_id=cloned.item_id,
            source=cloned.source,
            url=cloned.url,
            dataset_id=cloned.dataset_id,
            platform=cloned.platform,
            resource_type=cloned.resource_type,
            record=cloned.record,
            crawler_command=cloned.crawler_command,
            claim_task_id=cloned.claim_task_id,
            claim_task_type=cloned.claim_task_type,
            metadata={**cloned.metadata, "_retries": retries},
            resume=cloned.resume,
            output_dir=cloned.output_dir,
        )
        self.state_store.enqueue_backlog([updated])

    def _handle_preflight_common(self, item: WorkItem, writer: RunArtifactWriter | None, *, command: str) -> str | None:
        """Pre-submission check: URL occupancy via public GET endpoint (no auth needed).

        Skipped for:
        - discover-crawl: listing pages, not real submissions
        - repeat_crawl: platform explicitly assigned this URL for re-crawl —
          the URL is occupied *by design*; checking occupancy would block
          every repeat_crawl task
        """
        if item.claim_task_type == "repeat_crawl":
            return None
        if item.dataset_id and command != "discover-crawl":
            occupancy = self.client.check_url_occupancy_public(
                item.dataset_id,
                item.url,
            )
            if writer is not None:
                writer.write_json("occupancy/response.json", occupancy if isinstance(occupancy, dict) else {})
            if occupancy.get("occupied"):
                return "occupancy_blocked"
        return None

    def _artifact_writer_for_item(self, item: WorkItem) -> RunArtifactWriter:
        output_dir = resolve_item_output_dir(item, output_root=self.runner.output_root)
        return RunArtifactWriter(output_dir / "_run_artifacts")

    def _write_iteration_summary(self, payload: dict[str, Any]) -> None:
        writer = RunArtifactWriter(self.runner.output_root / "_run_once")
        writer.write_json("last-summary.json", payload)

    def _update_session_from_heartbeat(self, payload: dict[str, Any] | None) -> None:
        if not isinstance(payload, dict):
            self.state_store.save_session({"last_heartbeat_at": int(time.time())})
            return
        data = payload.get("data")
        source = data if isinstance(data, dict) else payload
        update: dict[str, Any] = {"last_heartbeat_at": int(time.time())}
        # Read from top-level first (legacy format)
        for key in ("credit_score", "credit_tier", "epoch_id", "epoch_submitted", "epoch_target", "epoch_submit_limit", "pow_probability"):
            if key in source:
                update[key] = source[key]
        # Miner sub-object takes precedence (new format), overrides top-level keys
        miner_info = source.get("miner")
        if isinstance(miner_info, dict):
            if "miner_id" in miner_info:
                update["miner_id"] = miner_info["miner_id"]
            for key in ("credit_tier", "epoch_submit_limit", "pow_probability"):
                if key in miner_info:
                    update[key] = miner_info[key]
            # Map miner.credit (integer) to credit_score for display consistency
            if "credit" in miner_info:
                credit_val = miner_info["credit"]
                if isinstance(credit_val, (int, float)):
                    update["credit_score"] = int(credit_val)
                else:
                    update["credit"] = credit_val
        # Top-level credit handling — only if miner sub-object didn't already set credit_score
        credit = source.get("credit")
        if "credit_score" not in update and isinstance(credit, (int, float)):
            update["credit_score"] = int(credit)
        elif isinstance(credit, dict) and "credit" not in update:
            update["credit"] = dict(credit)
        elif "credit_score" not in update:
            credit_update = {
                key: source[key]
                for key in ("credit_score", "credit_tier", "credit_delta", "credit_status")
                if key in source
            }
            if credit_update:
                update["credit"] = credit_update
        reward = source.get("reward")
        if isinstance(reward, dict):
            update["reward"] = dict(reward)
        else:
            reward_update = {
                key: source[key]
                for key in (
                    "pending_rewards",
                    "settled_rewards",
                    "claimable_rewards",
                    "reward_balance",
                    "reward_total",
                    "lifetime_rewards",
                )
                if key in source
            }
            if reward_update:
                update["reward"] = reward_update
        settlement = source.get("settlement")
        if isinstance(settlement, dict):
            update["settlement"] = settlement
        self.state_store.save_session(update)

    def _update_session_from_summary(self, payload: dict[str, Any]) -> None:
        session = self.state_store.load_session()
        totals = dict(session.get("session_totals") or {})
        totals["processed_items"] = int(totals.get("processed_items") or 0) + int(payload.get("processed_items") or 0)
        totals["submitted_items"] = int(totals.get("submitted_items") or 0) + int(payload.get("submitted_items") or 0)
        totals["failed_items"] = int(totals.get("failed_items") or 0) + len(payload.get("errors") or [])
        self.state_store.save_session({
            "last_summary": payload,
            "session_totals": totals,
            "last_activity_at": int(time.time()),
            "last_iteration": int(payload.get("iteration") or 0),
        })

    def _ensure_wallet_session(self, summary: WorkerIterationSummary) -> None:
        session = self.state_store.load_session()
        expires_at = session.get("token_expires_at")
        if not isinstance(expires_at, int):
            return
        now = int(time.time())
        remaining = expires_at - now
        if remaining > WALLET_SESSION_RENEW_THRESHOLD_SECONDS:
            return
        if remaining <= 0:
            summary.messages.append("wallet session expired, renewing now")
        else:
            summary.messages.append(f"wallet session expires in {remaining}s, renewing")
        refresh_if_needed = getattr(self.client, "refresh_wallet_session_if_needed", None)
        if callable(refresh_if_needed):
            try:
                payload = refresh_if_needed(threshold_seconds=WALLET_SESSION_RENEW_THRESHOLD_SECONDS)
            except TypeError:
                try:
                    payload = refresh_if_needed(WALLET_SESSION_RENEW_THRESHOLD_SECONDS)
                except Exception as exc:
                    summary.errors.append(f"wallet session refresh failed: {exc}")
                    return
            except Exception as exc:
                summary.errors.append(f"wallet session refresh failed: {exc}")
                return
            self._sync_wallet_refresh_state(payload if isinstance(payload, dict) else None)
            summary.messages.append(f"wallet session renewed for {WALLET_SESSION_DURATION_SECONDS}s")
            return
        signer = getattr(self.client, "_signer", None)
        renew_session = getattr(signer, "renew_session", None)
        if not callable(renew_session):
            return
        try:
            payload = renew_session(duration_seconds=WALLET_SESSION_DURATION_SECONDS)
        except Exception as exc:
            summary.errors.append(f"wallet session refresh failed: {exc}")
            return
        self._sync_wallet_refresh_state(payload)
        summary.messages.append(f"wallet session renewed for {WALLET_SESSION_DURATION_SECONDS}s")

    def _sync_wallet_refresh_state(self, payload: dict[str, Any] | None = None) -> None:
        if isinstance(payload, dict):
            refresh = payload
        else:
            consume_refresh = getattr(self.client, "consume_wallet_refresh", None)
            refresh = consume_refresh() if callable(consume_refresh) else None
        if not isinstance(refresh, dict):
            return
        update: dict[str, Any] = {}
        expires_at = refresh.get("expires_at")
        issued_at = refresh.get("issued_at")
        if isinstance(expires_at, int):
            update["token_expires_at"] = expires_at
        if isinstance(issued_at, int):
            update["last_wallet_refresh_at"] = issued_at
        if update:
            self.state_store.save_session(update)

    def _filter_collectible_item(
        self,
        item: WorkItem,
        *,
        selected_dataset_ids: set[str],
        summary: WorkerIterationSummary,
    ) -> WorkItem | None:
        if selected_dataset_ids and item.source in {"dataset_discovery", "discovery_followup"} and item.dataset_id not in selected_dataset_ids:
            return None
        if item.dataset_id and not self.state_store.is_dataset_available(item.dataset_id):
            summary.messages.append(f"dataset {item.dataset_id} cooling down; deferred {item.item_id}")
            if item.source not in {"dataset_discovery"}:
                self.state_store.enqueue_backlog([_clone_item(item, resume=True, output_dir=Path(item.output_dir) if item.output_dir else None)])
            return None
        return item

    def _save_current_batch(
        self,
        *,
        iteration: int,
        items: list[WorkItem],
        state: str,
        summary: WorkerIterationSummary | None = None,
        stop_reason: str | None = None,
    ) -> dict[str, Any]:
        dataset_ids = sorted({item.dataset_id for item in items if item.dataset_id})
        payload: dict[str, Any] = {
            "state": state,
            "iteration": iteration,
            "size": len(items),
            "item_ids": [item.item_id for item in items],
            "dataset_ids": dataset_ids,
        }
        if summary is not None:
            payload.update({
                "processed_items": summary.processed_items,
                "submitted_items": summary.submitted_items,
                "error_count": len(summary.errors),
                "message_count": len(summary.messages),
            })
        if stop_reason:
            payload["stop_reason"] = stop_reason
        self.state_store.save_session({"current_batch": payload})
        return payload

    def _finalize_iteration(
        self,
        summary: WorkerIterationSummary,
        *,
        current_batch: dict[str, Any] | None = None,
        stop_reason: str | None = None,
    ) -> dict[str, Any]:
        # Merge async submit thread progress into the iteration summary.
        # Without this, status displays submitted=0 forever because the
        # submit path is now async and no longer increments summary counters
        # directly. We compute the delta since last iteration and attribute
        # it to this iteration.
        with self._submit_stats_lock:
            submitted_now = self._submit_stats.get("submitted", 0)
            discarded_now = self._submit_stats.get("discarded", 0)
            submitted_delta = submitted_now - self._submit_stats_baseline["submitted"]
            discarded_delta = discarded_now - self._submit_stats_baseline["discarded"]
            self._submit_stats_baseline["submitted"] = submitted_now
            self._submit_stats_baseline["discarded"] = discarded_now
        if submitted_delta > 0:
            summary.submitted_items += submitted_delta
        if discarded_delta > 0:
            summary.messages.append(f"{discarded_delta} submission(s) discarded by platform")

        payload = summary.to_dict()
        if current_batch:
            payload["current_batch"] = current_batch
        if stop_reason:
            payload["stop_reason"] = stop_reason
        self._write_iteration_summary(payload)
        self._update_session_from_summary(payload)
        return payload

    def _mark_stop_condition(self, reason: str) -> None:
        self.state_store.save_session({
            "mining_state": "stopped",
            "stop_reason": reason,
            "last_control_action": "stop-condition",
            "last_state_change_at": int(time.time()),
        })

    def _active_stop_reason(
        self,
        *,
        payload: dict[str, Any] | None = None,
    ) -> str | None:
        session = self.state_store.load_session()
        conditions = session.get("stop_conditions")
        if not isinstance(conditions, dict):
            return None
        totals = dict(session.get("session_totals") or {})
        submitted_items = int(totals.get("submitted_items") or 0) + int((payload or {}).get("submitted_items") or 0)
        failed_items = int(totals.get("failed_items") or 0) + len((payload or {}).get("errors") or [])
        max_submissions = int(conditions.get("max_submissions") or 0)
        if max_submissions > 0 and submitted_items >= max_submissions:
            return "max_submissions"
        max_errors = int(conditions.get("max_errors") or 0)
        if max_errors > 0 and failed_items >= max_errors:
            return "max_errors"
        if bool(conditions.get("epoch_target_reached")):
            epoch_target = int(session.get("epoch_target") or 0)
            epoch_submitted = int(session.get("epoch_submitted") or 0)
            if epoch_target > 0 and epoch_submitted >= epoch_target:
                return "epoch_target_reached"
        max_runtime_minutes = int(conditions.get("max_runtime_minutes") or 0)
        if max_runtime_minutes > 0:
            started_at = session.get("run_started_at") or session.get("last_state_change_at")
            if isinstance(started_at, int) and int(time.time()) - started_at >= max_runtime_minutes * 60:
                return "max_runtime_minutes"
        return None

    def _build_progress_message(self, item: WorkItem, result: CrawlerRunResult) -> str | None:
        details: list[str] = []
        progress_path = result.output_dir / "_run_artifacts" / "progress.json"
        progress_payload = read_json_file(progress_path) if progress_path.exists() else {}
        if not isinstance(progress_payload, dict):
            progress_payload = {}
        phase = optional_string(progress_payload.get("phase")) or optional_string(result.summary.get("phase")) or optional_string(result.summary.get("stage"))
        if phase:
            details.append(f"phase {phase}")
        completed_steps = progress_payload.get("completed_steps")
        total_steps = progress_payload.get("total_steps")
        if isinstance(completed_steps, int) and isinstance(total_steps, int) and total_steps > 0:
            details.append(f"step {completed_steps}/{total_steps}")
        records_emitted = result.summary.get("records_emitted")
        if isinstance(records_emitted, int):
            details.append(f"records {records_emitted}")
        stderr_lines = [
            line.strip()
            for line in result.stderr.splitlines()
            if line.strip() and "I/O operation on closed pipe" not in line
        ]
        if stderr_lines:
            details.append(stderr_lines[-1])
        if not details:
            return None
        return f"progress {item.item_id}: {'; '.join(details)}"

    def _reward_status_from_session(self, session: dict[str, Any]) -> dict[str, Any]:
        reward = dict(session.get("reward") or {}) if isinstance(session.get("reward"), dict) else {}
        settlement = dict(session.get("settlement") or {}) if isinstance(session.get("settlement"), dict) else {}
        if not reward:
            for source_key, target_key in (("pending_rewards", "pending_rewards"), ("settled_rewards", "settled_rewards"), ("claimable_rewards", "claimable_rewards")):
                if source_key in settlement and target_key not in reward:
                    reward[target_key] = settlement[source_key]
        return reward

    def _credit_status_from_session(self, session: dict[str, Any]) -> dict[str, Any]:
        credit = dict(session.get("credit") or {}) if isinstance(session.get("credit"), dict) else {}
        if session.get("credit_score") is not None:
            credit["score"] = session.get("credit_score")
        if session.get("credit_tier"):
            credit["tier"] = session.get("credit_tier")
        expires_at = session.get("token_expires_at")
        if isinstance(expires_at, int):
            credit["token_expires_at"] = expires_at
        return credit

    def _resolve_phase(
        self,
        *,
        session: dict[str, Any],
        current_batch: dict[str, Any],
        reward: dict[str, Any],
    ) -> dict[str, Any]:
        settlement = dict(session.get("settlement") or {}) if isinstance(session.get("settlement"), dict) else {}
        mining_state = str(session.get("mining_state") or "idle")
        if (reward or settlement) and mining_state != "running":
            return {"id": 4, "label": "Phase 4 - Settlement"}
        if current_batch.get("state") == "running":
            return {"id": 2, "label": "Phase 2 - Work Loop"}
        if session.get("epoch_id") or int(session.get("epoch_submitted") or 0) > 0:
            return {"id": 3, "label": "Phase 3 - Epoch Monitor"}
        if session.get("last_heartbeat_at"):
            return {"id": 1, "label": "Phase 1 - Heartbeat"}
        return {"id": 0, "label": "Phase 0 - Init"}

    def _maybe_handle_rate_limit(
        self,
        item: WorkItem,
        exc: httpx.HTTPStatusError,
        summary: WorkerIterationSummary,
        *,
        output_dir: Path,
    ) -> bool:
        if exc.response.status_code != 429 or not item.dataset_id:
            return False
        retry_after = _extract_retry_after_seconds(exc, default=self.config.auth_retry_interval_seconds)
        self.state_store.mark_dataset_cooldown(
            item.dataset_id,
            retry_after_seconds=retry_after,
            reason="429 Rate Limited",
        )
        self.state_store.enqueue_backlog([_clone_item(item, resume=True, output_dir=output_dir)])
        summary.errors.append(f"submit deferred for {item.item_id}: 429 Rate Limited")
        summary.messages.append(
            f"{item.dataset_id} cooled down for {retry_after}s after 429 Rate Limited"
        )
        return True


def build_worker_from_env(*, auto_register_awp: bool = False) -> AgentWorker:
    from signer import WalletSigner

    output_root = Path(os.environ.get("CRAWLER_OUTPUT_ROOT", str(CRAWLER_ROOT / "output" / "agent-runs"))).resolve()
    # Prefer the current interpreter so the child process does not use a bare `python` outside the venv on Windows.
    python_bin = os.environ.get("PYTHON_BIN") or os.environ.get("PLUGIN_PYTHON_BIN") or sys.executable
    state_root = Path(os.environ.get("WORKER_STATE_ROOT", str(output_root / "_worker_state"))).resolve()
    gateway_model_config = resolve_mine_gateway_model_config()
    signature_config = resolve_signature_config()
    config = WorkerConfig(
        base_url=resolve_platform_base_url(),
        token=os.environ.get("PLATFORM_TOKEN", ""),
        miner_id=resolve_miner_id(),
        output_root=output_root,
        crawler_root=CRAWLER_ROOT,
        python_bin=python_bin,
        state_root=state_root,
        default_backend=(os.environ.get("DEFAULT_BACKEND") or None),
        max_parallel=max(1, int(os.environ.get("WORKER_MAX_PARALLEL", "3"))),
        per_dataset_parallel=os.environ.get("WORKER_PER_DATASET_PARALLEL", "1").lower() in ("1", "true", "yes"),
        dataset_refresh_seconds=max(60, int(os.environ.get("DATASET_REFRESH_SECONDS", "120"))),
        discovery_max_pages=max(1, int(os.environ.get("DISCOVERY_MAX_PAGES", "25"))),
        discovery_max_depth=max(0, int(os.environ.get("DISCOVERY_MAX_DEPTH", "1"))),
        auth_retry_interval_seconds=max(30, int(os.environ.get("AUTH_RETRY_INTERVAL_SECONDS", "300"))),
        gateway_model_config=gateway_model_config,
        # Signature params follow platform discovery/cache; env vars override when set.
        eip712_domain_name=str(signature_config.get("domain_name") or DEFAULT_EIP712_DOMAIN_NAME),
        eip712_domain_version=str(signature_config.get("domain_version") or "1"),
        eip712_chain_id=int(signature_config.get("chain_id") or DEFAULT_EIP712_CHAIN_ID),
        eip712_verifying_contract=str(
            signature_config.get("verifying_contract") or DEFAULT_EIP712_VERIFYING_CONTRACT
        ),
    )

    wallet_bin, wallet_token = resolve_wallet_config()
    if auto_register_awp and wallet_token.strip():
        registration = resolve_awp_registration(auto_register=True)
        if registration.get("registration_required"):
            raise RuntimeError(str(registration.get("message") or "wallet registration required before startup"))
    signer: WalletSigner | None = None
    if wallet_token.strip():
        signer = WalletSigner(wallet_bin=wallet_bin, session_token=wallet_token)

    client = PlatformClient(
        base_url=config.base_url,
        token=config.token,
        signer=signer,
        eip712_chain_id=config.eip712_chain_id,
        eip712_domain_name=config.eip712_domain_name,
        eip712_domain_version=config.eip712_domain_version,
        eip712_verifying_contract=config.eip712_verifying_contract,
    )
    runner = CrawlerRunner(config)

    # Create WebSocket client for push-based task receiving (optional)
    ws_client = None
    if signer is not None and os.environ.get("MINER_DISABLE_WS", "").lower() not in ("1", "true"):
        try:
            from ws_client import ValidatorWSClient
            ws_url = resolve_ws_url()
            auth_headers = signer.build_auth_headers("GET", ws_url, None)

            def _refresh_miner_ws_auth() -> dict[str, str]:
                return signer.build_auth_headers("GET", ws_url, None)

            ws_client = ValidatorWSClient(
                ws_url=ws_url,
                auth_headers=auth_headers,
                on_auth_refresh=_refresh_miner_ws_auth,
            )
            # Don't connect here — the receive loop will connect on start()
        except Exception as exc:
            import logging
            logging.getLogger("miner.ws").warning("WS setup failed, using HTTP polling: %s", exc)
            ws_client = None

    return AgentWorker(client=client, runner=runner, config=config, ws_client=ws_client)


def run_single_item_for_test(*, item: WorkItem, client: Any, runner: Any, root: Path) -> dict[str, Any]:
    writer = RunArtifactWriter(root / "run-artifacts")
    worker = AgentWorker(client=client, runner=runner, config=_build_test_config(root))
    terminal_state = worker._process_single_item_for_test(item, writer)
    return {"terminal_state": terminal_state}


def export_core_submissions(
    input_path: str,
    output_path: str,
    dataset_id: str,
    *,
    client: PlatformClient | Any | None = None,
) -> Path:
    input_file = Path(input_path)
    records = read_jsonl_file(input_file)
    generated_at = None
    manifest_path = input_file.parent / "run_manifest.json"
    if manifest_path.exists():
        manifest = read_json_file(manifest_path)
        if isinstance(manifest, dict):
            generated_at = optional_string(manifest.get("generated_at"))
    payload = build_submission_request(records, dataset_id=dataset_id, generated_at=generated_at)
    fetch_dataset = getattr(client, "fetch_dataset", None) if client is not None else None
    if callable(fetch_dataset) and records:
        dataset = fetch_dataset(dataset_id)
        first_record = records[0] if isinstance(records[0], dict) else {}
        item = WorkItem(
            item_id=f"export:{dataset_id}",
            source="local_file",
            url=str(first_record.get("canonical_url") or first_record.get("url") or ""),
            dataset_id=dataset_id,
            platform=optional_string(first_record.get("platform")) or "",
            resource_type=optional_string(first_record.get("resource_type")) or "",
            record={},
        )
        _augment_submission_payload_for_dataset(payload, dataset=dataset, record=first_record, item=item)
        _normalize_entry_urls(payload, dataset=dataset)
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return output


def _export_core_submissions_for_task(output_dir: Path, record: dict[str, Any], item: WorkItem) -> Path:
    dataset_id = optional_string(item.dataset_id)
    if not dataset_id:
        raise RuntimeError(f"item {item.item_id} is missing dataset_id for core submission export")
    export_path = output_dir / "core-submissions.json"
    generated_at = None
    manifest_path = output_dir / "run_manifest.json"
    if manifest_path.exists():
        manifest = read_json_file(manifest_path)
        if isinstance(manifest, dict):
            generated_at = optional_string(manifest.get("generated_at"))
    export_record = dict(record)
    export_record.setdefault("platform", item.platform)
    export_record.setdefault("resource_type", item.resource_type)
    payload = build_submission_request([export_record], dataset_id=dataset_id, generated_at=generated_at)
    export_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return export_path


def _export_and_submit_core_submissions_for_task(
    client: PlatformClient,
    output_dir: Path,
    record: dict[str, Any],
    item: WorkItem,
    *,
    report_result: dict[str, Any] | None = None,
) -> tuple[Path, Path]:
    export_path = _export_core_submissions_for_task(output_dir, record, item)
    payload = read_json_file(export_path)
    if not isinstance(payload, dict):
        raise RuntimeError(f"invalid core submission export payload at {export_path}")
    dataset_id = optional_string(item.dataset_id)
    fetch_dataset = getattr(client, "fetch_dataset", None)
    if dataset_id and callable(fetch_dataset):
        dataset = fetch_dataset(dataset_id)
        _augment_submission_payload_for_dataset(payload, dataset=dataset, record=record, item=item)
        _normalize_entry_urls(payload, dataset=dataset)
        export_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    response_path = output_dir / "core-submissions-response.json"
    submission_id = _extract_submission_id(report_result)
    if submission_id:
        response_data = _resolve_existing_submission_response(client, submission_id=submission_id, report_result=report_result)
        if response_data is not None:
            response_path.write_text(json.dumps(response_data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            return export_path, response_path

    # Pre-submit dedup check: verify structured_data hash is not already occupied
    dataset_id = optional_string(item.dataset_id)
    entries = payload.get("entries")
    if dataset_id and isinstance(entries, list):
        dedup_blocked: list[str] = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            entry_url = str(entry.get("url") or "")
            structured = entry.get("structured_data")
            if isinstance(structured, dict) and structured:
                try:
                    occupancy = client.check_url_occupancy(dataset_id, entry_url, structured_data=structured)
                    if occupancy.get("occupied"):
                        dedup_blocked.append(entry_url)
                except Exception:
                    pass  # dedup check is best-effort, don't block submission on failure
        if dedup_blocked:
            logging.getLogger("agent.submit").info(
                "dedup hash check blocked %d/%d entries: %s", len(dedup_blocked), len(entries), dedup_blocked
            )
            # Remove blocked entries from payload before submitting
            blocked_set = set(dedup_blocked)
            payload["entries"] = [e for e in entries if str(e.get("url") or "") not in blocked_set]
            if not payload["entries"]:
                response_path.write_text(json.dumps({"data": {"rejected": [{"url": u, "reason": "dedup_hash_conflict"} for u in dedup_blocked]}}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
                return export_path, response_path

    # Check submission gate before submitting — resolve PoW if in "checking" state
    try:
        gate = client.fetch_submission_gate()
        if isinstance(gate, dict) and gate.get("state") == "checking":
            challenge = gate.get("challenge")
            if isinstance(challenge, dict) and challenge:
                challenge_id = str(challenge.get("id") or "")
                if challenge_id:
                    answer = solve_challenge(challenge)
                    client.answer_pow_challenge(challenge_id, answer)
    except Exception:
        pass  # best-effort — submission will trigger challenge_required if still needed

    response = client.submit_core_submissions(payload)
    # Handle PoW challenge: if admission_status is challenge_required, answer and retry
    resp_data = response.get("data") if isinstance(response, dict) else None
    if isinstance(resp_data, dict) and resp_data.get("admission_status") == "challenge_required":
        challenge = resp_data.get("challenge")
        if isinstance(challenge, dict) and challenge:
            challenge_id = str(challenge.get("id") or "")
            if challenge_id:
                try:
                    answer = solve_challenge(challenge)
                    client.answer_pow_challenge(challenge_id, answer)
                    response = client.submit_core_submissions(payload)
                except UnsupportedChallenge:
                    pass  # Unsupported challenge type, keep original response
                except Exception:
                    pass  # PoW answer or retry failed, keep original response
    # Re-derive resp_data in case PoW retry updated response
    resp_data = response.get("data") if isinstance(response, dict) else None
    # If admission_status is still challenge_required after PoW attempt, raise so callers
    # don't count this as a successful submission
    if isinstance(resp_data, dict) and resp_data.get("admission_status") == "challenge_required":
        raise RuntimeError(f"submission requires PoW challenge that could not be resolved for {item.item_id}")
    # Check for submission_too_frequent in per-entry rejections
    if isinstance(resp_data, dict):
        rejected = resp_data.get("rejected")
        if isinstance(rejected, list):
            for entry in rejected:
                if isinstance(entry, dict) and entry.get("reason") == "submission_too_frequent":
                    import logging as _log
                    _log.getLogger("agent.submit").warning(
                        "submission_too_frequent for %s — back off before next submit",
                        entry.get("url", "?"),
                    )
    response_path.write_text(json.dumps(response, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return export_path, response_path


def _normalize_entry_urls(payload: dict[str, Any], *, dataset: dict[str, Any]) -> None:
    """Normalize URLs in submission entries using schema regex if provided."""
    schema = dataset.get("schema")
    regex_pattern = None
    if isinstance(schema, dict):
        regex_pattern = schema.get("url_normalize_regex")
        if regex_pattern is not None and not isinstance(regex_pattern, str):
            regex_pattern = None
    entries = payload.get("entries")
    if not isinstance(entries, list):
        return
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        url = entry.get("url")
        if isinstance(url, str) and url:
            entry["url"] = normalize_url(url, regex_pattern)


def _augment_submission_payload_for_dataset(
    payload: dict[str, Any],
    *,
    dataset: dict[str, Any],
    record: dict[str, Any],
    item: WorkItem,
) -> None:
    schema = dataset.get("schema")
    entries = payload.get("entries")
    if not isinstance(schema, dict) or not isinstance(entries, list):
        return
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        original_structured_data = entry.get("structured_data")
        if not isinstance(original_structured_data, dict):
            original_structured_data = {}
        schema_properties = schema.get("properties", {})
        if not isinstance(schema_properties, dict) or not schema_properties:
            # No schema properties available — keep original structured_data as-is
            continue
        structured_data: dict[str, Any] = {
            field_name: original_structured_data[field_name]
            for field_name in schema_properties
            if field_name in original_structured_data and original_structured_data[field_name] not in (None, "")
        }
        entry["structured_data"] = structured_data


def _extract_submission_id(report_result: dict[str, Any] | None) -> str | None:
    if not isinstance(report_result, dict):
        return None
    data = report_result.get("data")
    if isinstance(data, dict):
        return optional_string(data.get("submission_id"))
    return optional_string(report_result.get("submission_id"))


def _resolve_existing_submission_response(
    client: PlatformClient,
    *,
    submission_id: str,
    report_result: dict[str, Any] | None,
) -> dict[str, Any] | None:
    fetch_core_submission = getattr(client, "fetch_core_submission", None)
    if callable(fetch_core_submission):
        try:
            submission = fetch_core_submission(submission_id)
        except PlatformApiError as api_err:
            if api_err.status_code != 404:
                raise
            return None
        except httpx.HTTPStatusError as error:
            if error.response.status_code != 404:
                raise
            return None
        else:
            return {"data": [submission]}
    if isinstance(report_result, dict):
        return report_result
    return {"data": [{"id": submission_id}]}


def _safe_path_segment(value: str) -> str:
    slug = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in value)
    return slug or "item"


def _extract_retry_after_seconds(error: httpx.HTTPStatusError, *, default: int) -> int:
    header_value = error.response.headers.get("Retry-After", "").strip()
    try:
        return max(1, int(header_value))
    except ValueError:
        return max(1, default)


def _clone_item(item: WorkItem, *, resume: bool, output_dir: Path | None = None) -> WorkItem:
    payload = item.to_dict()
    payload["resume"] = resume
    if output_dir is not None:
        payload["output_dir"] = str(output_dir)
    return WorkItem.from_dict(payload)
