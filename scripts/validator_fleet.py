"""Validator fleet — 批量创建并运行 N 个 validator 实例。

每个 validator 使用独立的自动生成私钥，跳过 LLM 评估，随机打 70-90 分。
所有 validator 并行运行在独立线程中。

用法:
    # 启动 5 个 validator
    python scripts/validator_fleet.py start 5

    # 启动 10 个，使用自定义 platform URL
    PLATFORM_BASE_URL=https://api.minework.net python scripts/validator_fleet.py start 10

    # 查看已保存的私钥
    python scripts/validator_fleet.py list-keys

    # 清理所有私钥和状态
    python scripts/validator_fleet.py clean
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import random
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any

# 确保 scripts/ 在 sys.path 里
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from eth_account import Account
from pk_signer import PrivateKeySigner
from lib.platform_client import PlatformClient, PlatformApiError
from ws_client import ValidatorWSClient, WSMessage, WSDisconnected
from evaluation_engine import EvaluationResult
from common import resolve_platform_base_url, resolve_ws_url

logging.basicConfig(
    format="%(asctime)s %(levelname)-5s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
    stream=sys.stdout,
)
# 压制噪音库
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("websocket").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)

# ────────────────────────────────────────────────────────────────────
# 私钥管理
# ────────────────────────────────────────────────────────────────────

FLEET_DIR = PROJECT_ROOT / "output" / "validator-fleet"
KEYS_FILE = FLEET_DIR / "keys.json"


def _load_keys() -> list[dict[str, str]]:
    if not KEYS_FILE.exists():
        return []
    try:
        return json.loads(KEYS_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []


def _save_keys(keys: list[dict[str, str]]) -> None:
    FLEET_DIR.mkdir(parents=True, exist_ok=True)
    KEYS_FILE.write_text(json.dumps(keys, indent=2), encoding="utf-8")


def ensure_keys(n: int) -> list[dict[str, str]]:
    """确保至少有 n 个私钥，不足则自动生成。"""
    keys = _load_keys()
    while len(keys) < n:
        acct = Account.create()
        keys.append({
            "address": acct.address,
            "private_key": acct.key.hex(),
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        })
    _save_keys(keys)
    return keys[:n]


# ────────────────────────────────────────────────────────────────────
# 随机评估引擎（替代 LLM）
# ────────────────────────────────────────────────────────────────────


class RandomScoreEngine:
    """替代 EvaluationEngine，跳过 LLM，随机给 70-90 分。"""

    def evaluate(
        self,
        cleaned_data: str | dict[str, Any],
        structured_data: dict[str, Any],
        schema_fields: list[str],
        repeat_cleaned_data: str = "",
        dataset_schema: dict[str, Any] | None = None,
    ) -> EvaluationResult:
        score = random.randint(70, 90)
        return EvaluationResult(
            result="match",
            verdict="accepted",
            consistent=True,
            score=score,
        )


# ────────────────────────────────────────────────────────────────────
# 单个 Validator 实例
# ────────────────────────────────────────────────────────────────────

WS_RECEIVE_TIMEOUT = 30.0
HEARTBEAT_INTERVAL = 120


class ValidatorInstance:
    """轻量级 validator，复用核心协议但跳过 LLM。"""

    def __init__(self, *, index: int, private_key: str, address: str) -> None:
        self.index = index
        self.address = address
        self.log = logging.getLogger(f"v{index}:{address[:10]}")
        self._stop_event = threading.Event()

        # 签名器
        self._signer = PrivateKeySigner(private_key)

        # Platform client
        self._platform = PlatformClient(
            base_url=resolve_platform_base_url(),
            token="",
            signer=self._signer,
        )

        # WebSocket client
        ws_url = resolve_ws_url()
        auth_headers = self._signer.build_auth_headers("GET", ws_url, None)
        self._ws = ValidatorWSClient(
            ws_url=ws_url,
            auth_headers=auth_headers,
            on_auth_refresh=lambda: self._signer.build_auth_headers("GET", ws_url, None),
        )

        # 随机评分引擎
        self._engine = RandomScoreEngine()

        # 统计
        self._stats = {
            "tasks_received": 0,
            "tasks_evaluated": 0,
            "tasks_match": 0,
            "errors": 0,
        }
        self._lock = threading.Lock()
        self._platform_lock = threading.Lock()
        self._in_ready_pool = False
        self._min_task_interval = 30

    def _inc_stat(self, key: str) -> None:
        with self._lock:
            self._stats[key] = self._stats.get(key, 0) + 1

    def stats_snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self._stats)

    # ── 启动流程 ──

    def start(self) -> None:
        """在当前线程中运行 validator 主循环（阻塞）。"""
        self.log.info("Starting validator %s", self.address)

        # 1. 提交 application（若缺）
        try:
            with self._platform_lock:
                app = self._platform.get_my_validator_application()
            if not app or not app.get("status"):
                self.log.info("Submitting validator application")
                with self._platform_lock:
                    self._platform.submit_validator_application()
        except PlatformApiError as err:
            if err.status_code != 403:
                self.log.warning("Application check failed: %s", err)
        except Exception as exc:
            self.log.warning("Application check failed: %s", exc)

        # 2. 连接 WS
        try:
            self._ws.connect()
        except WSDisconnected:
            self.log.warning("Initial WS connect failed, will retry")

        # 3. 加入 ready pool
        self._try_join_ready_pool()

        # 4. 启动心跳线程
        heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, name=f"hb-v{self.index}", daemon=True
        )
        heartbeat_thread.start()

        # 5. 主消息循环
        self._main_loop()

        # 清理
        try:
            with self._platform_lock:
                self._platform.leave_ready_pool()
        except Exception:
            pass
        self._ws.close()
        self.log.info("Validator stopped. Stats: %s", self.stats_snapshot())

    def stop(self) -> None:
        self._stop_event.set()
        self._ws.close()

    # ── 心跳 ──

    def _heartbeat_loop(self) -> None:
        ws_skip_counter = 0
        while not self._stop_event.is_set():
            send_hb = True
            if self._ws.connected:
                ws_skip_counter += 1
                send_hb = ws_skip_counter >= 5
                if send_hb:
                    ws_skip_counter = 0
            else:
                ws_skip_counter = 0
            if send_hb:
                try:
                    with self._platform_lock:
                        hb = self._platform.send_unified_heartbeat(
                            client_name=f"mine-fleet-v{self.index}",
                        )
                    data = hb.get("data") if isinstance(hb.get("data"), dict) else hb
                    interval = data.get("min_task_interval_seconds")
                    if isinstance(interval, (int, float)) and interval > 0:
                        self._min_task_interval = int(interval)
                except Exception as exc:
                    self.log.warning("Heartbeat failed: %s", exc)

            if not self._in_ready_pool:
                self._try_join_ready_pool()

            self._stop_event.wait(timeout=HEARTBEAT_INTERVAL)

    def _try_join_ready_pool(self) -> None:
        try:
            with self._platform_lock:
                self._platform.join_ready_pool()
            if not self._in_ready_pool:
                self.log.info("Joined ready pool")
            self._in_ready_pool = True
        except PlatformApiError as err:
            if err.status_code == 409:
                # 409 = already in ready pool, not an error
                self._in_ready_pool = True
            elif err.status_code == 403:
                self.log.error("Cannot join ready pool (403): insufficient stake?")
            else:
                self.log.warning("join_ready_pool failed: %s", err)
        except Exception as exc:
            status = getattr(getattr(exc, "response", None), "status_code", 0)
            if status == 409:
                self._in_ready_pool = True
            else:
                self.log.warning("join_ready_pool failed: %s", exc)

    # ── 主消息循环 ──

    def _main_loop(self) -> None:
        consecutive_ws_failures = 0
        while not self._stop_event.is_set():
            # 重连
            if not self._ws.connected:
                # WS 断连意味着平台可能已驱逐此 validator
                self._in_ready_pool = False
                try:
                    self._ws.reconnect_with_backoff()
                except Exception:
                    pass
                if self._ws.connected:
                    consecutive_ws_failures = 0
                    # 重连后立即重新加入 ready pool
                    self._try_join_ready_pool()
                else:
                    consecutive_ws_failures += 1
                    if consecutive_ws_failures >= 3:
                        self._poll_http()
                        consecutive_ws_failures = 3
                    if self._stop_event.wait(timeout=5):
                        break
                    continue

            # 接收消息
            try:
                msg = self._ws.receive(timeout=WS_RECEIVE_TIMEOUT)
            except WSDisconnected:
                consecutive_ws_failures += 1
                continue

            if msg is None:
                continue

            consecutive_ws_failures = 0

            if msg.type == "evaluation_task":
                self._handle_task(msg)
            elif msg.type == "cooldown":
                retry = int(msg.data.get("retry_after_seconds", 30))
                self.log.info("Cooldown %ds", retry)
                self._stop_event.wait(timeout=retry)
            elif msg.type == "error":
                code = str(msg.raw.get("code") or "")
                retry = msg.raw.get("retry_after_seconds")
                self.log.warning("WS error: %s", code)
                if code == "validator_cooldown" and isinstance(retry, (int, float)) and retry > 0:
                    self._stop_event.wait(timeout=int(retry))

    def _poll_http(self) -> None:
        """HTTP polling fallback."""
        try:
            with self._platform_lock:
                claim = self._platform.claim_evaluation_task()
            if not claim:
                return
            if isinstance(claim, dict) and claim.get("_cooldown"):
                self._stop_event.wait(timeout=int(claim.get("retry_after_seconds", 30)))
                return
            if isinstance(claim, dict) and claim.get("_pow_required"):
                self.log.warning("PoW challenge received — fleet uses random scoring, cannot solve. Waiting.")
                self._stop_event.wait(timeout=60)
                return
            msg = WSMessage({"type": "evaluation_task", "data": claim})
            self._handle_task(msg, via_http=True)
        except Exception as exc:
            if "404" not in str(exc) and "409" not in str(exc):
                self.log.debug("HTTP poll failed: %s", exc)

    # ── 评估处理 ──

    def _handle_task(self, msg: WSMessage, *, via_http: bool = False) -> None:
        task_id = msg.task_id
        self._inc_stat("tasks_received")

        # HTTP POST /evaluation-tasks/claim → get assignment_id + full data
        if via_http:
            claim_data = msg.data
        else:
            try:
                with self._platform_lock:
                    claim_data = self._platform.claim_evaluation_task()
                if not claim_data:
                    return
                if claim_data.get("_cooldown"):
                    retry = int(claim_data.get("retry_after_seconds", 30))
                    self.log.info("Cooldown on WS claim: %ds", retry)
                    self._stop_event.wait(timeout=retry)
                    return
                if claim_data.get("_pow_required"):
                    self.log.warning("PoW challenge on WS claim — fleet cannot solve, waiting 60s")
                    self._stop_event.wait(timeout=60)
                    return
            except Exception as exc:
                self.log.warning("Claim failed: %s", exc)
                self._inc_stat("errors")
                return

        assignment_id = str(claim_data.get("assignment_id") or "")
        task_id = str(claim_data.get("task_id") or task_id)
        if not assignment_id:
            self.log.warning("No assignment_id from claim for %s", task_id)
            return

        # 随机评分
        result = self._engine.evaluate(
            str(claim_data.get("cleaned_data") or ""),
            claim_data.get("structured_data") or {},
            claim_data.get("schema_fields") or [],
        )
        self._inc_stat("tasks_evaluated")

        # 上报
        try:
            with self._platform_lock:
                self._platform.report_evaluation(
                    task_id, result.score,
                    assignment_id=assignment_id,
                    result=result.result,
                )
            self._inc_stat("tasks_match")
            self.log.info(
                "Reported: task=%s score=%d assignment=%s",
                task_id[:12], result.score, assignment_id[:12],
            )
        except Exception as exc:
            self._inc_stat("errors")
            self.log.error("Report failed: %s", exc)

        # 冷却
        if self._min_task_interval > 0:
            self._stop_event.wait(timeout=self._min_task_interval)


# ────────────────────────────────────────────────────────────────────
# Fleet 管理器
# ────────────────────────────────────────────────────────────────────


class ValidatorFleet:
    """管理 N 个并行 validator 实例。"""

    def __init__(self, count: int) -> None:
        self.count = count
        self.instances: list[ValidatorInstance] = []
        self.threads: list[threading.Thread] = []
        self._stop_event = threading.Event()
        self._auto_updater: Any = None

    def start(self) -> None:
        keys = ensure_keys(self.count)
        print(f"\n{'='*60}")
        print(f"  Validator Fleet - {self.count} instances")
        print(f"  Platform: {resolve_platform_base_url()}")
        print(f"  Scoring: random 70-90 (no LLM)")
        print(f"{'='*60}")
        for i, key in enumerate(keys):
            print(f"  [{i}] {key['address']}")
        print(f"{'='*60}\n")

        for i, key in enumerate(keys):
            instance = ValidatorInstance(
                index=i,
                private_key=key["private_key"],
                address=key["address"],
            )
            self.instances.append(instance)
            thread = threading.Thread(
                target=instance.start,
                name=f"validator-{i}",
                daemon=True,
            )
            self.threads.append(thread)

        # 错开启动，避免同时请求平台
        for i, thread in enumerate(self.threads):
            thread.start()
            if i < len(self.threads) - 1:
                time.sleep(2)

        # Start auto-updater
        try:
            from auto_updater import AutoUpdater
            project_root = Path(__file__).resolve().parents[1]
            self._auto_updater = AutoUpdater(
                project_root,
                on_update_applied=self.stop,  # update → gracefully stop fleet
            )
            self._auto_updater.start()
        except Exception as exc:
            print(f"Auto-updater start failed: {exc}")

        print(f"All {self.count} validators started. Press Ctrl+C to stop.\n")

        # 定期打印汇总
        try:
            while not self._stop_event.is_set():
                self._stop_event.wait(timeout=60)
                if not self._stop_event.is_set():
                    self._print_summary()
        except KeyboardInterrupt:
            pass

        self.stop()

    def stop(self) -> None:
        print("\nStopping all validators...")
        if self._auto_updater is not None:
            try:
                self._auto_updater.stop()
            except Exception:
                pass
        self._stop_event.set()
        for instance in self.instances:
            instance.stop()
        for thread in self.threads:
            thread.join(timeout=15)
        self._print_summary()
        print("Fleet stopped.")

    def _print_summary(self) -> None:
        total_received = 0
        total_evaluated = 0
        total_match = 0
        total_errors = 0
        for inst in self.instances:
            s = inst.stats_snapshot()
            total_received += s.get("tasks_received", 0)
            total_evaluated += s.get("tasks_evaluated", 0)
            total_match += s.get("tasks_match", 0)
            total_errors += s.get("errors", 0)
        print(
            f"[Fleet] received={total_received} evaluated={total_evaluated} "
            f"reported={total_match} errors={total_errors}"
        )


# ────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(description="Validator Fleet Manager")
    sub = parser.add_subparsers(dest="command")

    start_p = sub.add_parser("start", help="Start N validators in parallel")
    start_p.add_argument("count", type=int, help="Number of validators")

    sub.add_parser("list-keys", help="Show saved private keys")
    sub.add_parser("clean", help="Delete all keys and state")

    args = parser.parse_args()

    if args.command == "start":
        if args.count < 1:
            print("Count must be >= 1")
            return 1
        fleet = ValidatorFleet(args.count)

        def handle_signal(sig: int, frame: Any) -> None:
            fleet.stop()
            raise SystemExit(0)

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)
        fleet.start()
        return 0

    if args.command == "list-keys":
        keys = _load_keys()
        if not keys:
            print("No keys found.")
            return 0
        print(f"\n{len(keys)} saved validator keys:\n")
        for i, k in enumerate(keys):
            print(f"  [{i}] {k['address']}  created={k.get('created_at', '?')}")
        print()
        return 0

    if args.command == "clean":
        if FLEET_DIR.exists():
            import shutil
            shutil.rmtree(FLEET_DIR)
            print(f"Cleaned: {FLEET_DIR}")
        else:
            print("Nothing to clean.")
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
