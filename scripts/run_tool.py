from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from common import (
    DEFAULT_MINER_ID,
    DEFAULT_PLATFORM_BASE_URL,
    WALLET_SESSION_DURATION_SECONDS,
    resolve_local_venv_python,
    resolve_miner_id,
    resolve_platform_base_url,
    resolve_wallet_bin,
    resolve_wallet_config,
)
from install_guidance import awp_wallet_install_steps

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_ROOT = SCRIPT_DIR.parent


def inject_skill_root() -> Path:
    """Allow direct script execution to import sibling packages like ``lib``."""
    root_str = str(SKILL_ROOT)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)
    return SKILL_ROOT


inject_skill_root()


def _ensure_local_venv_python() -> None:
    if os.environ.get("MINE_SKIP_VENV_REEXEC") == "1":
        return
    local_venv_python = resolve_local_venv_python(SKILL_ROOT)
    if local_venv_python is None:
        return
    current = Path(sys.executable).resolve()
    target = local_venv_python.resolve()
    if current == target:
        return
    env = os.environ.copy()
    env["MINE_SKIP_VENV_REEXEC"] = "1"
    argv = [str(target), __file__, *sys.argv[1:]]
    if os.name == "nt":
        result = subprocess.run(argv, env=env)
        raise SystemExit(result.returncode)
    os.execve(str(target), argv, env)


_ensure_local_venv_python()

# Verify critical dependencies are available
try:
    import bs4  # noqa: F401
except ImportError:
    print(
        "ERROR: Required package 'beautifulsoup4' is not installed.\n"
        "Run bootstrap first:  bash scripts/bootstrap.sh\n"
        "Or install manually:  pip install -r requirements-core.txt",
        file=sys.stderr,
    )
    raise SystemExit(1)


def render_env_check() -> str:
    """Check and display all environment variables needed by Mine."""
    lines = ["Environment Variable Check", "=" * 40, ""]

    auto_defaults = [
        ("PLATFORM_BASE_URL", resolve_platform_base_url(), f"default: {DEFAULT_PLATFORM_BASE_URL}"),
        ("MINER_ID", resolve_miner_id(), f"default: {DEFAULT_MINER_ID}"),
    ]

    optional = [
        ("AWP_WALLET_TOKEN", "Optional explicit wallet session token override"),
        ("AWP_WALLET_BIN", f"Resolved awp-wallet binary (current: {resolve_wallet_bin()})"),
        ("SOCIAL_CRAWLER_ROOT", "Mine runtime root (default: auto-detected)"),
        ("OPENCLAW_GATEWAY_BASE_URL", "LLM gateway for PoW challenges"),
        ("WORKER_MAX_PARALLEL", "Concurrent crawl workers (default: 3)"),
        ("DATASET_REFRESH_SECONDS", "Dataset refresh interval (default: 900)"),
    ]

    lines.append("Auto defaults:")
    for name, value, desc in auto_defaults:
        display = value if len(value) < 50 else value[:47] + "..."
        source = "env" if os.environ.get(name, "").strip() else "default"
        lines.append(f"  ✓ {name} = {display} ({source})")
        lines.append(f"      {desc}")

    lines.append("")
    lines.append("Optional:")
    for name, desc in optional:
        value = os.environ.get(name, "").strip()
        if value:
            if "TOKEN" in name or "KEY" in name:
                display = value[:8] + "..." if len(value) > 8 else "***"
            else:
                display = value if len(value) < 40 else value[:37] + "..."
            lines.append(f"  ✓ {name} = {display}")
        else:
            lines.append(f"  · {name} — not set (optional)")

    lines.append("")
    lines.append("✓ Mine can run without a .env file. Environment variables only override defaults.")

    return "\n".join(lines)


def _bootstrap_command() -> str:
    if os.name == "nt":
        return "powershell -ExecutionPolicy Bypass -File .\\scripts\\bootstrap.ps1"
    return "./scripts/bootstrap.sh"


def _configure_background_logging() -> None:
    """Route INFO+ log records to stdout for the background worker.

    The default Python root logger only emits WARNING+ to stderr, which made
    every log.info() call in the mining loop silently disappear — producing a
    0-byte log file that looked like a stuck worker. By installing a plain
    stream handler on the root logger we capture every INFO message into
    whatever stdout is pointing at (the background worker's log file, after
    Popen redirection).

    Called only from the `run-worker` subcommand. Does NOT interfere with
    interactive commands, which keep Python's default silent behavior.
    """
    import logging

    root = logging.getLogger()
    # Guard against double-configuration when the worker re-execs itself.
    if getattr(root, "_mine_bg_configured", False):
        return

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s %(levelname)-5s %(name)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    )
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    # Noisy third-party libraries we don't want in the log.
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("websocket").setLevel(logging.WARNING)
    root._mine_bg_configured = True  # type: ignore[attr-defined]


def _project_root() -> Path:
    return SCRIPT_DIR.parent


def _default_output_root() -> Path:
    return Path(os.environ.get("CRAWLER_OUTPUT_ROOT", str(_project_root() / "output" / "agent-runs"))).resolve()


def _default_state_root() -> Path:
    return Path(os.environ.get("WORKER_STATE_ROOT", str(_default_output_root() / "_worker_state"))).resolve()


def _default_browser_auth_root() -> Path:
    return (_default_output_root() / "_browser_auth").resolve()


def _background_session_snapshot() -> dict[str, object]:
    from background_worker import process_is_running
    from worker_state import WorkerStateStore

    store = WorkerStateStore(_default_state_root())
    payload = store.load_background_session()
    if not payload:
        return {}
    pid = int(payload.get("pid") or 0)
    payload["pid"] = pid
    payload["running"] = process_is_running(pid)
    return payload


def _browser_auth_dir(platform: str) -> Path:
    return _default_browser_auth_root() / platform


def _browser_auth_state_path(platform: str) -> Path:
    return _browser_auth_dir(platform) / "status.json"


def _browser_auth_log_path(platform: str) -> Path:
    return _browser_auth_dir(platform) / "waiter.log"


def _read_browser_auth_state(platform: str) -> dict[str, Any]:
    path = _browser_auth_state_path(platform)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _write_browser_auth_state(platform: str, payload: dict[str, Any]) -> None:
    path = _browser_auth_state_path(platform)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload["updated_at"] = int(time.time())
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _browser_waiter_running(payload: dict[str, Any]) -> bool:
    from background_worker import process_is_running

    pid = int(payload.get("waiter_pid") or 0)
    return process_is_running(pid)


def _browser_session_payload(
    *,
    session: Any,
    state: str,
    message: str,
    target_session_path: str = "",
    error: str = "",
    retryable: bool = False,
) -> dict[str, Any]:
    return {
        "platform": session.platform,
        "state": state,
        "message": message,
        "public_url": session.public_url,
        "switch_token": session.switch_token,
        "login_url": session.login_url,
        "session_path": str(session.session_path),
        "target_session_path": target_session_path,
        "requires_user_action": session.requires_user_action,
        "started_by_bridge": session.started_by_bridge,
        "cleanup_performed": session.cleanup_performed,
        "local_browser_mode": session.local_browser_mode,
        "guide_active": session.guide_active,
        "error": error,
        "retryable": retryable,
        "created_at": int(time.time()),
        "waiter_pid": 0,
    }


def _copy_browser_session_output(source_path: Path, target_path_text: str) -> Path:
    if not target_path_text:
        return source_path.resolve()
    target_path = Path(target_path_text).resolve()
    if target_path == source_path.resolve():
        return target_path
    target_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, target_path)
    return target_path


def _spawn_browser_session_waiter(platform: str) -> int:
    from background_worker import _creationflags

    log_path = _browser_auth_log_path(platform)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        # Filter env to exclude secrets from the waiter subprocess
        safe_env = {k: v for k, v in os.environ.items()
                    if k not in {"AWP_WALLET_TOKEN", "VALIDATOR_PRIVATE_KEY", "AWP_WALLET_TOKEN_SECRET_REF"}}
        safe_env["MINE_SKIP_VENV_REEXEC"] = "1"
        process = subprocess.Popen(
            [sys.executable, __file__, "browser-session-wait", platform],
            cwd=SKILL_ROOT,
            stdout=handle,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
            creationflags=_creationflags(),
            env=safe_env,
        )
    return process.pid


def _payload_to_browser_session(payload: dict[str, Any]) -> Any:
    from crawler.integrations.browser_auth import AutoBrowserSession

    return AutoBrowserSession(
        platform=str(payload.get("platform") or ""),
        session_path=Path(str(payload.get("session_path") or "")),
        public_url=str(payload.get("public_url") or ""),
        switch_token=str(payload.get("switch_token") or ""),
        login_url=str(payload.get("login_url") or ""),
        requires_user_action=bool(payload.get("requires_user_action", False)),
        started_by_bridge=bool(payload.get("started_by_bridge", False)),
        cleanup_performed=bool(payload.get("cleanup_performed", False)),
        local_browser_mode=bool(payload.get("local_browser_mode", False)),
        guide_active=bool(payload.get("guide_active", False)),
    )


def _browser_session_response(
    *,
    platform: str,
    state: str,
    user_message: str,
    user_actions: list[str],
    public_url: str = "",
    login_url: str = "",
    session_path: str = "",
    waiter_pid: int = 0,
    waiter_running: bool = False,
    cleanup_performed: bool = False,
    error: str = "",
    retryable: bool = False,
    status_command: str = "",
    extra_internal: dict[str, Any] | None = None,
) -> str:
    internal = {
        "platform": platform,
        "session_path": session_path,
        "public_url": public_url,
        "login_url": login_url,
        "waiter_pid": waiter_pid,
        "waiter_running": waiter_running,
        "cleanup_performed": cleanup_performed,
        "error": error,
        "retryable": retryable,
        "status_command": status_command,
    }
    if extra_internal:
        internal.update(extra_internal)
    return json.dumps({
        "status": state,
        "state": state,
        "platform": platform,
        "user_message": user_message,
        "user_actions": user_actions,
        "public_url": public_url,
        "login_url": login_url,
        "session_path": session_path,
        "waiter_pid": waiter_pid,
        "waiter_running": waiter_running,
        "cleanup_performed": cleanup_performed,
        "error": error,
        "retryable": retryable,
        "status_command": status_command,
        "_internal": internal,
    }, ensure_ascii=False, indent=2)


def run_browser_session(platform: str, output_path: str = "") -> str:
    from crawler.integrations.browser_auth import AutoBrowserAuthBridge, AutoBrowserAuthError
    from crawler.integrations.browser_auth import get_default_auto_browser_script, get_default_auto_browser_workdir

    normalized_platform = (platform or "").strip().lower()
    if not normalized_platform:
        return _browser_session_response(
            platform="",
            state="error",
            user_message="Platform is required.",
            user_actions=[],
            error="missing_platform",
        )

    existing = _read_browser_auth_state(normalized_platform)
    if existing.get("state") == "awaiting_user_action" and _browser_waiter_running(existing):
        return run_browser_session_status(normalized_platform)

    output_dir = _browser_auth_dir(normalized_platform)
    target_session_path: Path | None = None
    if output_path:
        target_session_path = Path(output_path).resolve()
        output_dir = target_session_path.parent

    bridge = AutoBrowserAuthBridge(
        script_path=get_default_auto_browser_script(),
        workdir=get_default_auto_browser_workdir(),
    )

    try:
        session = bridge.prepare_session(
            platform=normalized_platform,
            output_dir=output_dir,
            cleanup_on_success=True,
        )
    except AutoBrowserAuthError as exc:
        fetch_error = getattr(exc, "fetch_error", None)
        return _browser_session_response(
            platform=normalized_platform,
            state="error",
            user_message="Browser session was not completed.",
            user_actions=["Retry browser session", "Diagnose"],
            public_url=getattr(exc, "public_url", ""),
            login_url=getattr(exc, "login_url", ""),
            error=getattr(fetch_error, "error_code", "AUTH_AUTO_LOGIN_FAILED"),
            retryable=bool(getattr(fetch_error, "retryable", False)),
            extra_internal={
                "message": str(exc),
                "next_action": getattr(fetch_error, "agent_hint", ""),
            },
        )
    except Exception as exc:
        return _browser_session_response(
            platform=normalized_platform,
            state="error",
            user_message="Browser session setup failed.",
            user_actions=["Retry browser session", "Diagnose"],
            error="browser_session_failed",
            extra_internal={"message": str(exc)},
        )

    final_session_path = session.session_path.resolve()
    target_path_text = str(target_session_path) if target_session_path is not None else ""
    if not session.requires_user_action:
        final_session_path = _copy_browser_session_output(final_session_path, target_path_text)
        payload = _browser_session_payload(
            session=session,
            state="ready",
            message="Browser session is ready and the browser stack has been cleaned up.",
            target_session_path=str(final_session_path),
        )
        _write_browser_auth_state(normalized_platform, payload)
        return _browser_session_response(
            platform=normalized_platform,
            state="ready",
            user_message=payload["message"],
            user_actions=["Continue task"],
            public_url=session.public_url,
            login_url=session.login_url,
            session_path=str(final_session_path),
            cleanup_performed=session.cleanup_performed,
            status_command=f"python scripts/run_tool.py browser-session-status {normalized_platform}",
            extra_internal={
                "requires_user_action": session.requires_user_action,
                "started_by_bridge": session.started_by_bridge,
            },
        )

    payload = _browser_session_payload(
        session=session,
        state="awaiting_user_action",
        message=(
            "Open the temporary browser link and complete login."
            if session.public_url
            else "Complete login in the opened local browser."
        ),
        target_session_path=target_path_text,
    )
    _write_browser_auth_state(normalized_platform, payload)
    waiter_pid = _spawn_browser_session_waiter(normalized_platform)
    payload["waiter_pid"] = waiter_pid
    _write_browser_auth_state(normalized_platform, payload)
    return _browser_session_response(
        platform=normalized_platform,
        state="awaiting_user_action",
        user_message=payload["message"],
        user_actions=["Open login link", "Check browser session status"] if session.public_url else ["Complete login in browser", "Check browser session status"],
        public_url=session.public_url,
        login_url=session.login_url,
        session_path=target_path_text or str(final_session_path),
        waiter_pid=waiter_pid,
        waiter_running=True,
        cleanup_performed=False,
        status_command=f"python scripts/run_tool.py browser-session-status {normalized_platform}",
        extra_internal={
            "requires_user_action": True,
        },
    )


def run_browser_session_wait(platform: str) -> str:
    from crawler.integrations.browser_auth import AutoBrowserAuthBridge, AutoBrowserAuthError
    from crawler.integrations.browser_auth import get_default_auto_browser_script, get_default_auto_browser_workdir

    normalized_platform = (platform or "").strip().lower()
    payload = _read_browser_auth_state(normalized_platform)
    if not payload:
        return json.dumps({"state": "error", "message": "No pending browser session."}, ensure_ascii=False, indent=2)
    if payload.get("state") != "awaiting_user_action":
        return json.dumps(payload, ensure_ascii=False, indent=2)

    bridge = AutoBrowserAuthBridge(
        script_path=get_default_auto_browser_script(),
        workdir=get_default_auto_browser_workdir(),
    )
    session = _payload_to_browser_session(payload)

    try:
        completed = bridge.complete_prepared_session(session, cleanup_on_success=True)
        final_session_path = _copy_browser_session_output(completed.session_path.resolve(), str(payload.get("target_session_path") or ""))
        ready_payload = _browser_session_payload(
            session=completed,
            state="ready",
            message="Browser session is ready and the browser stack has been cleaned up.",
            target_session_path=str(final_session_path),
        )
        ready_payload["waiter_pid"] = int(payload.get("waiter_pid") or 0)
        _write_browser_auth_state(normalized_platform, ready_payload)
        return json.dumps(ready_payload, ensure_ascii=False, indent=2)
    except AutoBrowserAuthError as exc:
        fetch_error = getattr(exc, "fetch_error", None)
        error_payload = dict(payload)
        error_payload.update({
            "state": "error",
            "message": str(exc),
            "error": getattr(fetch_error, "error_code", "AUTH_SESSION_EXPORT_FAILED"),
            "retryable": bool(getattr(fetch_error, "retryable", False)),
            "public_url": getattr(exc, "public_url", "") or payload.get("public_url", ""),
            "login_url": getattr(exc, "login_url", "") or payload.get("login_url", ""),
            "guide_active": False,
        })
        _write_browser_auth_state(normalized_platform, error_payload)
        return json.dumps(error_payload, ensure_ascii=False, indent=2)
    except Exception as exc:
        error_payload = dict(payload)
        error_payload.update({
            "state": "error",
            "message": str(exc),
            "error": "browser_session_wait_failed",
            "retryable": False,
            "guide_active": False,
        })
        _write_browser_auth_state(normalized_platform, error_payload)
        return json.dumps(error_payload, ensure_ascii=False, indent=2)


def run_browser_session_status(platform: str) -> str:
    normalized_platform = (platform or "").strip().lower()
    if not normalized_platform:
        return _browser_session_response(
            platform="",
            state="error",
            user_message="Platform is required.",
            user_actions=[],
            error="missing_platform",
        )

    payload = _read_browser_auth_state(normalized_platform)
    if not payload:
        return _browser_session_response(
            platform=normalized_platform,
            state="idle",
            user_message="No browser session job is active.",
            user_actions=["Start browser session"],
            status_command=f"python scripts/run_tool.py browser-session-status {normalized_platform}",
        )

    waiter_running = _browser_waiter_running(payload) if payload.get("state") == "awaiting_user_action" else False
    user_actions = ["Continue task"]
    if payload.get("state") == "awaiting_user_action":
        user_actions = ["Open login link", "Check again"] if payload.get("public_url") else ["Complete login in browser", "Check again"]
    elif payload.get("state") == "error":
        user_actions = ["Retry browser session", "Diagnose"]

    return _browser_session_response(
        platform=normalized_platform,
        state=str(payload.get("state", "idle")),
        user_message=str(payload.get("message", "No browser session job is active.")),
        user_actions=user_actions,
        public_url=str(payload.get("public_url", "")),
        login_url=str(payload.get("login_url", "")),
        session_path=str(payload.get("target_session_path") or payload.get("session_path", "")),
        waiter_pid=int(payload.get("waiter_pid") or 0),
        waiter_running=waiter_running,
        cleanup_performed=bool(payload.get("cleanup_performed", False)),
        error=str(payload.get("error", "")),
        retryable=bool(payload.get("retryable", False)),
        status_command=f"python scripts/run_tool.py browser-session-status {normalized_platform}",
    )


def run_diagnosis() -> str:
    """Run comprehensive diagnosis for 401 and connectivity issues."""
    lines = ["Mine Diagnosis", "=" * 40, ""]

    # 1. Check environment
    lines.append("1. Environment Variables")
    lines.append("-" * 30)
    platform_url = resolve_platform_base_url()
    miner_id = resolve_miner_id()
    _wallet_bin, wallet_token = resolve_wallet_config()

    if platform_url:
        lines.append(f"  ✓ PLATFORM_BASE_URL = {platform_url}")
    else:
        lines.append("  ✗ PLATFORM_BASE_URL — NOT SET")
        lines.append("    Fix: export PLATFORM_BASE_URL=https://api.minework.net")

    if miner_id:
        lines.append(f"  ✓ MINER_ID = {miner_id}")
    else:
        lines.append("  ✗ MINER_ID — NOT SET")

    if wallet_token:
        lines.append(f"  ✓ Wallet session = {wallet_token[:8]}...")
    else:
        lines.append("  ! Wallet session — not currently loaded (Mine will manage it automatically)")

    lines.append("")

    # 2. Check awp-wallet
    lines.append("2. AWP Wallet Status")
    lines.append("-" * 30)
    from common import format_wallet_bin_display, resolve_wallet_bin

    configured_wallet_bin = os.environ.get("AWP_WALLET_BIN", "awp-wallet").strip() or "awp-wallet"
    wallet_bin = resolve_wallet_bin()
    wallet_label = format_wallet_bin_display(configured_wallet_bin)
    wallet_found = bool(shutil.which(wallet_bin) or Path(wallet_bin).exists())

    if not wallet_found:
        lines.append(f"  ✗ awp-wallet not found: {wallet_label}")
        lines.append("    Fix:")
        for step in awp_wallet_install_steps():
            lines.append(f"      {step}")
        return "\n".join(lines)

    lines.append(f"  ✓ awp-wallet found: {wallet_label}")

    # Try to get wallet address
    import subprocess
    try:
        env = os.environ.copy()
        if not env.get("HOME") and env.get("USERPROFILE"):
            env["HOME"] = env["USERPROFILE"]
        result = subprocess.run(
            [wallet_bin, "receive"],
            capture_output=True, text=True, timeout=10, env=env
        )
        if result.returncode == 0:
            data = json.loads(result.stdout)
            address = data.get("address") or data.get("eoaAddress") or ""
            if not address:
                addresses = data.get("addresses", [])
                if addresses and isinstance(addresses[0], dict):
                    address = addresses[0].get("address", "")
            if address:
                lines.append(f"  ✓ Wallet address: {address}")
            else:
                lines.append("  ! Could not get wallet address from response")
        else:
            lines.append(f"  ✗ awp-wallet receive failed: {result.stderr.strip()}")
    except Exception as exc:
        lines.append(f"  ✗ Error checking wallet: {exc}")

    lines.append("")

    # 3. Test platform connectivity
    lines.append("3. Platform Connectivity")
    lines.append("-" * 30)

    if not platform_url:
        lines.append("  ✗ Cannot test — PLATFORM_BASE_URL not set")
        return "\n".join(lines)

    import httpx
    try:
        # Test basic connectivity (no auth)
        response = httpx.get(f"{platform_url}/health", timeout=10)
        lines.append(f"  ✓ Platform reachable: {response.status_code}")
    except httpx.ConnectError:
        lines.append(f"  ✗ Cannot connect to {platform_url}")
        lines.append("    Check: Is the platform URL correct? Is your network working?")
        return "\n".join(lines)
    except Exception as exc:
        lines.append(f"  ! Health check: {exc}")

    lines.append("")

    # 4. Test authenticated endpoint
    lines.append("4. Authentication Test (Heartbeat)")
    lines.append("-" * 30)

    try:
        from agent_runtime import build_worker_from_env

        worker = build_worker_from_env()

        # Try heartbeat
        try:
            worker.client.send_miner_heartbeat(client_name=worker.config.client_name)
            lines.append("  ✓ Heartbeat successful — authentication working!")
        except httpx.HTTPStatusError as error:
            status = error.response.status_code
            lines.append(f"  ✗ Heartbeat failed: HTTP {status}")

            # Parse error response for details
            try:
                error_payload = error.response.json()
                error_body = error_payload.get("error", {})
                error_code = error_body.get("code", "")
                error_msg = error_body.get("message", "")

                lines.append("")
                lines.append("  Error details:")
                if error_code:
                    lines.append(f"    Code: {error_code}")
                if error_msg:
                    lines.append(f"    Message: {error_msg}")

                lines.append("")
                lines.append("  Possible causes:")

                if status == 401:
                    if error_code == "MISSING_HEADERS":
                        lines.append("    → Missing signature headers")
                        lines.append("    Fix: rerun bootstrap or refresh the wallet session with awp-wallet unlock --duration 3600 --scope full")
                    elif error_code in {"INVALID_SIGNATURE", "SIGNATURE_MISMATCH"}:
                        lines.append("    → Signature format/content mismatch")
                        lines.append("    This may indicate platform-side signature verification changed")
                    elif error_code in {"TOKEN_EXPIRED", "SESSION_EXPIRED", "UNAUTHORIZED"}:
                        lines.append("    → Session token expired")
                        lines.append("    Fix: refresh the wallet session with awp-wallet unlock --duration 3600 --scope full")
                    elif error_code == "WALLET_NOT_REGISTERED":
                        lines.append("    → This wallet is not registered on-chain")
                        lines.append("    Fix: Install and use the AWP Skill to complete registration, then retry")
                    elif error_code == "WALLET_BANNED":
                        lines.append("    → This wallet has been banned")
                        lines.append("    Contact: Platform support")
                    else:
                        lines.append("    → Unknown 401 error")
                        lines.append("    • Auto-managed wallet session may be expired — try: awp-wallet unlock --duration 3600 --scope full")
                        lines.append("    • Wallet may not be registered on platform")
                        lines.append("    • Platform signature requirements may have changed")

            except Exception:
                lines.append("    Could not parse error response")
                lines.append(f"    Raw: {error.response.text[:200]}")

        except RuntimeError as exc:
            lines.append(f"  ✗ Runtime error: {exc}")

    except Exception as exc:
        lines.append(f"  ✗ Could not initialize worker: {exc}")

    lines.append("")
    lines.append("=" * 40)
    lines.append("Diagnosis complete.")

    return "\n".join(lines)


def run_doctor() -> str:
    """Run doctor command - simpler diagnosis with exact fix commands (JSON output).

    Uses the unified readiness contract from common.resolve_runtime_readiness().
    """
    import subprocess

    from common import format_wallet_bin_display, resolve_runtime_readiness

    # Get unified readiness state
    readiness = resolve_runtime_readiness()

    result = {
        "status": "ok" if readiness["can_start"] else "error",
        "readiness": {
            "state": readiness["state"],
            "can_diagnose": readiness["can_diagnose"],
            "can_start": readiness["can_start"],
            "can_mine": readiness["can_mine"],
        },
        "warnings": readiness.get("warnings", []),
        "checks": [],
        "_internal": {
            "fix_commands": [],
            "next_command": None,
        },
    }

    # Check 1: Python version
    py_ver = f"{sys.version_info.major}.{sys.version_info.minor}"
    py_ok = sys.version_info >= (3, 11)
    result["checks"].append({
        "name": "python",
        "ok": py_ok,
        "value": py_ver,
        "required": "3.11+",
    })
    if not py_ok:
        result["status"] = "error"
        result["_internal"]["fix_commands"].append("# Install Python 3.11+ from https://python.org")

    # Check 2: Node.js
    node_bin = shutil.which("node")
    node_ok = False
    node_ver = "not found"
    if node_bin:
        try:
            node_result = subprocess.run([node_bin, "--version"], capture_output=True, text=True, timeout=5)
            node_ver = node_result.stdout.strip().lstrip("v")
            node_major = int(node_ver.split(".")[0])
            node_ok = node_major >= 20
        except Exception:
            pass
    result["checks"].append({
        "name": "nodejs",
        "ok": node_ok,
        "value": node_ver,
        "required": "20+",
    })
    if not node_ok:
        result["status"] = "error"
        result["_internal"]["fix_commands"].append("# Install Node.js 20+ from https://nodejs.org")

    # Check 3: awp-wallet (from unified readiness)
    result["checks"].append({
        "name": "awp-wallet",
        "ok": readiness["wallet_found"],
        "value": format_wallet_bin_display(readiness["wallet_bin"]),
    })
    if not readiness["wallet_found"]:
        result["status"] = "error"
        result["_internal"]["fix_commands"].extend(awp_wallet_install_steps())

    # Check 4: Runtime defaults (from unified readiness)
    signature_config = readiness.get("signature_config", {})
    registration = readiness.get("registration", {})
    result["checks"].append({
        "name": "runtime_defaults",
        "ok": True,
        "PLATFORM_BASE_URL": readiness["platform_base_url"],
        "MINER_ID": readiness["miner_id"],
        "auth_mode": "auto-managed wallet session",
        "wallet_session": readiness["wallet_session"],
        "signature_config_origin": readiness["signature_config_origin"],
        "signature_config_status": signature_config.get("status"),
        "signature_domain_name": signature_config.get("domain_name"),
        "signature_chain_id": signature_config.get("chain_id"),
        "registration_status": registration.get("status"),
        "registration_required": registration.get("registration_required"),
        "wallet_address": registration.get("wallet_address"),
    })

    # Check 5: Wallet session (from unified readiness)
    if not readiness["wallet_session_ready"] and readiness["wallet_found"]:
        result["checks"].append({
            "name": "wallet_session",
            "ok": False,
            "message": "Wallet session unavailable or expired",
        })
        result["_internal"]["fix_commands"].append(_bootstrap_command())
        result["_internal"]["fix_commands"].append("awp-wallet unlock --duration 3600 --scope full")

    # Add session expiry warning if present
    expiry_seconds = readiness.get("session_expiry_seconds")
    if expiry_seconds is not None and expiry_seconds < 300 and expiry_seconds > 0:
        result["checks"].append({
            "name": "wallet_session_expiry",
            "ok": False,
            "message": f"Wallet session expires in {expiry_seconds}s",
        })
        if "awp-wallet unlock --duration 3600 --scope full" not in result["_internal"]["fix_commands"]:
            result["_internal"]["fix_commands"].append("awp-wallet unlock --duration 3600 --scope full")

    # Determine next command based on unified readiness
    if readiness["can_start"]:
        result["status"] = "ok"
        result["_internal"]["next_command"] = "python scripts/run_tool.py agent-start"
    elif result["_internal"]["fix_commands"]:
        result["_internal"]["next_command"] = result["_internal"]["fix_commands"][0]

    return json.dumps(result, ensure_ascii=False, indent=2)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mine")
    parser.add_argument("--session", "-s", default="", help="Optional session suffix for state separation")
    parser.add_argument(
        "command",
        choices=(
            # Core commands (user / agent)
            "init",
            "doctor",
            "agent-status",
            "agent-start",
            "agent-control",
            "list-datasets",
            # Validator
            "validator-start",
            "validator-control",
            "validator-doctor",
            # Browser auth
            "browser-session",
            "browser-session-status",
            "browser-session-wait",
            # Internal (worker processes; not for end users)
            "run-worker",
            "run-validator-worker",
            "run-once",
            "run-loop",
            "agent-run",
            "process-task-file",
            "export-core-submissions",
            # Legacy aliases
            "setup",
            "setup-status",
            "setup-fix",
            "first-load",
            "check-again",
            "start-working",
            "check-status",
            "status-json",
            "pause",
            "resume",
            "stop",
            "heartbeat",
            "route-intent",
            "classify-intent",
            "intent-help",
            "diagnose",
            "check-env",
            "validator-status",
        ),
    )
    parser.add_argument("args", nargs="*")
    return parser


def render_agent_status() -> str:
    """Ultra-concise status for AI agents. Single JSON with state + next action.

    Uses the unified readiness contract from common.resolve_runtime_readiness().

    Output structure:
    - user_message: Show this to user (natural language, no commands)
    - user_actions: Action options to show user (natural language)
    - _internal: Commands for host agent execution (NEVER show to user)
    """
    from common import resolve_runtime_readiness

    readiness = resolve_runtime_readiness()
    background = _background_session_snapshot()

    if not readiness["can_diagnose"]:
        return json.dumps({
            "ready": False,
            "state": readiness["state"],
            "user_message": "Mining environment not initialized. Setup required.",
            "user_actions": ["Initialize environment"],
            "_internal": {
                "next_command": _bootstrap_command(),
                "action_map": {"Initialize environment": _bootstrap_command()},
            },
        }, ensure_ascii=False, indent=2)

    if not readiness["can_start"]:
        return json.dumps({
            "ready": False,
            "state": readiness["state"],
            "user_message": "Wallet session expired or unavailable. Re-initialization needed.",
            "user_actions": ["Re-initialize", "Run diagnostics"],
            "_internal": {
                "next_command": _bootstrap_command(),
                "action_map": {
                    "Re-initialize": _bootstrap_command(),
                    "Run diagnostics": "python scripts/run_tool.py doctor",
                },
            },
        }, ensure_ascii=False, indent=2)

    if background.get("running"):
        session_id = background.get("session_id", "")
        return json.dumps({
            "ready": True,
            "state": "running",
            "user_message": f"Mining is running in the background (session: {session_id}).",
            "user_actions": ["Check status", "Pause mining", "Stop mining"],
            "_internal": {
                "next_command": "python scripts/run_tool.py agent-control status",
                "action_map": {
                    "Check status": "python scripts/run_tool.py agent-control status",
                    "Pause mining": "python scripts/run_tool.py agent-control pause",
                    "Stop mining": "python scripts/run_tool.py agent-control stop",
                },
                "session": background,
            },
        }, ensure_ascii=False, indent=2)

    # Check LLM availability (non-blocking warning)
    warnings: list[str] = []
    try:
        from mine_gateway import resolve_mine_gateway_model_config
        gateway_cfg = resolve_mine_gateway_model_config()
        if not gateway_cfg:
            warnings.append("No LLM API key configured. Structured data enrichment will be limited. Set ANTHROPIC_API_KEY or OPENAI_API_KEY for better quality.")
    except Exception:
        pass

    if readiness["can_mine"]:
        user_msg = "Mining environment is ready. You can start mining now."
    else:
        user_msg = "Mining environment is ready. Registration will complete automatically on start."
    if warnings:
        user_msg += " Warning: " + " ".join(warnings)
    return json.dumps({
        "ready": readiness["can_start"],
        "state": readiness["state"],
        "user_message": user_msg,
        "user_actions": ["Start mining", "Check status"],
        "_internal": {
            "next_command": "python scripts/run_tool.py agent-start",
            "action_map": {
                "Start mining": "python scripts/run_tool.py agent-start",
                "Check status": "python scripts/run_tool.py agent-control status",
            },
        },
    }, ensure_ascii=False, indent=2)


def run_agent_start(dataset_arg: str = "") -> str:
    from agent_runtime import build_worker_from_env
    from background_worker import start_background_worker
    from worker_state import WorkerStateStore

    readiness = json.loads(render_agent_status())
    if not readiness.get("ready"):
        return json.dumps(readiness, ensure_ascii=False, indent=2)

    store = WorkerStateStore(_default_state_root())
    existing = _background_session_snapshot()
    if existing.get("running"):
        session_id = existing.get("session_id", "")
        return json.dumps({
            "state": "running",
            "user_message": f"Mining is already running in the background (session: {session_id}).",
            "user_actions": ["Check status", "Pause mining", "Stop mining"],
            "_internal": {
                "next_command": "python scripts/run_tool.py agent-control status",
                "action_map": {
                    "Check status": "python scripts/run_tool.py agent-control status",
                    "Pause mining": "python scripts/run_tool.py agent-control pause",
                    "Stop mining": "python scripts/run_tool.py agent-control stop",
                },
                "session": existing,
            },
        }, ensure_ascii=False, indent=2)
    # if existing:
    #     store.clear_background_session()

    try:
        worker = build_worker_from_env(auto_register_awp=True)
    except RuntimeError as exc:
        return json.dumps({
            "state": "error",
            "user_message": "Wallet not registered on-chain. Please install and use the AWP Skill to complete registration, then retry.",
            "user_actions": ["Install AWP Skill", "Retry"],
            "_internal": {
                "error": "registration_required",
                "detail": str(exc),
                "action_map": {
                    "Install AWP Skill": "Install the AWP Skill from the skill marketplace to complete on-chain registration",
                    "Retry": "python scripts/run_tool.py agent-start",
                },
            },
        }, ensure_ascii=False, indent=2)
    selected_dataset_ids = [item.strip() for item in dataset_arg.split(",") if item.strip()] if dataset_arg else None

    import httpx

    try:
        payload = worker.start_working(selected_dataset_ids=selected_dataset_ids)
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        if status == 401:
            return json.dumps({
                "state": "error",
                "user_message": "Authentication failed. Wallet session may have expired. Please re-initialize.",
                "user_actions": ["Re-initialize", "Run diagnostics"],
                "_internal": {
                    "error": "unauthorized",
                    "http_status": 401,
                    "action_map": {
                        "Re-initialize": _bootstrap_command(),
                        "Run diagnostics": "python scripts/run_tool.py doctor",
                    },
                },
            }, ensure_ascii=False, indent=2)
        return json.dumps({
            "state": "error",
            "user_message": f"Platform returned HTTP {status}. The service may be temporarily unavailable.",
            "user_actions": ["Retry", "Run diagnostics"],
            "_internal": {
                "error": "http_error",
                "http_status": status,
                "detail": str(exc)[:200],
                "action_map": {
                    "Retry": "python scripts/run_tool.py agent-start",
                    "Diagnose": "python scripts/run_tool.py doctor",
                },
            },
        }, ensure_ascii=False, indent=2)
    except (httpx.ConnectError, httpx.TimeoutException, OSError) as exc:
        return json.dumps({
            "state": "error",
            "user_message": "Cannot reach the platform. Please check your network connection and try again.",
            "user_actions": ["Retry", "Run diagnostics"],
            "_internal": {
                "error": "network_error",
                "detail": str(exc)[:200],
                "action_map": {
                    "Retry": "python scripts/run_tool.py agent-start",
                    "Diagnose": "python scripts/run_tool.py doctor",
                },
            },
        }, ensure_ascii=False, indent=2)

    if payload.get("selection_required"):
        datasets = payload.get("datasets") or []
        dataset_names = [str(item.get("name") or item.get("dataset_id") or item.get("id") or "").strip() for item in datasets[:5]]
        dataset_ids = [str(item.get("dataset_id") or item.get("id") or "").strip() for item in datasets[:5]]
        action_map = {name: f"python scripts/run_tool.py agent-start {did}" for name, did in zip(dataset_names, dataset_ids)}
        return json.dumps({
            "state": "selection_required",
            "user_message": f"Please select a dataset to start mining. Available: {', '.join(dataset_names)}",
            "user_actions": dataset_names[:3],
            "_internal": {
                "datasets": datasets,
                "action_map": action_map,
            },
        }, ensure_ascii=False, indent=2)

    background = start_background_worker(
        project_root=_project_root(),
        script_path=SCRIPT_DIR / "run_tool.py",
        interval=60,
    )
    store.save_background_session({
        **background,
        "selected_dataset_ids": payload.get("selected_dataset_ids") or [],
    })
    session_id = background["session_id"]
    return json.dumps({
        "state": "running",
        "user_message": f"Mining started successfully. Background worker launched (session: {session_id}).",
        "user_actions": ["Check status", "Pause mining", "Stop mining"],
        "_internal": {
            "next_command": "python scripts/run_tool.py agent-control status",
            "action_map": {
                "Check status": "python scripts/run_tool.py agent-control status",
                "Pause mining": "python scripts/run_tool.py agent-control pause",
                "Stop mining": "python scripts/run_tool.py agent-control stop",
            },
            "session": store.load_background_session(),
        },
    }, ensure_ascii=False, indent=2)


def run_agent_control(action: str = "status") -> str:
    from agent_runtime import build_worker_from_env
    from background_worker import terminate_process
    from worker_state import WorkerStateStore

    normalized = (action or "status").strip().lower()
    store = WorkerStateStore(_default_state_root())
    background = _background_session_snapshot()

    if normalized == "status":
        if not background:
            return json.dumps({
                "state": "idle",
                "user_message": "No active mining session. Start a new session to begin earning.",
                "user_actions": ["Start mining"],
                "_internal": {
                    "action_map": {"Start mining": "python scripts/run_tool.py agent-start"},
                },
            }, ensure_ascii=False, indent=2)
        worker = build_worker_from_env()
        status = worker.check_status()
        is_running = background.get("running")
        session_id = background.get("session_id", "")
        log_path = str(background.get("log_path") or "")

        # Extract recent errors from background worker log
        recent_errors: list[str] = []
        if log_path:
            try:
                log_file = Path(log_path)
                if log_file.exists():
                    lines = log_file.read_text(encoding="utf-8", errors="replace").splitlines()
                    for line in lines[-50:]:
                        lowered = line.lower()
                        if any(kw in lowered for kw in ("error", "401", "403", "traceback", "failed", "exception")):
                            recent_errors.append(line.strip())
                    recent_errors = recent_errors[-10:]  # keep last 10
            except Exception:
                pass

        # Build detailed status summary so LLM doesn't need to guess
        epoch_submitted = status.get("epoch_submitted", 0)
        epoch_target = status.get("epoch_target", 80)
        credit_score = status.get("credit_score", "?")
        credit_tier = status.get("credit_tier", "?")
        epoch_avg_score = status.get("epoch_avg_score")
        processed = status.get("session_totals", {}).get("processed_items", 0)
        submitted = status.get("session_totals", {}).get("submitted_items", 0)
        errors_count = status.get("session_totals", {}).get("errors", 0)
        # Historical totals from profile
        miner_summary = status.get("miner_stats") or status.get("profile", {}).get("miner_summary") or {}
        total_rewards = miner_summary.get("total_rewards")
        total_epochs = miner_summary.get("total_epochs")

        if is_running:
            progress = f"Epoch progress: {epoch_submitted}/{epoch_target} submissions."
            if epoch_avg_score is not None:
                progress += f" Avg score: {epoch_avg_score}."
            credit = f"Credit: {credit_score} ({credit_tier})."
            session_stats = f"This session: {processed} processed, {submitted} submitted, {errors_count} errors."
            rewards_info = ""
            if total_rewards is not None:
                rewards_info = f" Total rewards: {total_rewards} aMine ({total_epochs or '?'} epochs)."
            # Explicit reminder so the host LLM cannot invent "waiting for task
            # assignment" — miners are self-driven, they pull Discovery seeds
            # directly from dataset source_domains every iteration.
            role_note = (
                "Miner is self-driven: it pulls random seed URLs from the selected "
                "dataset and crawls them directly — it does NOT wait for the platform "
                "to assign tasks."
            )
            user_msg = f"Mining is running (session: {session_id}). {progress} {credit} {session_stats}{rewards_info} {role_note}"
            # Diagnostic hints for common issues
            if submitted == 0 and processed > 0 and errors_count > 0:
                user_msg += " Note: items processed but none submitted — check errors in log for submission failures."
            elif submitted == 0 and processed == 0 and errors_count == 0:
                user_msg += (
                    " Note: no submissions yet this session — Discovery is still "
                    "generating seed URLs from dataset source_domains. This is "
                    "normal during the first iteration. Do NOT describe this as "
                    "'waiting for the platform to assign tasks' — the miner pulls "
                    "work itself, it is never assigned."
                )
            if recent_errors:
                user_msg += f" Warning: {len(recent_errors)} recent error(s) in log."
            user_acts = ["Pause mining", "Stop mining"]
            action_map = {
                "Pause mining": "python scripts/run_tool.py agent-control pause",
                "Stop mining": "python scripts/run_tool.py agent-control stop",
            }
        else:
            user_msg = f"Session {session_id} has stopped."
            if recent_errors:
                user_msg += f" Last error: {recent_errors[-1][:120]}"
            user_acts = ["Start mining"]
            action_map = {"Start mining": "python scripts/run_tool.py agent-start"}
        return json.dumps({
            "state": "running" if is_running else "stopped",
            "user_message": user_msg,
            "user_actions": user_acts,
            "_internal": {
                "action_map": action_map,
                "session": background,
                "status": status,
                "log_path": log_path,
                "recent_errors": recent_errors,
            },
        }, ensure_ascii=False, indent=2)

    if normalized not in {"pause", "resume", "stop"}:
        return json.dumps({
            "state": "error",
            "user_message": f"unknown action: {normalized}",
            "user_actions": ["Check status"],
            "_internal": {
                "action_map": {"Check status": "python scripts/run_tool.py agent-control status"},
            },
        }, ensure_ascii=False, indent=2)

    if not background:
        return json.dumps({
            "state": "idle",
            "user_message": "No active mining session found. Start a new session to begin.",
            "user_actions": ["Start mining"],
            "_internal": {
                "action_map": {"Start mining": "python scripts/run_tool.py agent-start"},
            },
        }, ensure_ascii=False, indent=2)

    worker = build_worker_from_env()
    if normalized == "pause":
        payload = worker.pause()
        user_msg = "Mining paused. Session state has been saved."
        user_acts = ["Resume mining", "Stop mining"]
        action_map = {
            "Resume mining": "python scripts/run_tool.py agent-control resume",
            "Stop mining": "python scripts/run_tool.py agent-control stop",
        }
    elif normalized == "resume":
        payload = worker.resume()
        user_msg = "Mining resumed. Continuing from saved state."
        user_acts = ["Pause mining", "Stop mining"]
        action_map = {
            "Pause mining": "python scripts/run_tool.py agent-control pause",
            "Stop mining": "python scripts/run_tool.py agent-control stop",
        }
    else:
        payload = worker.stop()
        pid = int(background.get("pid") or 0)
        if pid > 0:
            terminate_process(pid)
            for _ in range(20):
                refreshed = _background_session_snapshot()
                if not refreshed.get("running"):
                    break
                time.sleep(0.1)
        store.save_background_session({"last_stop_requested_at": int(payload.get("last_state_change_at") or 0)})
        user_msg = "Mining stopped. Background worker terminated."
        user_acts = ["Start new session"]
        action_map = {"Start new session": "python scripts/run_tool.py agent-start"}

    refreshed = _background_session_snapshot()
    if normalized == "stop" and refreshed and not refreshed.get("running"):
        store.clear_background_session()
        refreshed = {}

    return json.dumps({
        "state": payload.get("mining_state"),
        "user_message": user_msg,
        "user_actions": user_acts,
        "_internal": {
            "action_map": action_map,
            "session": refreshed,
            "status": payload,
        },
    }, ensure_ascii=False, indent=2)


def run_agent_loop(max_iterations: int = 1) -> str:
    """
    Agent-friendly mining loop with structured progress output.
    Outputs one JSON per significant event for easy parsing.
    """
    import shutil
    import subprocess as sp

    results = []

    # Step 1: Check prerequisites
    platform_url = resolve_platform_base_url()
    miner_id = resolve_miner_id()
    wallet_token = os.environ.get("AWP_WALLET_TOKEN", "").strip()
    wallet_bin = resolve_wallet_bin()

    if not platform_url:
        return json.dumps({
            "success": False,
            "error": "missing_config",
            "message": "PLATFORM_BASE_URL could not be resolved",
            "events": []
        })

    # Step 2: Auto-unlock wallet if needed
    if not wallet_token:
        results.append({"event": "wallet_unlock", "status": "attempting"})
        try:
            env = os.environ.copy()
            if not env.get("HOME") and env.get("USERPROFILE"):
                env["HOME"] = env["USERPROFILE"]
            proc = sp.run([wallet_bin, "unlock", "--duration", str(WALLET_SESSION_DURATION_SECONDS), "--scope", "full"],
                         capture_output=True, text=True, timeout=30, env=env)
            if proc.returncode == 0:
                data = json.loads(proc.stdout)
                wallet_token = data.get("sessionToken", "")
                os.environ["AWP_WALLET_TOKEN"] = wallet_token
                results.append({"event": "wallet_unlock", "status": "success"})
            else:
                return json.dumps({
                    "success": False,
                    "error": "wallet_unlock_failed",
                    "message": proc.stderr.strip() or "Failed to unlock wallet",
                    "events": results
                })
        except Exception as e:
            return json.dumps({
                "success": False,
                "error": "wallet_unlock_error",
                "message": str(e),
                "events": results
            })

    # Step 3: Run worker
    results.append({"event": "mining_start", "iterations": max_iterations})

    try:
        from agent_runtime import build_worker_from_env
        try:
            worker = build_worker_from_env(auto_register_awp=True)
        except RuntimeError as exc:
            return json.dumps({
                "success": False,
                "error": "registration_required",
                "message": str(exc),
                "events": results,
            })

        for i in range(max_iterations):
            results.append({"event": "iteration_start", "iteration": i + 1})

            # Heartbeat
            try:
                worker.client.send_miner_heartbeat(client_name=worker.config.client_name)
                results.append({"event": "heartbeat", "status": "ok"})
            except Exception as e:
                error_msg = str(e)
                if "401" in error_msg:
                    return json.dumps({
                        "success": False,
                        "error": "auth_failed",
                        "message": "401 Unauthorized - check wallet registration",
                        "events": results
                    })
                results.append({"event": "heartbeat", "status": "error", "message": error_msg})

            results.append({"event": "iteration_complete", "iteration": i + 1})

        return json.dumps({
            "success": True,
            "message": f"Completed {max_iterations} iteration(s)",
            "events": results
        })

    except Exception as e:
        return json.dumps({
            "success": False,
            "error": "worker_error",
            "message": str(e),
            "events": results
        })


def _validator_state_root() -> Path:
    from common import resolve_validator_state_root
    return resolve_validator_state_root()


def _validator_background_snapshot() -> dict[str, object]:
    from validator_worker import get_status
    return get_status(state_root=_validator_state_root())


def render_validator_status() -> str:
    """Validator readiness and background status (JSON for agents)."""
    from common import resolve_validator_readiness

    snapshot = _validator_background_snapshot()
    bg_status = str(snapshot.get("status") or "not_running")

    if bg_status == "running":
        session_id = str(snapshot.get("session_id") or "")
        # Read validator runtime status file (written by ValidatorRuntime._write_status)
        detail_parts: list[str] = [f"Validator is running (session: {session_id})."]
        try:
            # The runtime writes to output/validator-runs/validator-{id}-status.json
            from common import resolve_validator_id, resolve_validator_output_root
            vid = resolve_validator_id()
            suffix = f"-{vid}" if vid else ""
            output_root = resolve_validator_output_root()
            status_file = output_root / f"validator{suffix}-status.json"
            vstatus = json.loads(status_file.read_text(encoding="utf-8")) if status_file.exists() else {}
            ws_ok = vstatus.get("ws_connected", False)
            eligible = vstatus.get("eligible", True)
            stats = vstatus.get("stats", {})
            received = stats.get("tasks_received", 0)
            evaluated = stats.get("tasks_evaluated", 0)
            # match/mismatch are the validator's verdicts on miner data.
            # BOTH are reported to the platform as valid evaluations — the
            # wording below is deliberate to stop host LLMs from parroting
            # "accepted/rejected" and hallucinating that "rejected" means the
            # platform refused the submission.
            match_count = stats.get("tasks_match", stats.get("tasks_accepted", 0))
            mismatch_count = stats.get("tasks_mismatch", stats.get("tasks_rejected", 0))
            in_pool = vstatus.get("in_ready_pool", False)
            # Real-time phase so host LLM knows *exactly* what the validator
            # is doing right now — eliminates guessing / hallucination about
            # "waiting" vs "evaluating" vs "cooling down".
            phase = vstatus.get("phase", "")
            phase_detail = vstatus.get("phase_detail", "")
            phase_descriptions = {
                "waiting_for_task": "idle — waiting for the platform to push an evaluation task via WebSocket (this is normal)",
                "evaluating": f"actively evaluating a task ({phase_detail})" if phase_detail else "actively evaluating a task",
                "cooldown": f"in cooldown ({phase_detail}) — will accept the next task after cooldown expires" if phase_detail else "in cooldown between tasks",
                "starting": "starting up",
                "stopped": "stopped",
            }
            phase_text = phase_descriptions.get(phase, f"phase: {phase}")
            detail_parts.append(f"Current activity: {phase_text}.")
            detail_parts.append(f"Ready pool: {'joined' if in_pool else 'not joined (retrying on next heartbeat)'}.")
            detail_parts.append(f"WebSocket: {'connected' if ws_ok else 'reconnecting (normal during idle periods)'}.")
            detail_parts.append(f"Eligible: {'yes' if eligible else 'no (check heartbeat)'}.")
            if evaluated > 0:
                detail_parts.append(
                    f"Tasks reported to platform: {evaluated} (received {received}). "
                    f"Validator verdicts: {match_count} match, {mismatch_count} mismatch. "
                    f"Both match and mismatch verdicts are submitted as valid evaluations — "
                    f"'mismatch' means the validator judged the miner data inconsistent with the re-crawl, "
                    f"NOT that the platform rejected anything."
                )
            else:
                detail_parts.append(
                    "No tasks evaluated yet."
                )
            # Enrich with profile data for historical stats
            try:
                from common import resolve_wallet_config
                from lib.platform_client import PlatformClient
                _, wtoken = resolve_wallet_config()
                if wtoken:
                    from signer import WalletSigner
                    _signer = WalletSigner(session_token=wtoken)
                    _pc = PlatformClient(base_url=resolve_platform_base_url(), token="", signer=_signer)
                    addr = _pc.get_signer_address()
                    if not addr:
                        raise RuntimeError("signer address unavailable")
                    profile = _pc.fetch_profile(addr)
                    v_summary = profile.get("validator_summary") or {}
                    if v_summary.get("total_rewards") is not None:
                        detail_parts.append(f"Total rewards: {v_summary['total_rewards']} aMine ({v_summary.get('total_epochs', '?')} epochs).")
                    v_epoch = (profile.get("current_epoch") or {}).get("validator") or {}
                    if v_epoch.get("accuracy") is not None:
                        detail_parts.append(f"Current epoch accuracy: {v_epoch['accuracy']}%.")
            except Exception:
                pass
        except Exception:
            pass
        return json.dumps({
            "ready": True,
            "state": "running",
            "user_message": " ".join(detail_parts),
            "user_actions": ["Check validator status", "Stop validator"],
            "_internal": {
                "next_command": "python scripts/run_tool.py validator-control status",
                "action_map": {
                    "Check validator status": "python scripts/run_tool.py validator-control status",
                    "Stop validator": "python scripts/run_tool.py validator-control stop",
                },
                "session": snapshot,
            },
        }, ensure_ascii=False, indent=2)

    readiness = resolve_validator_readiness(auto_install_deps=False)

    if not readiness["can_start"]:
        return json.dumps({
            "ready": False,
            "state": readiness["state"],
            "user_message": f"Validator is not ready: {'; '.join(readiness.get('warnings', []))}",
            "user_actions": ["Run diagnostics", "Start validator"],
            "_internal": {
                "next_command": "python scripts/run_tool.py validator-doctor",
                "action_map": {
                    "Run diagnostics": "python scripts/run_tool.py validator-doctor",
                    "Start validator": "python scripts/run_tool.py validator-start",
                },
                "readiness": readiness,
            },
        }, ensure_ascii=False, indent=2)

    warnings = readiness.get("warnings", [])
    msg = "Validator environment is ready."
    if warnings:
        msg += f" Note: {'; '.join(warnings)}"
    return json.dumps({
        "ready": True,
        "state": "idle",
        "user_message": msg,
        "user_actions": ["Start validator"],
        "_internal": {
            "next_command": "python scripts/run_tool.py validator-start",
            "action_map": {
                "Start validator": "python scripts/run_tool.py validator-start",
            },
        },
    }, ensure_ascii=False, indent=2)


def run_validator_start() -> str:
    """Start the validator background worker with full readiness checks."""
    from common import resolve_validator_readiness
    from validator_worker import start_background

    snapshot = _validator_background_snapshot()
    if snapshot.get("status") == "running":
        session_id = str(snapshot.get("session_id") or "")
        return json.dumps({
            "state": "running",
            "user_message": f"Validator is already running (session: {session_id}).",
            "user_actions": ["Check validator status", "Stop validator"],
            "_internal": {
                "next_command": "python scripts/run_tool.py validator-control status",
                "action_map": {
                    "Check validator status": "python scripts/run_tool.py validator-control status",
                    "Stop validator": "python scripts/run_tool.py validator-control stop",
                },
                "session": snapshot,
            },
        }, ensure_ascii=False, indent=2)

    readiness = resolve_validator_readiness(auto_install_deps=True)

    if not readiness["can_start"]:
        fix_commands: list[str] = []
        state = readiness["state"]
        if state == "missing_dependencies":
            missing = readiness.get("checks", {}).get("dependencies", {}).get("missing", [])
            pip_names = [m["pip"] for m in missing]
            fix_commands.append(f'pip install {" ".join(pip_names)}')
        elif state == "signer_unavailable":
            fix_commands.append("# set VALIDATOR_PRIVATE_KEY or ensure awp-wallet is available")
        elif state == "no_llm_backend":
            # Give the agent-runner explicit, actionable fix steps so the
            # wrapper agent can surface them to the user in plain language.
            fix_commands.append(
                "# Option A: install the openclaw CLI so `which openclaw` succeeds"
            )
            fix_commands.append(
                "# Option B: export MINE_GATEWAY_TOKEN (and optionally "
                "MINE_GATEWAY_BASE_URL, MINE_GATEWAY_MODEL) to route evaluation "
                "through the OpenClaw gateway or any OpenAI-compatible API"
            )
        return json.dumps({
            "state": state,
            "user_message": f"Validator is not ready: {'; '.join(readiness['warnings'])}",
            "user_actions": ["Run diagnostics", "Retry"],
            "_internal": {
                "readiness": readiness,
                "fix_commands": fix_commands,
                "action_map": {
                    "Diagnose": "python scripts/run_tool.py validator-doctor",
                    "Retry": "python scripts/run_tool.py validator-start",
                },
            },
        }, ensure_ascii=False, indent=2)

    try:
        result = start_background(state_root=_validator_state_root())
    except Exception as exc:
        return json.dumps({
            "state": "error",
            "user_message": f"Validator failed to start: {exc}",
            "user_actions": ["Retry", "Run diagnostics"],
            "_internal": {
                "error": str(exc),
                "action_map": {
                    "Retry": "python scripts/run_tool.py validator-start",
                    "Diagnose": "python scripts/run_tool.py validator-doctor",
                },
            },
        }, ensure_ascii=False, indent=2)

    session_id = str(result.get("session_id") or "")
    warnings = readiness.get("warnings", [])
    msg = f"Validator started successfully (session: {session_id})."
    if warnings:
        msg += f" Note: {'; '.join(warnings)}"
    return json.dumps({
        "state": result.get("status", "started"),
        "user_message": msg,
        "user_actions": ["Check validator status", "Stop validator"],
        "_internal": {
            "next_command": "python scripts/run_tool.py validator-control status",
            "action_map": {
                "Check status": "python scripts/run_tool.py validator-control status",
                "Stop": "python scripts/run_tool.py validator-control stop",
            },
            "session": result,
            "readiness": readiness,
        },
    }, ensure_ascii=False, indent=2)


def run_validator_control(action: str = "status") -> str:
    """Control the validator background worker: status, stop."""
    from validator_worker import stop_background

    normalized = (action or "status").strip().lower()
    snapshot = _validator_background_snapshot()

    if normalized == "status":
        return render_validator_status()

    if normalized != "stop":
        return json.dumps({
            "state": "error",
            "user_message": f"unknown action: {normalized}",
            "user_actions": ["Check status"],
            "_internal": {
                "action_map": {"Check status": "python scripts/run_tool.py validator-control status"},
            },
        }, ensure_ascii=False, indent=2)

    if snapshot.get("status") != "running":
        return json.dumps({
            "state": "idle",
            "user_message": "Validator is not currently running.",
            "user_actions": ["Start validator"],
            "_internal": {
                "action_map": {"Start validator": "python scripts/run_tool.py validator-start"},
            },
        }, ensure_ascii=False, indent=2)

    result = stop_background(state_root=_validator_state_root())
    return json.dumps({
        "state": result.get("status", "stopped"),
        "user_message": "Validator has been stopped.",
        "user_actions": ["Start validator"],
        "_internal": {
            "action_map": {"Start validator": "python scripts/run_tool.py validator-start"},
            "result": result,
        },
    }, ensure_ascii=False, indent=2)


def run_validator_doctor() -> str:
    """Full validator diagnostics (aligned with miner doctor).

    Checks: Python version, deps, signer, platform, auth heartbeat,
    AWP registration, config, background process.
    """
    from common import (
        resolve_validator_id, resolve_ws_url, resolve_eval_timeout,
        check_validator_dependencies, resolve_validator_readiness,
    )

    snapshot = _validator_background_snapshot()
    state_root = _validator_state_root()
    readiness = resolve_validator_readiness(auto_install_deps=False)

    checks: list[dict[str, object]] = []
    fix_commands: list[str] = []

    # 1. Python version
    py_ver = f"{sys.version_info.major}.{sys.version_info.minor}"
    py_ok = sys.version_info >= (3, 11)
    checks.append({"name": "python", "ok": py_ok, "value": py_ver, "required": "3.11+"})

    # 2. Dependencies
    deps = check_validator_dependencies()
    checks.append({
        "name": "dependencies",
        "ok": deps["ok"],
        "installed": deps["installed"],
        "missing": deps["missing"],
    })
    if not deps["ok"]:
        pip_names = [m["pip"] for m in deps["missing"]]
        fix_commands.append(f'pip install {" ".join(pip_names)}')

    # 3. Signer
    signer_check = readiness.get("checks", {}).get("signer", {})
    checks.append({
        "name": "signer",
        "ok": signer_check.get("ok", False),
        "type": signer_check.get("type", ""),
        "address": signer_check.get("address", ""),
        "error": signer_check.get("error", ""),
    })
    if not signer_check.get("ok"):
        fix_commands.append("# set VALIDATOR_PRIVATE_KEY or run bootstrap to configure awp-wallet")

    # 4. Platform connectivity
    platform_check = readiness.get("checks", {}).get("platform", {})
    checks.append({
        "name": "platform",
        "ok": platform_check.get("ok", False),
        "url": platform_check.get("url", ""),
        "error": platform_check.get("error", ""),
    })

    # 5. Auth (heartbeat)
    auth_check: dict[str, object] = {"name": "auth_heartbeat", "ok": False}
    if signer_check.get("ok") and platform_check.get("ok"):
        try:
            from common import resolve_validator_signer, resolve_platform_base_url
            from lib.platform_client import PlatformClient

            signer, _ = resolve_validator_signer()
            client = PlatformClient(
                base_url=resolve_platform_base_url(),
                token="",
                signer=signer,
            )
            hb = client.send_unified_heartbeat(client_name=f"validator-{resolve_validator_id()}")
            auth_check["ok"] = True
            eligible = hb.get("data", {}).get("eligible") if isinstance(hb.get("data"), dict) else hb.get("eligible")
            auth_check["eligible"] = eligible
        except Exception as exc:
            auth_check["error"] = str(exc)
    checks.append(auth_check)

    # 6. AWP registration
    reg_check = readiness.get("checks", {}).get("registration", {})
    checks.append({
        "name": "awp_registration",
        "ok": reg_check.get("registered", False),
        "status": reg_check.get("status", "unknown"),
        "address": reg_check.get("wallet_address", ""),
        "registration_required": reg_check.get("registration_required", False),
    })
    if reg_check.get("registration_required") and not reg_check.get("registered"):
        fix_commands.append("python scripts/run_tool.py validator-start  # auto-registers on start")

    # 7. LLM backend (openclaw CLI / gateway / API)
    llm_backend = readiness.get("checks", {}).get("llm_backend", {})
    checks.append({
        "name": "llm_backend",
        "ok": llm_backend.get("ok", False),
        "available_methods": llm_backend.get("available_methods", []),
        "model_config_loaded": llm_backend.get("model_config_loaded", False),
        "error": llm_backend.get("error", ""),
    })
    if not llm_backend.get("ok"):
        fix_commands.append(
            "# install openclaw CLI (so `which openclaw` succeeds) OR export "
            "MINE_GATEWAY_TOKEN to use the OpenClaw gateway / API fallback"
        )

    # 8. Config
    validator_id = resolve_validator_id()
    ws_url = resolve_ws_url()
    eval_timeout = resolve_eval_timeout()
    checks.append({
        "name": "config",
        "ok": True,
        "validator_id": validator_id,
        "ws_url": ws_url,
        "eval_timeout": eval_timeout,
        "platform_url": readiness.get("checks", {}).get("platform", {}).get("url", ""),
    })

    # 9. Background process
    running = snapshot.get("status") == "running"
    checks.append({
        "name": "background_process",
        "ok": running,
        "value": "running" if running else "not running",
        "pid": snapshot.get("pid"),
        "session_id": snapshot.get("session_id"),
    })

    all_ok = all(c.get("ok", False) for c in checks)
    next_command = None
    if all_ok and not running:
        next_command = "python scripts/run_tool.py validator-start"
    elif fix_commands:
        next_command = fix_commands[0]

    return json.dumps({
        "status": "ok" if all_ok else "error",
        "can_start": readiness.get("can_start", False),
        "checks": checks,
        "warnings": readiness.get("warnings", []),
        "_internal": {
            "fix_commands": fix_commands,
            "next_command": next_command,
        },
    }, ensure_ascii=False, indent=2)


def main() -> int:
    namespace = build_parser().parse_args()

    if getattr(namespace, "session", ""):
        suffix = f"-{namespace.session}"
        out_root = str(_project_root() / "output" / f"agent-runs{suffix}")
        os.environ["CRAWLER_OUTPUT_ROOT"] = out_root
        os.environ["WORKER_STATE_ROOT"] = str(Path(out_root) / "_worker_state")

    # Handle setup commands first (don't need full imports)
    if namespace.command == "setup":
        import subprocess
        result = subprocess.run(
            [sys.executable, str(Path(__file__).parent / "mine_setup.py")],
            cwd=Path(__file__).parent.parent,
        )
        return result.returncode

    if namespace.command == "setup-status":
        import subprocess
        result = subprocess.run(
            [sys.executable, str(Path(__file__).parent / "mine_setup.py"), "--status"],
            cwd=Path(__file__).parent.parent,
        )
        return result.returncode

    if namespace.command == "setup-fix":
        import subprocess
        result = subprocess.run(
            [sys.executable, str(Path(__file__).parent / "mine_setup.py"), "--fix"],
            cwd=Path(__file__).parent.parent,
        )
        return result.returncode

    if namespace.command == "doctor":
        print(run_doctor())
        return 0

    if namespace.command == "init":
        import subprocess
        init_args = ["--mainnet"] if "--mainnet" in namespace.args else []
        result = subprocess.run(
            [sys.executable, str(Path(__file__).parent / "init_agent.py")] + init_args,
            cwd=Path(__file__).parent.parent,
        )
        return result.returncode

    from skill_runtime import (
        classify_intent,
        render_control_response,
        render_dataset_listing,
        render_first_load_experience,
        render_intent_help,
        render_start_working_response,
        render_status_summary,
        route_and_execute,
    )

    if namespace.command in {"first-load", "check-again"}:
        print(render_first_load_experience())
        return 0

    if namespace.command == "check-env":
        print(render_env_check())
        return 0

    if namespace.command == "agent-status":
        print(render_agent_status())
        return 0

    if namespace.command == "agent-start":
        dataset_arg = namespace.args[0] if namespace.args else ""
        print(run_agent_start(dataset_arg))
        return 0

    if namespace.command == "agent-control":
        action = namespace.args[0] if namespace.args else "status"
        print(run_agent_control(action))
        return 0

    if namespace.command == "agent-run":
        try:
            max_iter = int(namespace.args[0]) if namespace.args else 1
        except ValueError:
            raise SystemExit(f"agent-run: expected integer argument, got {namespace.args[0]!r}")
        print(run_agent_loop(max_iterations=max_iter))
        return 0

    if namespace.command == "browser-session":
        if not namespace.args:
            print("Usage: browser-session <platform> [outputPath]")
            return 1
        platform = namespace.args[0]
        output_path = namespace.args[1] if len(namespace.args) > 1 else ""
        print(run_browser_session(platform, output_path))
        return 0

    if namespace.command == "browser-session-status":
        if not namespace.args:
            print("Usage: browser-session-status <platform>")
            return 1
        print(run_browser_session_status(namespace.args[0]))
        return 0

    if namespace.command == "browser-session-wait":
        if not namespace.args:
            print("Usage: browser-session-wait <platform>")
            return 1
        print(run_browser_session_wait(namespace.args[0]))
        return 0

    if namespace.command == "validator-status":
        print(render_validator_status())
        return 0

    if namespace.command == "validator-start":
        print(run_validator_start())
        return 0

    if namespace.command == "validator-control":
        action = namespace.args[0] if namespace.args else "status"
        print(run_validator_control(action))
        return 0

    if namespace.command == "validator-doctor":
        print(run_validator_doctor())
        return 0

    if namespace.command == "run-validator-worker":
        # Same logging/buffering fix as the miner run-worker path. Without
        # this, validator_runtime's log.info() calls ("WS connected",
        # "evaluation reported to platform", etc.) never reach the log file.
        _configure_background_logging()
        session_id = namespace.args[0] if namespace.args else None
        from common import (
            resolve_validator_state_root,
            resolve_platform_base_url,
            resolve_validator_id,
            resolve_ws_url,
            resolve_eval_timeout,
            resolve_awp_registration,
            check_validator_dependencies,
            install_validator_dependencies,
            resolve_validator_signer,
        )
        from worker_state import ValidatorStateStore

        state_root = resolve_validator_state_root()
        store = ValidatorStateStore(state_root)
        if session_id:
            store.update_session(session_id=session_id, status="starting")

        # Phase 1: dependency check and optional install
        deps = check_validator_dependencies()
        if not deps["ok"]:
            print(json.dumps({"phase": "deps", "status": "installing", "missing": deps["missing"]}, ensure_ascii=False), flush=True)
            install_result = install_validator_dependencies()
            if install_result["ok"]:
                print(json.dumps({"phase": "deps", "status": "installed", "packages": install_result["installed"]}, ensure_ascii=False), flush=True)
            else:
                store.update_session(status="error", error=f"dependency install failed: {install_result['failed']}")
                print(json.dumps({"status": "error", "phase": "deps", "failed": install_result["failed"]}, ensure_ascii=False, indent=2))
                return 1
            deps = check_validator_dependencies()
            if not deps["ok"]:
                store.update_session(status="error", error=f"dependencies still missing: {deps['missing']}")
                print(json.dumps({"status": "error", "phase": "deps", "still_missing": deps["missing"]}, ensure_ascii=False, indent=2))
                return 1

        # Phase 2: signer initialization
        try:
            signer, signer_type = resolve_validator_signer()
            signer_address = signer.get_address() if hasattr(signer, "get_address") else str(getattr(signer, "signer_address", ""))
            print(json.dumps({"phase": "signer", "type": signer_type, "address": signer_address}, ensure_ascii=False), flush=True)
        except Exception as exc:
            store.update_session(status="error", error=str(exc))
            print(json.dumps({"status": "error", "phase": "signer", "error": str(exc)}, ensure_ascii=False, indent=2))
            return 1

        # Phase 3: AWP registration (auto)
        try:
            registration = resolve_awp_registration(auto_register=True, signer=signer)
            reg_status = registration.get("status", "")
            print(json.dumps({"phase": "registration", "status": reg_status, "address": registration.get("wallet_address")}, ensure_ascii=False), flush=True)
            if reg_status == "auto_register_failed":
                print(json.dumps({"warning": "auto_register_failed", "message": registration.get("message")}, ensure_ascii=False), flush=True)
            elif registration.get("registration_required") and not registration.get("registered"):
                print(json.dumps({"warning": "awp_unregistered", "message": registration.get("message")}, ensure_ascii=False), flush=True)
        except Exception as reg_exc:
            print(json.dumps({"warning": "registration_check_failed", "error": str(reg_exc)}, ensure_ascii=False), flush=True)

        if session_id:
            store.update_session(status="running")

        # Phase 4: build runtime and start
        try:
            from validator_runtime import ValidatorRuntime
            from evaluation_engine import EvaluationEngine
            from ws_client import ValidatorWSClient
            from lib.platform_client import PlatformClient
            from common import resolve_validator_model_config

            platform = PlatformClient(
                base_url=resolve_platform_base_url(),
                token="",
                signer=signer,
            )

            ws_url = resolve_ws_url()
            auth_headers = signer.build_auth_headers("GET", ws_url, None)

            def _refresh_ws_auth() -> dict[str, str]:
                return signer.build_auth_headers("GET", ws_url, None)

            ws = ValidatorWSClient(
                ws_url=ws_url,
                auth_headers=auth_headers,
                on_auth_refresh=_refresh_ws_auth,
            )

            # Load LLM backend config once so both the initial engine and any
            # auto-restart rebuild share the same routing configuration.
            validator_model_config = resolve_validator_model_config()
            engine = EvaluationEngine(
                timeout=resolve_eval_timeout(),
                model_config=validator_model_config,
            )

            runtime = ValidatorRuntime(
                platform_client=platform,
                ws_client=ws,
                engine=engine,
                validator_id=resolve_validator_id(),
            )

            print(json.dumps({"status": "worker_started", "session_id": session_id}, ensure_ascii=False, indent=2))

            # Auto-restart loop (#7): restart on crash up to 5 times
            max_restarts = 5
            restart_cooldown = 10
            restarts = 0
            while True:
                try:
                    result = runtime.start()
                    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
                    while runtime._running:
                        time.sleep(1)
                    break  # clean exit
                except KeyboardInterrupt:
                    runtime.stop()
                    break
                except Exception as loop_exc:
                    restarts += 1
                    print(json.dumps({"status": "crash", "restart": restarts, "error": str(loop_exc)}, ensure_ascii=False), flush=True)
                    if restarts > max_restarts:
                        print(json.dumps({"status": "error", "error": f"exceeded {max_restarts} restarts"}, ensure_ascii=False))
                        store.update_session(status="error", error=f"exceeded {max_restarts} restarts: {loop_exc}")
                        return 1
                    time.sleep(restart_cooldown)
                    # Re-create WS client and runtime for restart
                    ws = ValidatorWSClient(
                        ws_url=ws_url,
                        auth_headers=_refresh_ws_auth(),
                        on_auth_refresh=_refresh_ws_auth,
                    )
                    engine = EvaluationEngine(
                        timeout=resolve_eval_timeout(),
                        model_config=validator_model_config,
                    )
                    runtime = ValidatorRuntime(
                        platform_client=platform,
                        ws_client=ws,
                        engine=engine,
                        validator_id=resolve_validator_id(),
                    )

            store.update_session(status="stopped")
            print(json.dumps(runtime.status(), ensure_ascii=False, indent=2))
        except Exception as exc:
            store.update_session(status="error", error=str(exc))
            print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False, indent=2))
            return 1
        return 0

    if namespace.command == "diagnose":
        print(run_diagnosis())
        return 0

    if namespace.command == "intent-help":
        print(render_intent_help())
        return 0

    if namespace.command == "classify-intent":
        if not namespace.args:
            print("Usage: classify-intent <user_input>")
            return 1
        user_input = " ".join(namespace.args)
        result = classify_intent(user_input)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    if namespace.command == "route-intent":
        if not namespace.args:
            print("Usage: route-intent <user_input>")
            return 1
        user_input = " ".join(namespace.args)
        from agent_runtime import build_worker_from_env
        try:
            worker = build_worker_from_env(auto_register_awp=True)
        except RuntimeError as exc:
            print(str(exc))
            return 1
        result = route_and_execute(user_input, worker)
        if result.get("executed"):
            print(result.get("output", ""))
        else:
            print(result.get("output", ""))
            if result.get("needs_confirmation"):
                print("\n(Awaiting confirmation)")
        return 0

    from agent_runtime import build_worker_from_env, export_core_submissions
    runtime_registration_commands = {
        "start-working",
        "list-datasets",
        "heartbeat",
        "run-once",
        "run-loop",
        "run-worker",
    }
    try:
        worker = build_worker_from_env(auto_register_awp=namespace.command in runtime_registration_commands)
    except RuntimeError as exc:
        print(str(exc))
        return 1

    if namespace.command == "start-working":
        selected_dataset_ids = []
        if namespace.args:
            selected_dataset_ids = [dataset_id.strip() for dataset_id in namespace.args[0].split(",") if dataset_id.strip()]
        print(render_start_working_response(worker, selected_dataset_ids=selected_dataset_ids or None))
        return 0

    if namespace.command == "check-status":
        print(render_status_summary(worker))
        return 0

    if namespace.command == "status-json":
        print(json.dumps(worker.check_status(), ensure_ascii=False, indent=2))
        return 0

    if namespace.command == "list-datasets":
        try:
            datasets = worker.list_datasets()["datasets"] if hasattr(worker, "list_datasets") else worker.client.list_datasets()
            print(render_dataset_listing(datasets))
        except Exception as exc:
            error_msg = str(exc)
            print(f"✗ Failed to list datasets: {error_msg}")
            print("")
            if "401" in error_msg or "Unauthorized" in error_msg:
                print("This appears to be an authentication issue.")
                print("Running diagnostics to identify the cause.")
            else:
                print("Check your network connection and platform URL.")
            return 1
        return 0

    if namespace.command == "pause":
        print(render_control_response(worker.pause()))
        return 0

    if namespace.command == "resume":
        print(render_control_response(worker.resume()))
        return 0

    if namespace.command == "stop":
        print(render_control_response(worker.stop()))
        return 0

    if namespace.command == "heartbeat":
        try:
            worker.client.send_miner_heartbeat(client_name=worker.config.client_name)
            print("✓ Heartbeat sent successfully")
        except Exception as exc:
            error_msg = str(exc)
            print(f"✗ Heartbeat failed: {error_msg}")
            print("")
            if "401" in error_msg or "Unauthorized" in error_msg:
                print("This appears to be an authentication issue.")
                print("Running diagnostics to identify the cause.")
            return 1
        return 0

    if namespace.command == "run-once":
        print(worker.run_once())
        return 0

    if namespace.command == "run-loop":
        try:
            interval = int(namespace.args[0]) if namespace.args else 60
            max_iter = int(namespace.args[1]) if len(namespace.args) > 1 else 0
        except ValueError:
            raise SystemExit("run-loop: expected integer arguments for interval and max_iterations")
        print(worker.run_loop(interval=interval, max_iterations=max_iter))
        return 0

    if namespace.command == "run-worker":
        try:
            interval = int(namespace.args[0]) if namespace.args else 60
            max_iter = int(namespace.args[1]) if len(namespace.args) > 1 else 1
        except ValueError:
            raise SystemExit("run-worker: expected integer arguments for interval and max_iterations")
        # Configure logging so background-worker INFO-level messages actually
        # reach the log file. Without this the default Python logger only
        # emits WARNING+ to stderr, and every log.info("heartbeat ok") /
        # log.info("discovery iteration") call is silently dropped —
        # producing a 0-byte log file that looks like a hung worker.
        _configure_background_logging()
        print(json.dumps(worker.run_worker(interval=interval, max_iterations=max_iter), ensure_ascii=False, indent=2))
        return 0

    if namespace.command == "process-task-file":
        if len(namespace.args) != 2:
            raise SystemExit("process-task-file requires: <taskType> <taskJsonPath>")
        task_type, task_json_path = namespace.args
        payload = json.loads(Path(task_json_path).read_text(encoding="utf-8-sig"))
        if not isinstance(payload, dict):
            raise SystemExit("task payload file must contain a JSON object")
        print(worker.process_task_payload(task_type, payload))
        return 0

    if namespace.command == "export-core-submissions":
        if len(namespace.args) != 3:
            raise SystemExit("export-core-submissions requires: <inputPath> <outputPath> <datasetId>")
        output = export_core_submissions(
            namespace.args[0],
            namespace.args[1],
            namespace.args[2],
            client=worker.client,
        )
        print(f"exported core submissions to {output}")
        return 0

    raise SystemExit(f"unknown command: {namespace.command!r}")


if __name__ == "__main__":
    raise SystemExit(main())
