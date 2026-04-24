from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from secret_refs import read_mine_config, resolve_secret_ref

DEFAULT_PLATFORM_BASE_URL = "https://api.minework.net"
DEFAULT_MINER_ID = "mine-agent"
DEFAULT_EIP712_DOMAIN_NAME = "aDATA"
DEFAULT_EIP712_CHAIN_ID = 8453
DEFAULT_EIP712_VERIFYING_CONTRACT = "0xAB41eE5C44D4568aD802D104A6dAB1Fe09C590D1"
DEFAULT_SIGNATURE_SCHEME = "eip712-http-request"
DEFAULT_SIGNATURE_CONFIG_PATH = "/api/public/v1/signature-config"
DEFAULT_AWP_API_BASE_URL = "https://api.awp.sh/v2"
DEFAULT_SIGNATURE_REQUIRED_HEADERS = [
    "X-Signer",
    "X-Signature",
    "X-Nonce",
    "X-Issued-At",
    "X-Expires-At",
]
DEFAULT_SIGNATURE_OPTIONAL_HEADERS = [
    "X-Chain-Id",
    "X-Signed-Headers",
    "Content-Type",
]
SIGNATURE_CONFIG_CACHE_TTL_SECONDS = 24 * 60 * 60
AWP_REGISTRATION_POLL_ATTEMPTS = 5
AWP_REGISTRATION_POLL_INTERVAL_SECONDS = 2

# Wallet session lifetime and renewal policy
WALLET_SESSION_DURATION_SECONDS = 86400          # 24 hours — 1 hour was too short,
                                                  # background workers (600s crawl timeout)
                                                  # could miss the 5-min renewal window
WALLET_SESSION_RENEW_THRESHOLD_SECONDS = 3600    # renew when <= 1 hour left


def resolve_crawler_root() -> Path:
    import os

    root = os.environ.get("SOCIAL_CRAWLER_ROOT", "").strip()
    candidates: list[Path] = []
    if root:
        candidates.append(Path(root).resolve())
    candidates.append(Path(__file__).resolve().parents[1])
    for path in candidates:
        if path.exists():
            return path
    if root:
        raise RuntimeError(f"SOCIAL_CRAWLER_ROOT does not exist: {Path(root).resolve()}")
    raise RuntimeError("SOCIAL_CRAWLER_ROOT does not exist and the local Mine runtime root could not be resolved")


def inject_crawler_root() -> Path:
    root = resolve_crawler_root()
    root_str = str(root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)
    return root


def resolve_local_venv_python(root: Path | None = None) -> Path | None:
    base = (root or Path(__file__).resolve().parents[1]).resolve()
    candidates = [
        base / ".venv" / "Scripts" / "python.exe",
        base / ".venv" / "bin" / "python",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def resolve_worker_python(project_root: Path) -> str:
    """Pick the best Python for background workers.

    Prefers the project .venv so workers always run inside the venv even
    when the parent process is system Python.
    """
    venv_python = resolve_local_venv_python(project_root)
    return str(venv_python) if venv_python is not None else sys.executable


def worker_subprocess_env() -> dict[str, str]:
    """Build the environment dict for background worker subprocesses.

    Forces unbuffered output and removes the venv re-exec skip flag so
    the child can self-correct.
    """
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env.pop("MINE_SKIP_VENV_REEXEC", None)
    return env


def resolve_output_root() -> Path:
    return Path(os.environ.get("CRAWLER_OUTPUT_ROOT", str(resolve_crawler_root() / "output" / "agent-runs"))).resolve()


def resolve_worker_state_root() -> Path:
    return Path(os.environ.get("WORKER_STATE_ROOT", str(resolve_output_root() / "_worker_state"))).resolve()


def resolve_platform_base_url() -> str:
    return os.environ.get("PLATFORM_BASE_URL", "").strip() or DEFAULT_PLATFORM_BASE_URL


def _signature_config_cache_path() -> Path:
    return resolve_worker_state_root() / "signature_config.json"


def _signature_config_path() -> str:
    return os.environ.get("SIGNATURE_CONFIG_PATH", "").strip() or DEFAULT_SIGNATURE_CONFIG_PATH


def _normalize_signature_config(payload: dict[str, Any], *, fetched_at: int | None = None) -> dict[str, Any]:
    body = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    domain = body.get("domain") if isinstance(body.get("domain"), dict) else {}
    chain_id_raw = body.get("chain_id", domain.get("chain_id", DEFAULT_EIP712_CHAIN_ID))
    try:
        chain_id = int(chain_id_raw)
    except (TypeError, ValueError):
        chain_id = DEFAULT_EIP712_CHAIN_ID
    return {
        "scheme": str(body.get("scheme") or DEFAULT_SIGNATURE_SCHEME),
        "domain_name": str(body.get("domain_name") or domain.get("name") or DEFAULT_EIP712_DOMAIN_NAME),
        "domain_version": str(body.get("domain_version") or domain.get("version") or "1"),
        "chain_id": chain_id,
        "verifying_contract": str(
            body.get("verifying_contract") or domain.get("verifying_contract") or DEFAULT_EIP712_VERIFYING_CONTRACT
        ),
        "required_headers": [
            str(item)
            for item in (body.get("required_headers") or DEFAULT_SIGNATURE_REQUIRED_HEADERS)
            if str(item).strip()
        ],
        "optional_headers": [
            str(item)
            for item in (body.get("optional_headers") or DEFAULT_SIGNATURE_OPTIONAL_HEADERS)
            if str(item).strip()
        ],
        "fetched_at": int(fetched_at if fetched_at is not None else time.time()),
        "source_url": str(
            body.get("source_url")
            or payload.get("source_url")
            or urljoin(resolve_platform_base_url().rstrip("/") + "/", _signature_config_path().lstrip("/"))
        ),
    }


def _default_signature_config() -> dict[str, Any]:
    return {
        "scheme": DEFAULT_SIGNATURE_SCHEME,
        "domain_name": DEFAULT_EIP712_DOMAIN_NAME,
        "domain_version": "1",
        "chain_id": DEFAULT_EIP712_CHAIN_ID,
        "verifying_contract": DEFAULT_EIP712_VERIFYING_CONTRACT,
        "required_headers": list(DEFAULT_SIGNATURE_REQUIRED_HEADERS),
        "optional_headers": list(DEFAULT_SIGNATURE_OPTIONAL_HEADERS),
        "fetched_at": 0,
        "source_url": urljoin(resolve_platform_base_url().rstrip("/") + "/", _signature_config_path().lstrip("/")),
    }


def _load_cached_signature_config() -> dict[str, Any] | None:
    cache_path = _signature_config_cache_path()
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    try:
        return _normalize_signature_config(payload, fetched_at=int(payload.get("fetched_at") or 0))
    except (TypeError, ValueError):
        return None


def _persist_signature_config(config: dict[str, Any]) -> None:
    cache_path = _signature_config_cache_path()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _fetch_signature_config_from_platform(base_url: str) -> dict[str, Any]:
    request_url = urljoin(base_url.rstrip("/") + "/", _signature_config_path().lstrip("/"))
    try:
        with urlopen(request_url, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"signature config fetch failed: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("signature config fetch failed: unexpected payload")
    return _normalize_signature_config(payload)


def _signature_status(*, source: str, stale: bool) -> str:
    if source == "fallback":
        return "fallback"
    if stale:
        return "stale"
    return "fresh"


def _signature_origin(*, has_platform_config: bool) -> str:
    return "platform" if has_platform_config else "fallback"


def resolve_signature_config(*, force_refresh: bool = False) -> dict[str, Any]:
    base_url = resolve_platform_base_url()
    request_url = urljoin(base_url.rstrip("/") + "/", _signature_config_path().lstrip("/"))
    cached = _load_cached_signature_config()
    cache_matches_target = bool(cached and str(cached.get("source_url") or "") == request_url)
    now = int(time.time())
    cache_is_fresh = bool(
        cached
        and cache_matches_target
        and int(cached.get("fetched_at") or 0) >= now - SIGNATURE_CONFIG_CACHE_TTL_SECONDS
    )

    resolved = cached if (cached and cache_matches_target) else _default_signature_config()
    source = "cache" if (cached and cache_matches_target) else "fallback"
    has_platform_config = bool(cached and cache_matches_target)
    stale = bool(cached and cache_matches_target) and not cache_is_fresh

    if force_refresh or not cached or not cache_matches_target or not cache_is_fresh:
        try:
            fetched = _fetch_signature_config_from_platform(base_url)
        except RuntimeError:
            fetched = None
        if fetched is not None:
            _persist_signature_config(fetched)
            resolved = fetched
            source = "platform"
            has_platform_config = True
            stale = False

    overrides = {
        "domain_name": os.environ.get("EIP712_DOMAIN_NAME", "").strip(),
        "chain_id": os.environ.get("EIP712_CHAIN_ID", "").strip(),
        "verifying_contract": os.environ.get("EIP712_VERIFYING_CONTRACT", "").strip(),
    }
    if overrides["domain_name"] or overrides["chain_id"] or overrides["verifying_contract"]:
        resolved = dict(resolved)
        if overrides["domain_name"]:
            resolved["domain_name"] = overrides["domain_name"]
        if overrides["chain_id"]:
            try:
                resolved["chain_id"] = int(overrides["chain_id"])
            except ValueError:
                resolved["chain_id"] = DEFAULT_EIP712_CHAIN_ID
        if overrides["verifying_contract"]:
            resolved["verifying_contract"] = overrides["verifying_contract"]
        source = "env"
        stale = False

    resolved["source"] = source
    resolved["origin"] = _signature_origin(has_platform_config=has_platform_config)
    resolved["status"] = _signature_status(source=source, stale=stale)
    return resolved


def resolve_awp_api_base_url() -> str:
    return os.environ.get("AWP_API_URL", "").strip() or DEFAULT_AWP_API_BASE_URL


def _extract_wallet_address(payload: dict[str, Any]) -> str:
    address = str(payload.get("address") or payload.get("eoaAddress") or "").strip()
    if not address:
        addresses = payload.get("addresses")
        if isinstance(addresses, list) and addresses:
            first = addresses[0]
            if isinstance(first, dict):
                address = str(first.get("address") or first.get("eoaAddress") or "").strip()
    if not address:
        raise RuntimeError("wallet address missing from awp-wallet receive")
    return address


def _awp_request_json(method: str, base_url: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    request_url = urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = Request(
        request_url,
        data=data,
        headers=headers,
        method=method.upper(),
    )
    try:
        with urlopen(request, timeout=20) as response:
            body = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"AWP API request failed: {request_url} — {exc}") from exc
    if not isinstance(body, dict):
        raise RuntimeError(f"AWP API request failed: unexpected payload from {request_url}")
    return body


def _awp_jsonrpc(base_url: str, method: str, params: dict[str, Any] | None = None) -> dict[str, Any] | list[Any]:
    """Call AWP API v2 using JSON-RPC format."""
    request_url = base_url.rstrip("/")
    payload = {
        "jsonrpc": "2.0",
        "method": method,
        "params": params or {},
        "id": 1,
    }
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    request = Request(request_url, data=data, headers=headers, method="POST")
    try:
        with urlopen(request, timeout=15) as response:
            body = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"AWP JSON-RPC failed: {method} — {exc}") from exc
    if not isinstance(body, dict):
        raise RuntimeError(f"AWP JSON-RPC failed: unexpected payload from {method}")
    if "error" in body:
        err = body["error"]
        raise RuntimeError(f"AWP JSON-RPC error: {err.get('message', err)}")
    return body.get("result", {})


def _awp_get_json(base_url: str, path: str) -> dict[str, Any]:
    return _awp_request_json("GET", base_url, path)


def _awp_post_json(base_url: str, path: str, payload: dict[str, Any]) -> dict[str, Any]:
    return _awp_request_json("POST", base_url, path, payload)


def _registration_domain_from_registry(registry: dict[str, Any] | list[Any], chain_id: int = DEFAULT_EIP712_CHAIN_ID) -> dict[str, Any]:
    """Extract EIP-712 domain from registry API response.

    The registry.get RPC returns a list (multi-chain) or a single dict.
    Select the matching chain entry, then extract eip712Domain.
    """
    entry: dict[str, Any] = {}
    if isinstance(registry, list):
        for item in registry:
            if isinstance(item, dict) and int(item.get("chainId") or 0) == chain_id:
                entry = item
                break
        if not entry and registry:
            entry = registry[0] if isinstance(registry[0], dict) else {}
    elif isinstance(registry, dict):
        entry = registry

    domain = entry.get("eip712Domain")
    if isinstance(domain, dict):
        return {
            "name": str(domain.get("name") or "AWPRegistry"),
            "version": str(domain.get("version") or "1"),
            "chainId": int(domain.get("chainId") or chain_id),
            "verifyingContract": str(domain.get("verifyingContract") or entry.get("awpRegistry") or ""),
        }
    return {
        "name": "AWPRegistry",
        "version": "1",
        "chainId": chain_id,
        "verifyingContract": str(entry.get("awpRegistry") or ""),
    }


def _build_set_recipient_typed_data(
    *,
    wallet_address: str,
    nonce: int,
    deadline: int,
    domain: dict[str, Any],
) -> dict[str, Any]:
    return {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            "SetRecipient": [
                {"name": "user", "type": "address"},
                {"name": "recipient", "type": "address"},
                {"name": "nonce", "type": "uint256"},
                {"name": "deadline", "type": "uint256"},
            ],
        },
        "primaryType": "SetRecipient",
        "domain": domain,
        "message": {
            "user": wallet_address,
            "recipient": wallet_address,
            "nonce": nonce,
            "deadline": deadline,
        },
    }


def _is_awp_registered(payload: dict[str, Any]) -> bool:
    return bool(payload.get("isRegistered") or payload.get("isRegisteredUser"))


def resolve_awp_registration(*, auto_register: bool = False, signer: Any | None = None) -> dict[str, Any]:
    """Check AWP registration and optionally auto-register.

    When *signer* implements ``get_address()`` and ``sign_typed_data(typed_data)``,
    use it to sign SetRecipient without awp-wallet CLI or session token.
    Otherwise fall back to the awp-wallet CLI flow.
    """
    base_url = resolve_awp_api_base_url()
    result: dict[str, Any] = {
        "api_base_url": base_url,
        "wallet_address": "",
        "registered": False,
        "status": "unavailable",
        "message": "",
        "tx_hash": "",
        "registration_required": False,
    }

    # --- Phase 1: resolve wallet address ---
    wallet_bin = ""
    wallet_token = ""
    if signer is not None:
        try:
            wallet_address = signer.get_address()
        except Exception as exc:
            result["status"] = "signer_error"
            result["message"] = f"signer.get_address() failed: {exc}"
            return result
    else:
        wallet_bin, wallet_token = resolve_wallet_config()
        if not (Path(wallet_bin).exists() or shutil.which(wallet_bin)):
            result["status"] = "wallet_missing"
            result["message"] = "awp-wallet is unavailable, cannot verify AWP registration"
            return result
        try:
            wallet_payload = _run_wallet_json(wallet_bin, "receive")
            wallet_address = _extract_wallet_address(wallet_payload)
        except RuntimeError as exc:
            result["status"] = "wallet_unavailable"
            result["message"] = str(exc)
            return result

    result["wallet_address"] = wallet_address

    # --- Phase 2: on-chain registration status ---
    try:
        check = _awp_jsonrpc(base_url, "address.check", {"address": wallet_address, "chainId": DEFAULT_EIP712_CHAIN_ID})
    except RuntimeError as exc:
        result["status"] = "status_check_failed"
        result["message"] = str(exc)
        return result

    if _is_awp_registered(check):
        result["registered"] = True
        result["status"] = "registered"
        result["bound_to"] = str(check.get("boundTo") or "")
        result["recipient"] = str(check.get("recipient") or "")
        result["message"] = "wallet already registered on AWP"
        return result

    result["status"] = "unregistered"
    result["message"] = "wallet is not registered on AWP"
    result["registration_required"] = True
    if not auto_register:
        return result

    # --- Phase 3: auto-register (sign SetRecipient and relay) ---
    can_sign = signer is not None or bool(wallet_token.strip())
    if not can_sign:
        result["status"] = "wallet_session_unavailable"
        result["message"] = "wallet is not registered and no wallet session is available for auto registration"
        return result

    try:
        registry = _awp_jsonrpc(base_url, "registry.get", {"chainId": DEFAULT_EIP712_CHAIN_ID})
        nonce_payload = _awp_jsonrpc(base_url, "nonce.get", {"address": wallet_address, "chainId": DEFAULT_EIP712_CHAIN_ID})
        nonce = int(nonce_payload.get("nonce") or 0)
        if nonce < 0:
            raise ValueError("invalid nonce")
        deadline = int(time.time()) + 3600
        typed_data = _build_set_recipient_typed_data(
            wallet_address=wallet_address,
            nonce=nonce,
            deadline=deadline,
            domain=_registration_domain_from_registry(registry),
        )

        if signer is not None:
            raw_sig = signer.sign_typed_data(typed_data)
            signature = raw_sig if raw_sig.startswith("0x") else f"0x{raw_sig}"
        else:
            signature_payload = _run_wallet_json(
                wallet_bin,
                "sign-typed-data",
                "--token",
                wallet_token,
                "--data",
                json.dumps(typed_data, ensure_ascii=False, separators=(",", ":")),
            )
            signature = str(signature_payload.get("signature") or "").strip()
            if not signature:
                raise RuntimeError("awp-wallet sign-typed-data returned empty signature")

        relay_base_url = base_url.replace("/v2", "")
        domain = _registration_domain_from_registry(registry, chain_id=DEFAULT_EIP712_CHAIN_ID)
        relay = _awp_post_json(
            relay_base_url,
            "/api/relay/set-recipient",
            {
                "user": wallet_address,
                "recipient": wallet_address,
                "nonce": nonce,
                "deadline": deadline,
                "chainId": domain.get("chainId", DEFAULT_EIP712_CHAIN_ID),
                "signature": signature,
            },
        )
        result["tx_hash"] = str(relay.get("txHash") or relay.get("tx_hash") or "").strip()
    except (RuntimeError, ValueError, AttributeError, TypeError) as exc:
        result["status"] = "auto_register_failed"
        result["message"] = f"auto registration failed: {exc}"
        return result

    for attempt in range(AWP_REGISTRATION_POLL_ATTEMPTS):
        try:
            refreshed = _awp_jsonrpc(base_url, "address.check", {"address": wallet_address, "chainId": DEFAULT_EIP712_CHAIN_ID})
        except RuntimeError:
            break
        if _is_awp_registered(refreshed):
            result["registered"] = True
            result["status"] = "auto_registered"
            result["bound_to"] = str(refreshed.get("boundTo") or "")
            result["recipient"] = str(refreshed.get("recipient") or "")
            result["message"] = "wallet auto-registered on AWP"
            result["registration_required"] = False
            return result
        if attempt < AWP_REGISTRATION_POLL_ATTEMPTS - 1:
            time.sleep(AWP_REGISTRATION_POLL_INTERVAL_SECONDS)

    result["status"] = "registration_pending"
    result["message"] = "gasless self-registration submitted; awaiting AWP confirmation"
    result["registration_required"] = True
    return result


def resolve_runtime_readiness() -> dict[str, Any]:
    """Unified readiness contract for Mine skill.

    Returns a dict with:
    - state: "ready" | "registration_required" | "auth_required" | "degraded" | "agent_not_initialized"
    - can_diagnose: bool - wallet found, can at least run diagnostics
    - can_start: bool - wallet_found AND session_ready AND not expired
    - can_mine: bool - can_start AND registered
    - warnings: list[str] - actionable warnings (expiry, fallback config, etc.)
    """
    wallet_bin = resolve_wallet_bin()
    wallet_found = bool(shutil.which(wallet_bin) or Path(wallet_bin).exists())
    _wallet_bin, wallet_token = resolve_wallet_config()
    signature_config = resolve_signature_config()
    signature_origin = str(signature_config.get("origin") or signature_config.get("source") or "fallback")
    try:
        registration = resolve_awp_registration(auto_register=False)
    except Exception as exc:
        registration = {
            "status": "unknown",
            "registered": False,
            "registration_required": False,
            "wallet_address": "",
            "message": str(exc),
        }

    # Session expiry check — auto-renew if expired or near expiry
    warnings: list[str] = []
    session_expiry_seconds: int | None = None
    expires_at_raw = os.environ.get("AWP_WALLET_TOKEN_EXPIRES_AT", "").strip()
    if expires_at_raw.isdigit():
        session_expiry_seconds = int(expires_at_raw) - int(time.time())

    wallet_session_ready = bool(wallet_token.strip())

    if session_expiry_seconds is not None and session_expiry_seconds <= 0 and wallet_found:
        renewed_token = _try_auto_renew_session(wallet_bin)
        if renewed_token:
            wallet_token = renewed_token
            wallet_session_ready = True
            new_expires_raw = os.environ.get("AWP_WALLET_TOKEN_EXPIRES_AT", "").strip()
            session_expiry_seconds = int(new_expires_raw) - int(time.time()) if new_expires_raw.isdigit() else None
            warnings.append("wallet session auto-renewed on startup")

    if session_expiry_seconds is not None:
        if session_expiry_seconds <= 0:
            warnings.append("wallet session expired")
            wallet_session_ready = False
        elif session_expiry_seconds < WALLET_SESSION_RENEW_THRESHOLD_SECONDS:
            warnings.append(f"wallet session expires in {session_expiry_seconds}s (auto-renew pending)")

    # Signature config warning
    if signature_origin == "fallback":
        warnings.append("using fallback signature config (platform unreachable)")

    registration_required = bool(registration.get("registration_required"))
    registration_registered = bool(registration.get("registered"))

    # Three-tier readiness
    can_diagnose = wallet_found
    can_start = wallet_found and wallet_session_ready
    can_mine = can_start and registration_registered
    auto_registration_possible = can_start and registration_required

    # State determination
    if not wallet_found:
        state = "agent_not_initialized"
    elif not wallet_session_ready:
        state = "auth_required"
    elif registration_registered:
        state = "ready"
    elif registration_required:
        state = "registration_required"
    else:
        state = "degraded"

    return {
        "state": state,
        "can_diagnose": can_diagnose,
        "can_start": can_start,
        "can_mine": can_mine,
        "warnings": warnings,
        "wallet_found": wallet_found,
        "wallet_bin": wallet_bin,
        "wallet_session_ready": wallet_session_ready,
        "wallet_session": (wallet_token[:8] + "...") if wallet_token else "(auto-managed, not currently available)",
        "session_expiry_seconds": session_expiry_seconds,
        "platform_base_url": resolve_platform_base_url(),
        "miner_id": resolve_miner_id(),
        "signature_config": signature_config,
        "signature_config_origin": signature_origin,
        "registration": registration,
        "auto_registration_possible": auto_registration_possible,
    }


def resolve_miner_id() -> str:
    return os.environ.get("MINER_ID", "").strip() or DEFAULT_MINER_ID


def wallet_bin_candidates() -> list[str]:
    configured = os.environ.get("AWP_WALLET_BIN", "").strip()
    candidates: list[str] = []

    def add(candidate: str) -> None:
        value = candidate.strip()
        if value and value not in candidates:
            candidates.append(value)

    if configured:
        add(configured)
        resolved = shutil.which(configured)
        if resolved:
            add(resolved)

    add("awp-wallet")
    if os.name == "nt":
        add("awp-wallet.cmd")
        appdata = os.environ.get("APPDATA", "").strip()
        if appdata:
            add(str(Path(appdata) / "npm" / "awp-wallet.cmd"))
        add(str(Path.home() / "AppData" / "Roaming" / "npm" / "awp-wallet.cmd"))
        npm_prefix = os.environ.get("npm_config_prefix", "").strip() or os.environ.get("NPM_CONFIG_PREFIX", "").strip()
        if npm_prefix:
            add(str(Path(npm_prefix) / "awp-wallet.cmd"))
    else:
        add(str(Path.home() / ".local" / "bin" / "awp-wallet"))
        npm_prefix = os.environ.get("npm_config_prefix", "").strip() or os.environ.get("NPM_CONFIG_PREFIX", "").strip()
        if npm_prefix:
            add(str(Path(npm_prefix) / "bin" / "awp-wallet"))

    return candidates


def format_wallet_bin_display(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return "awp-wallet"
    name = Path(raw).name or raw
    lowered = name.lower()
    if lowered in {"awp-wallet", "awp-wallet.cmd", "awp-wallet.exe"}:
        return "awp-wallet"
    stem = Path(name).stem
    return stem or "awp-wallet"


def resolve_wallet_bin() -> str:
    configured = os.environ.get("AWP_WALLET_BIN", "").strip()
    for candidate in wallet_bin_candidates():
        candidate_path = Path(candidate).expanduser()
        if candidate_path.exists():
            return str(candidate_path)
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
    return configured or "awp-wallet"


def _wallet_command_env() -> dict[str, str]:
    env = os.environ.copy()
    if not env.get("HOME") and env.get("USERPROFILE"):
        env["HOME"] = env["USERPROFILE"]
    return env


def _load_state_session() -> dict[str, Any]:
    session_path = resolve_worker_state_root() / "session.json"
    try:
        return json.loads(session_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def persist_wallet_session(session_token: str, *, expires_at: int | None = None) -> None:
    if not session_token.strip():
        return
    state_root = resolve_worker_state_root()
    state_root.mkdir(parents=True, exist_ok=True)
    session_path = state_root / "session.json"
    payload = _load_state_session()
    payload["wallet_session_token"] = session_token
    if expires_at is not None:
        payload["token_expires_at"] = int(expires_at)
    session_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_persisted_wallet_session() -> tuple[str, int | None]:
    payload = _load_state_session()
    session_token = str(payload.get("wallet_session_token") or "").strip()
    expires_at_raw = payload.get("token_expires_at")
    expires_at = int(expires_at_raw) if str(expires_at_raw or "").isdigit() else None
    if session_token and (expires_at is None or expires_at > int(time.time()) + 30):
        os.environ.setdefault("AWP_WALLET_TOKEN", session_token)
        if expires_at is not None:
            os.environ.setdefault("AWP_WALLET_TOKEN_EXPIRES_AT", str(expires_at))
        return session_token, expires_at
    return "", expires_at


def _run_wallet_json(wallet_bin: str, *args: str) -> dict[str, Any]:
    result = subprocess.run(
        [wallet_bin, *args],
        capture_output=True,
        text=True,
        timeout=30,
        env=_wallet_command_env(),
    )
    if result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(stderr or f"awp-wallet {' '.join(args)} failed")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"awp-wallet returned non-JSON output for {' '.join(args)}") from exc


def _try_auto_renew_session(wallet_bin: str) -> str:
    """Attempt to renew an expired wallet session via awp-wallet unlock.

    Returns the new session token on success, empty string on failure.
    Safe to call even if wallet is locked or unavailable — all errors are swallowed.
    """
    if not (Path(wallet_bin).exists() or shutil.which(wallet_bin)):
        return ""
    try:
        token = _ensure_wallet_session(wallet_bin, duration_seconds=WALLET_SESSION_DURATION_SECONDS)
        if token:
            logging.getLogger("common").info("Auto-renewed expired wallet session on startup")
        return token
    except Exception:
        return ""


def _ensure_wallet_session(wallet_bin: str, *, duration_seconds: int = WALLET_SESSION_DURATION_SECONDS) -> str:
    try:
        _run_wallet_json(wallet_bin, "receive")
    except RuntimeError as exc:
        message = str(exc).lower()
        if "no wallet found" not in message and "init first" not in message:
            return ""
        try:
            _run_wallet_json(wallet_bin, "init")
        except RuntimeError:
            return ""

    issued_at = int(time.time())
    try:
        payload = _run_wallet_json(wallet_bin, "unlock", "--duration", str(max(1, duration_seconds)), "--scope", "full")
    except RuntimeError:
        return ""

    session_token = str(payload.get("sessionToken") or "").strip()
    if not session_token:
        return ""
    expires_at = issued_at + max(1, duration_seconds)
    os.environ["AWP_WALLET_TOKEN"] = session_token
    os.environ["AWP_WALLET_TOKEN_EXPIRES_AT"] = str(expires_at)
    persist_wallet_session(session_token, expires_at=expires_at)
    return session_token


def resolve_wallet_config() -> tuple[str, str]:
    """Return ``(wallet_bin, wallet_token)`` using explicit config, local state, and auto-recovery.

    * ``AWP_WALLET_BIN``   – path to awp-wallet CLI (default ``"awp-wallet"``)
    * ``AWP_WALLET_TOKEN`` – optional explicit wallet session token override
    * ``AWP_WALLET_TOKEN_SECRET_REF`` – JSON SecretRef resolved against Mine config providers
    """
    import os

    wallet_bin = resolve_wallet_bin()
    wallet_token = os.environ.get("AWP_WALLET_TOKEN", "").strip()

    # Discard env token if it's already expired
    if wallet_token:
        exp_raw = os.environ.get("AWP_WALLET_TOKEN_EXPIRES_AT", "").strip()
        if exp_raw.isdigit() and int(exp_raw) <= int(time.time()):
            wallet_token = ""
            os.environ.pop("AWP_WALLET_TOKEN", None)
            os.environ.pop("AWP_WALLET_TOKEN_EXPIRES_AT", None)

    if not wallet_token:
        ref_raw = os.environ.get("AWP_WALLET_TOKEN_SECRET_REF", "").strip()
        if ref_raw:
            try:
                ref = json.loads(ref_raw)
            except json.JSONDecodeError:
                ref = None
            if ref is not None:
                wallet_token = resolve_secret_ref(ref, read_mine_config())
    if not wallet_token:
        wallet_token, _expires_at = _load_persisted_wallet_session()
    if not wallet_token and (Path(wallet_bin).exists() or shutil.which(wallet_bin)):
        wallet_token = _ensure_wallet_session(wallet_bin)

    return (wallet_bin, wallet_token)


# === Validator-specific constants and functions ===

DEFAULT_VALIDATOR_ID = "validator-agent"
DEFAULT_EVAL_TIMEOUT = 120

CREDIT_TIER_INTERVALS = {
    "probation": 600,
    "low": 300,
    "moderate": 120,
    "good": 30,
    "excellent": 10,
}

def resolve_validator_id() -> str:
    return os.environ.get("VALIDATOR_ID", "").strip() or DEFAULT_VALIDATOR_ID

def resolve_validator_output_root() -> Path:
    env_val = os.environ.get("VALIDATOR_OUTPUT_ROOT", "").strip()
    if env_val:
        return Path(env_val).resolve()
    return resolve_crawler_root() / "output" / "validator-runs"

def resolve_validator_state_root() -> Path:
    return resolve_validator_output_root() / "_worker_state"

def resolve_eval_timeout() -> int:
    env_val = os.environ.get("EVAL_TIMEOUT_SECONDS", "").strip()
    if env_val:
        try:
            return int(env_val)
        except ValueError:
            pass
    return DEFAULT_EVAL_TIMEOUT

def resolve_credit_interval(credit_tier: str) -> int:
    return CREDIT_TIER_INTERVALS.get(credit_tier.lower(), CREDIT_TIER_INTERVALS["novice"])

def resolve_ws_url() -> str:
    base = resolve_platform_base_url()
    if base.startswith("wss://") or base.startswith("ws://"):
        ws_base = base
    elif base.startswith("https://"):
        ws_base = "wss://" + base[8:]
    elif base.startswith("http://"):
        ws_base = "ws://" + base[7:]
    else:
        ws_base = "ws://" + base
    return ws_base.rstrip("/") + "/api/mining/v1/ws"


# === Validator dependencies and readiness ===

VALIDATOR_REQUIRED_PACKAGES = {
    "websockets": "websockets>=16.0",
    "eth_account": "eth-account>=0.13.0",
    "Crypto": "pycryptodome>=3.20.0",
}


def _check_python_package(import_name: str) -> bool:
    """Return True if the Python package can be imported."""
    import importlib
    try:
        importlib.import_module(import_name)
        return True
    except ImportError:
        return False


def check_validator_dependencies() -> dict[str, Any]:
    """Check all Python packages required for the validator.

    Returns:
        {"ok": bool, "missing": [...], "installed": [...]}
    """
    missing: list[dict[str, str]] = []
    installed: list[str] = []
    for import_name, pip_name in VALIDATOR_REQUIRED_PACKAGES.items():
        if _check_python_package(import_name):
            installed.append(import_name)
        else:
            missing.append({"import": import_name, "pip": pip_name})
    return {"ok": not missing, "missing": missing, "installed": installed}


def install_validator_dependencies(*, venv_python: str | None = None) -> dict[str, Any]:
    """Install missing validator dependencies via pip.

    Returns:
        {"ok": bool, "installed": [...], "failed": [...]}
    """
    deps = check_validator_dependencies()
    if deps["ok"]:
        return {"ok": True, "installed": [], "failed": []}

    python_bin = venv_python or sys.executable
    installed: list[str] = []
    failed: list[dict[str, str]] = []
    for pkg in deps["missing"]:
        pip_spec = pkg["pip"]
        try:
            subprocess.run(
                [python_bin, "-m", "pip", "install", pip_spec],
                capture_output=True, text=True, timeout=120,
                check=True,
            )
            installed.append(pip_spec)
        except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as exc:
            failed.append({"pip": pip_spec, "error": str(exc)})

    return {"ok": not failed, "installed": installed, "failed": failed}


def resolve_validator_signer() -> tuple[Any, str]:
    """Resolve validator signer: VALIDATOR_PRIVATE_KEY first, else WalletSigner.

    Returns:
        (signer, signer_type) where signer_type is "pk" or "wallet".
    """
    private_key = os.environ.get("VALIDATOR_PRIVATE_KEY", "").strip()
    if private_key:
        from pk_signer import PrivateKeySigner
        return PrivateKeySigner(private_key), "pk"
    from signer import WalletSigner
    wallet_bin, wallet_token = resolve_wallet_config()
    if not wallet_token.strip():
        raise RuntimeError("no signer available: VALIDATOR_PRIVATE_KEY not set and wallet session unavailable")
    return WalletSigner(wallet_bin=wallet_bin, session_token=wallet_token), "wallet"


def resolve_validator_readiness(*, auto_install_deps: bool = False) -> dict[str, Any]:
    """Unified validator readiness (aligned with miner resolve_runtime_readiness).

    Checks: Python deps, signer (pk/wallet), AWP registration, platform connectivity.
    """
    result: dict[str, Any] = {
        "state": "ready",
        "can_start": False,
        "warnings": [],
        "checks": {},
    }

    # 1. Dependencies
    deps = check_validator_dependencies()
    if not deps["ok"] and auto_install_deps:
        install_result = install_validator_dependencies()
        deps = check_validator_dependencies()
        if install_result.get("installed"):
            result["warnings"].append(f"auto-installed dependencies: {', '.join(install_result['installed'])}")
        if install_result.get("failed"):
            for f in install_result["failed"]:
                result["warnings"].append(f"dependency install failed: {f['pip']} — {f['error']}")
    result["checks"]["dependencies"] = deps

    if not deps["ok"]:
        result["state"] = "missing_dependencies"
        missing_names = [m["pip"] for m in deps["missing"]]
        result["warnings"].append(f"missing dependencies: {', '.join(missing_names)}")
        return result

    # 2. Signer
    signer_check: dict[str, Any] = {"ok": False, "type": "", "address": ""}
    try:
        signer, signer_type = resolve_validator_signer()
        signer_check["ok"] = True
        signer_check["type"] = signer_type
        signer_check["address"] = signer.get_address() if hasattr(signer, "get_address") else str(getattr(signer, "signer_address", ""))
    except Exception as exc:
        signer_check["error"] = str(exc)
        result["state"] = "signer_unavailable"
        result["warnings"].append(f"signer unavailable: {exc}")
    result["checks"]["signer"] = signer_check

    if not signer_check["ok"]:
        return result

    # 3. AWP registration
    try:
        registration = resolve_awp_registration(auto_register=False, signer=signer)
        result["checks"]["registration"] = registration
        if registration.get("registration_required") and not registration.get("registered"):
            result["warnings"].append("AWP not registered; will auto-register on start")
    except Exception as exc:
        result["checks"]["registration"] = {"status": "check_failed", "error": str(exc)}
        result["warnings"].append(f"AWP registration check failed: {exc}")

    # 4. Platform connectivity
    platform_url = resolve_platform_base_url()
    platform_check: dict[str, Any] = {"ok": False, "url": platform_url}
    try:
        with urlopen(f"{platform_url}/health", timeout=10) as resp:
            platform_check["ok"] = resp.status == 200
            platform_check["status_code"] = resp.status
    except Exception as exc:
        platform_check["error"] = str(exc)
        result["warnings"].append(f"platform connection failed: {exc}")
    result["checks"]["platform"] = platform_check

    # 5. LLM backend (openclaw CLI / gateway / API). Validator evaluation
    # cannot function without at least one working LLM path, so this is a
    # hard gate on can_start — previously the validator would start and then
    # fail every task with "openclaw not found".
    llm_check = check_validator_llm_backend()
    result["checks"]["llm_backend"] = llm_check
    if not llm_check["ok"]:
        result["state"] = "no_llm_backend"
        result["warnings"].append(llm_check.get("error") or "no LLM backend available")

    # Final decision
    all_ok = deps["ok"] and signer_check["ok"] and llm_check["ok"]
    result["can_start"] = all_ok
    if all_ok:
        result["state"] = "ready"

    return result


def resolve_validator_model_config() -> dict[str, Any]:
    """Load the validator's LLM model config via the shared loader.

    Returns an empty dict when no gateway/API config is set — in that case the
    CLI path must be available for evaluation to work.
    """
    import logging as _logging
    _log = _logging.getLogger(__name__)
    try:
        from crawler.schema_runtime.model_config import load_model_config
    except ImportError as exc:
        _log.warning("model_config import failed: %s", exc)
        return {}
    try:
        return load_model_config(None, use_openclaw=True) or {}
    except Exception as exc:
        _log.warning("model_config load failed: %s", exc)
        return {}


def check_validator_llm_backend() -> dict[str, Any]:
    """Check whether at least one LLM execution path is available.

    The validator evaluation engine routes through ``crawler.enrich.generative.llm_enrich``
    which supports OpenClaw CLI → gateway → API. This helper reports which
    methods are available so validator-doctor and validator-start can give
    a precise error message when everything is missing.
    """
    try:
        from crawler.enrich.generative.llm_enrich import (
            available_methods,
            llm_execution_available,
        )
    except ImportError as exc:
        return {
            "ok": False,
            "available_methods": [],
            "model_config_loaded": False,
            "error": f"llm_enrich import failed: {exc}",
        }

    model_config = resolve_validator_model_config()
    methods = available_methods(model_config)
    ok = llm_execution_available(model_config)
    if ok:
        return {
            "ok": True,
            "available_methods": methods,
            "model_config_loaded": bool(model_config),
        }
    return {
        "ok": False,
        "available_methods": methods,
        "model_config_loaded": bool(model_config),
        "error": (
            "no LLM backend available: the openclaw CLI is not in PATH and no "
            "gateway/API fallback is configured. Either install openclaw (so "
            "`which openclaw` succeeds) or set MINE_GATEWAY_TOKEN (plus "
            "MINE_GATEWAY_BASE_URL / MINE_GATEWAY_MODEL if non-default)."
        ),
    }
