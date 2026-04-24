from __future__ import annotations

import re
import time
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urlencode, urljoin

import httpx
from common import (
    DEFAULT_EIP712_CHAIN_ID,
    DEFAULT_EIP712_DOMAIN_NAME,
    DEFAULT_EIP712_VERIFYING_CONTRACT,
    WALLET_SESSION_DURATION_SECONDS,
    resolve_signature_config,
)

if TYPE_CHECKING:
    from signer import WalletSigner


class PlatformApiError(Exception):
    """Raised when API returns success:false in the response envelope."""
    def __init__(self, code: str, message: str, category: str, status_code: int, response: Any = None) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.category = category
        self.status_code = status_code
        self.response = response


class PlatformClient:
    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        signer: "WalletSigner | None" = None,
        eip712_chain_id: int | None = None,
        eip712_domain_name: str | None = None,
        eip712_domain_version: str | None = None,
        eip712_verifying_contract: str | None = None,
    ) -> None:
        signature_config = (
            resolve_signature_config()
            if eip712_chain_id is None or eip712_domain_name is None or eip712_verifying_contract is None
            else None
        )
        self._base_url = base_url.rstrip("/")
        self._signer = signer
        self._eip712_chain_id = int(
            eip712_chain_id
            if eip712_chain_id is not None
            else signature_config.get("chain_id", DEFAULT_EIP712_CHAIN_ID)
        )
        self._eip712_domain_name = str(
            eip712_domain_name
            if eip712_domain_name is not None
            else signature_config.get("domain_name", DEFAULT_EIP712_DOMAIN_NAME)
        )
        self._eip712_domain_version = str(
            eip712_domain_version
            if eip712_domain_version is not None
            else (signature_config.get("domain_version") if signature_config else "1") or "1"
        )
        self._eip712_verifying_contract = str(
            eip712_verifying_contract
            if eip712_verifying_contract is not None
            else signature_config.get("verifying_contract", DEFAULT_EIP712_VERIFYING_CONTRACT)
        )
        self._max_retries = 3
        self._last_wallet_refresh: dict[str, Any] | None = None
        headers = {
            "Content-Type": "application/json",
        }
        if token.strip():
            headers["Authorization"] = f"Bearer {token}"
        self._client = httpx.Client(
            base_url=self._base_url,
            timeout=30.0,
            headers=headers,
        )

    def get_signer_address(self) -> str:
        """Return the signer's wallet address, or empty string if unavailable."""
        if self._signer is None:
            return ""
        try:
            return self._signer.get_address()
        except Exception:
            return ""

    def consume_wallet_refresh(self) -> dict[str, Any] | None:
        payload = self._last_wallet_refresh
        self._last_wallet_refresh = None
        return payload

    def send_miner_heartbeat(self, *, client_name: str) -> dict[str, Any]:
        return self.send_unified_heartbeat(client_name=client_name)

    def claim_repeat_crawl_task(self) -> dict[str, Any] | None:
        return self._claim("/api/mining/v1/repeat-crawl-tasks/claim")

    def claim_refresh_task(self) -> dict[str, Any] | None:
        return self._claim("/api/mining/v1/refresh-tasks/claim")

    def report_repeat_crawl_task_result(self, task_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", f"/api/mining/v1/repeat-crawl-tasks/{task_id}/report", payload)

    def reject_repeat_crawl_task(self, task_id: str) -> dict[str, Any]:
        """POST /api/mining/v1/repeat-crawl-tasks/{id}/reject — reject task without penalty"""
        return self._request("POST", f"/api/mining/v1/repeat-crawl-tasks/{task_id}/reject", {})

    def report_refresh_task_result(self, task_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", f"/api/mining/v1/refresh-tasks/{task_id}/report", payload)

    def submit_core_submissions(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._validate_submission_payload(payload)
        return self._request("POST", "/api/mining/v1/submissions", payload)

    def fetch_core_submission(self, submission_id: str) -> dict[str, Any]:
        payload = self._request("GET", f"/api/mining/v1/submissions/{submission_id}", None)
        data = payload.get("data")
        if not isinstance(data, dict):
            raise ValueError(f"unexpected submission payload for {submission_id}")
        return data

    def fetch_dataset(self, dataset_id: str) -> dict[str, Any]:
        payload = self._request("GET", f"/api/core/v1/datasets/{dataset_id}", None)
        data = payload.get("data")
        if not isinstance(data, dict):
            raise ValueError(f"unexpected dataset payload for {dataset_id}")
        return data

    def list_datasets(self) -> list[dict[str, Any]]:
        payload = self._request("GET", "/api/core/v1/datasets", None)
        data = payload.get("data")
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        if isinstance(data, dict):
            items = data.get("items")
            if isinstance(items, list):
                return [item for item in items if isinstance(item, dict)]
        return []

    def send_unified_heartbeat(self, *, client_name: str) -> dict[str, Any]:
        return self._request(
            "POST",
            "/api/mining/v1/heartbeat",
            {
                "client": client_name,
            },
        )

    def answer_pow_challenge(self, challenge_id: str, answer: str) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/mining/v1/pow-challenges/{challenge_id}/answer",
            {
                "answer": answer,
            },
        )

    def check_url_occupancy(
        self,
        dataset_id: str,
        url: str,
        *,
        structured_data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        dedup_payload = {
            "dataset_id": dataset_id,
            "structured_data": self._build_occupancy_structured_data(url, structured_data),
        }
        try:
            resp = self._request("POST", "/api/core/v1/dedup-occupancies/check", dedup_payload)
        except PlatformApiError as api_err:
            if api_err.status_code in (404, 422):
                return {}
            raise
        except httpx.HTTPStatusError as error:
            if error.response.status_code in (404, 422):
                return {}
            raise
        else:
            data = resp.get("data")
            return data if isinstance(data, dict) else {}

    @staticmethod
    def _build_occupancy_structured_data(url: str, structured_data: dict[str, Any] | None) -> dict[str, Any]:
        payload = dict(structured_data or {})
        payload.setdefault("canonical_url", url)
        payload.setdefault("url", url)
        return {
            key: value
            for key, value in payload.items()
            if value not in (None, "", [], {})
        }

    def join_miner_ready_pool(self) -> dict[str, Any]:
        """POST /api/mining/v1/miners/ready — join miner ready pool for repeat crawl tasks"""
        return self._request("POST", "/api/mining/v1/miners/ready", {})

    def leave_miner_ready_pool(self) -> dict[str, Any]:
        """POST /api/mining/v1/miners/unready — leave miner ready pool"""
        return self._request("POST", "/api/mining/v1/miners/unready", {})

    def check_dedup_by_hash(self, dataset_id: str, dedup_hash: str) -> dict[str, Any]:
        """GET /api/core/v1/dedup/check — check dedup by hash"""
        resp = self._request(
            "GET",
            f"/api/core/v1/dedup/check?dataset_id={quote(dataset_id, safe='')}&dedup_hash={quote(dedup_hash, safe='')}",
            None,
        )
        data = resp.get("data")
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _parse_cooldown(error_body: dict[str, Any] | None) -> dict[str, Any] | None:
        """Extract cooldown sentinel from a 409 validator_cooldown response."""
        if not isinstance(error_body, dict):
            return None
        err_obj = error_body.get("error")
        if not isinstance(err_obj, dict) or err_obj.get("code") != "validator_cooldown":
            return None
        retry = err_obj.get("retry_after_seconds")
        if isinstance(retry, (int, float)) and retry > 0:
            return {"_cooldown": True, "retry_after_seconds": int(retry)}
        return None

    def _claim(self, path: str) -> dict[str, Any] | None:
        """POST a claim request. Returns task data, None (no task), or a
        cooldown sentinel ``{"_cooldown": True, "retry_after_seconds": N}``
        when the platform returns 409 ``validator_cooldown``,
        or a PoW sentinel ``{"_pow_required": True, ...}``
        when the platform returns 428 ``pow_required``.
        """
        try:
            payload = self._request("POST", path, {})
        except PlatformApiError as api_err:
            if api_err.status_code == 404:
                return None
            if api_err.status_code == 409:
                body = api_err.response if isinstance(api_err.response, dict) else {}
                return self._parse_cooldown(body)
            # 428 with success:true never reaches PlatformApiError (raise_for_status
            # fires first → httpx.HTTPStatusError). Handled below.
            raise
        except httpx.HTTPStatusError as error:
            if error.response.status_code == 404:
                return None
            if error.response.status_code == 409:
                try:
                    body = error.response.json()
                except ValueError:
                    return None
                return self._parse_cooldown(body)
            if error.response.status_code == 428:
                try:
                    body = error.response.json()
                except ValueError:
                    return None
                data = body.get("data") if isinstance(body, dict) else {}
                if isinstance(data, dict) and data.get("pow_required"):
                    return {"_pow_required": True, **data}
                return None
            raise
        data = payload.get("data")
        if data in (None, {}, []):
            return None
        if not isinstance(data, dict):
            raise ValueError(f"unexpected claim response shape for {path}")
        return data

    def _request_optional_data(self, method: str, path: str) -> dict[str, Any]:
        try:
            payload = self._request(method, path, None)
        except PlatformApiError as api_err:
            if api_err.status_code in (404, 409):
                return {}
            raise
        except httpx.HTTPStatusError as error:
            if error.response.status_code in (404, 409):
                return {}
            raise
        data = payload.get("data")
        return data if isinstance(data, dict) else {}

    def _validate_submission_payload(self, payload: dict[str, Any]) -> None:
        dataset_id = str(payload.get("dataset_id") or "").strip()
        entries = payload.get("entries")
        if not dataset_id or not isinstance(entries, list) or not entries:
            return
        try:
            dataset = self.fetch_dataset(dataset_id)
        except PlatformApiError as api_err:
            if api_err.status_code == 404:
                return  # dataset gone, let server handle it
            raise
        except httpx.HTTPStatusError as http_err:
            if http_err.response.status_code == 404:
                return
            raise
        patterns = self._coerce_url_patterns(dataset)
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            url = str(entry.get("url") or "").strip()
            if not url:
                continue
            if patterns and not any(self._regex_matches(pattern, url) for pattern in patterns):
                raise RuntimeError(
                    f"submission preflight failed: url {url!r} does not match dataset url_patterns for {dataset_id}"
                )

    @staticmethod
    def _coerce_url_patterns(dataset: dict[str, Any]) -> list[str]:
        patterns = dataset.get("url_patterns")
        if not isinstance(patterns, list):
            return []
        return [str(pattern).strip() for pattern in patterns if str(pattern).strip()]

    @staticmethod
    def _regex_matches(pattern: str, value: str) -> bool:
        try:
            return re.fullmatch(pattern, value) is not None
        except re.error:
            return False

    def _request(self, method: str, path: str, payload: dict[str, Any] | None) -> dict[str, Any]:
        last_error: Exception | None = None
        renewed_session = False
        # Extra attempt slot for post-renewal retry
        max_attempts = self._max_retries + 1
        for attempt in range(1, max_attempts + 1):
            kwargs: dict[str, Any] = {}
            if payload is not None:
                kwargs["json"] = payload
            if self._signer is not None:
                request_url = urljoin(
                    self._base_url if self._base_url.endswith("/") else f"{self._base_url}/",
                    path.lstrip("/"),
                )
                kwargs["headers"] = self._signer.build_auth_headers(
                    method,
                    request_url,
                    payload,
                    content_type="application/json",
                    chain_id=self._eip712_chain_id,
                    domain_name=self._eip712_domain_name,
                    domain_version=self._eip712_domain_version,
                    verifying_contract=self._eip712_verifying_contract,
                )
            try:
                response = self._client.request(method, path, **kwargs)
                response.raise_for_status()
                if not response.content:
                    return {}
                body = response.json()
                if not isinstance(body, dict):
                    raise ValueError(f"unexpected response payload for {path}")
                # Check success field in response envelope
                if body.get("success") is False:
                    error_obj = body.get("error", {})
                    code = error_obj.get("code", "unknown") if isinstance(error_obj, dict) else "unknown"
                    msg = error_obj.get("message", "") if isinstance(error_obj, dict) else str(error_obj)
                    category = error_obj.get("category", "") if isinstance(error_obj, dict) else ""
                    status_map = {"not_found": 404, "authentication": 401, "permission": 403, "validation": 422, "state_conflict": 409, "precondition": 428, "rate_limit": 429, "internal": 500, "dependency": 503}
                    # Store both parsed body (for _claim cooldown parsing) and
                    # raw httpx response (for Retry-After header in retry loop).
                    err = PlatformApiError(code, msg, category, status_map.get(category, 500), body)
                    err.http_response = response  # type: ignore[attr-defined]
                    raise err
                return body
            except PlatformApiError as api_err:
                last_error = api_err
                status_code = api_err.status_code
                if (status_code >= 500 or status_code == 429) and attempt < max_attempts:
                    http_resp = getattr(api_err, "http_response", None)
                    if status_code == 429 and http_resp is not None:
                        retry_after = getattr(http_resp, "headers", {}).get("Retry-After")
                        if retry_after and str(retry_after).isdigit():
                            backoff = min(float(retry_after), 60.0)
                        else:
                            backoff = max(2.0, 1.0 * attempt)
                    elif status_code == 429:
                        backoff = max(2.0, 1.0 * attempt)
                    else:
                        backoff = 0.5 * attempt
                    time.sleep(backoff)
                    continue
                raise
            except httpx.HTTPStatusError as error:
                last_error = error
                status_code = error.response.status_code
                # Parse structured error response
                error_code = ""
                error_message = ""
                error_retryable = False
                error_category = ""
                try:
                    error_payload = error.response.json()
                except ValueError:
                    error_payload = {}
                if isinstance(error_payload, dict):
                    # Support two error formats: top-level fields or nested error object
                    error_body = error_payload.get("error")
                    if isinstance(error_body, dict):
                        error_code = str(error_body.get("code") or "")
                        error_message = str(error_body.get("message") or "")
                        error_retryable = bool(error_body.get("retryable", False))
                        error_category = str(error_body.get("category") or "")
                    else:
                        error_code = str(error_payload.get("code") or "")
                        error_message = str(error_payload.get("message") or "")
                        error_retryable = bool(error_payload.get("retryable", False))
                        error_category = str(error_payload.get("category") or "")
                if status_code == 401:
                    if error_code == "MISSING_HEADERS":
                        raise RuntimeError(
                            "Platform API requires Web3 signature headers. "
                            "Let Mine restore the local wallet session automatically or provide equivalent signed requests."
                        ) from error
                    if (
                        self._signer is not None
                        and not renewed_session
                        and (
                            error_code in {"UNAUTHORIZED", "TOKEN_EXPIRED", "SESSION_EXPIRED"}
                            or "expired session token" in error_message.lower()
                        )
                    ):
                        renew_session = getattr(self._signer, "renew_session", None)
                        if callable(renew_session):
                            try:
                                self._last_wallet_refresh = renew_session(duration_seconds=WALLET_SESSION_DURATION_SECONDS)
                            except Exception as renew_exc:
                                raise error from renew_exc
                            renewed_session = True
                            continue
                # Retryable server error or explicitly marked as retryable.
                # 409 Conflict is excluded from auto-retry even when the body
                # says retryable=true — it means "retry after cooldown", not
                # "retry immediately". Callers (e.g. _claim) handle 409 by
                # parsing retry_after_seconds from the response.
                if (status_code >= 500 or status_code == 429 or (error_retryable and status_code not in (409, 428))) and attempt < max_attempts:
                    if status_code == 429:
                        # Respect Retry-After header or use longer backoff for rate limits
                        retry_after = error.response.headers.get("Retry-After")
                        if retry_after and retry_after.isdigit():
                            backoff = min(float(retry_after), 60.0)
                        else:
                            backoff = max(2.0, 1.0 * attempt)
                    else:
                        backoff = 0.5 * attempt
                    time.sleep(backoff)
                    continue
                raise
        if last_error is not None:
            raise last_error from last_error.__cause__
        raise RuntimeError(f"request failed for {method} {path}")

    # === Validator Methods ===

    def get_me(self) -> dict[str, Any]:
        """GET /api/iam/v1/me"""
        resp = self._request("GET", "/api/iam/v1/me", None)
        data = resp.get("data")
        return data if isinstance(data, dict) else {}

    def submit_validator_application(self) -> dict[str, Any]:
        """POST /api/iam/v1/validator-applications"""
        return self._request("POST", "/api/iam/v1/validator-applications", {})

    def get_my_validator_application(self) -> dict[str, Any]:
        """GET /api/iam/v1/validator-applications/me — returns {} if no application exists"""
        return self._request_optional_data("GET", "/api/iam/v1/validator-applications/me")

    def join_ready_pool(self) -> dict[str, Any]:
        """POST /api/mining/v1/validators/ready"""
        return self._request("POST", "/api/mining/v1/validators/ready", {})

    def leave_ready_pool(self) -> dict[str, Any]:
        """POST /api/mining/v1/validators/unready"""
        return self._request("POST", "/api/mining/v1/validators/unready", {})

    def claim_evaluation_task(self) -> dict[str, Any] | None:
        """POST /api/mining/v1/evaluation-tasks/claim"""
        return self._claim("/api/mining/v1/evaluation-tasks/claim")

    def get_evaluation_task(self, task_id: str) -> dict[str, Any]:
        """GET /api/mining/v1/evaluation-tasks/{id}"""
        resp = self._request("GET", f"/api/mining/v1/evaluation-tasks/{task_id}", None)
        data = resp.get("data")
        return data if isinstance(data, dict) else {}

    def report_evaluation(self, task_id: str, score: int, *, assignment_id: str, result: str = "match") -> dict[str, Any]:
        """POST /api/mining/v1/evaluation-tasks/{id}/report"""
        return self._request("POST", f"/api/mining/v1/evaluation-tasks/{task_id}/report", {
            "assignment_id": assignment_id,
            "result": result,
            "score": score,
        })

    def create_validation_result(self, submission_id: str, verdict: str, score: int, comment: str, idempotency_key: str) -> dict[str, Any]:
        """POST /api/mining/v1/validation-results"""
        return self._request("POST", "/api/mining/v1/validation-results", {
            "submission_id": submission_id,
            "verdict": verdict,
            "score": score,
            "comment": comment,
            "idempotency_key": idempotency_key,
        })

    def list_validation_results(self, **params: Any) -> list[dict[str, Any]]:
        """GET /api/mining/v1/validation-results"""
        query = urlencode({k: v for k, v in params.items() if v is not None})
        path = "/api/mining/v1/validation-results"
        if query:
            path = f"{path}?{query}"
        resp = self._request("GET", path, None)
        data = resp.get("data")
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        if isinstance(data, dict):
            items = data.get("items")
            if isinstance(items, list):
                return [item for item in items if isinstance(item, dict)]
        return []

    def get_validation_result(self, result_id: str) -> dict[str, Any]:
        """GET /api/mining/v1/validation-results/{id}"""
        resp = self._request("GET", f"/api/mining/v1/validation-results/{result_id}", None)
        data = resp.get("data")
        return data if isinstance(data, dict) else {}

    # === Self-service endpoints (v2.1) ===

    def fetch_submission_gate(self) -> dict[str, Any]:
        """GET /api/mining/v1/miners/me/submission-gate — check PoW state before submitting."""
        return self._request_optional_data("GET", "/api/mining/v1/miners/me/submission-gate")

    def fetch_my_miner_stats(self) -> dict[str, Any]:
        """GET /api/mining/v1/miners/me/stats"""
        return self._request_optional_data("GET", "/api/mining/v1/miners/me/stats")

    def fetch_my_validator_stats(self) -> dict[str, Any]:
        """GET /api/mining/v1/validators/me/stats"""
        return self._request_optional_data("GET", "/api/mining/v1/validators/me/stats")

    def fetch_my_submissions(self) -> list[dict[str, Any]]:
        """GET /api/mining/v1/miners/me/submissions"""
        try:
            payload = self._request("GET", "/api/mining/v1/miners/me/submissions", None)
        except PlatformApiError as api_err:
            if api_err.status_code in (404, 409):
                return []
            raise
        except httpx.HTTPStatusError as error:
            if error.response.status_code in (404, 409):
                return []
            raise
        data = payload.get("data")
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        if isinstance(data, dict):
            items = data.get("items")
            if isinstance(items, list):
                return [item for item in items if isinstance(item, dict)]
        return []

    def fetch_current_epoch(self) -> dict[str, Any]:
        """GET /api/core/v1/epochs/current"""
        return self._request_optional_data("GET", "/api/core/v1/epochs/current")

    def fetch_dataset_stats(self, dataset_id: str) -> dict[str, Any]:
        """GET /api/core/v1/datasets/:id/stats"""
        return self._request_optional_data("GET", f"/api/core/v1/datasets/{dataset_id}/stats")

    def check_url_occupancy_public(self, dataset_id: str, url: str) -> dict[str, Any]:
        """GET /api/core/v1/url/check — public URL occupancy check"""
        encoded_url = quote(url, safe="")
        return self._request_optional_data(
            "GET", f"/api/core/v1/url/check?dataset_id={quote(dataset_id, safe='')}&url={encoded_url}"
        )

    # === Public info endpoints (v2.1) ===

    def fetch_protocol_info(self) -> dict[str, Any]:
        """GET /api/public/v1/protocol-info"""
        return self._request_optional_data("GET", "/api/public/v1/protocol-info")

    def fetch_network_stats(self) -> dict[str, Any]:
        """GET /api/public/v1/stats"""
        return self._request_optional_data("GET", "/api/public/v1/stats")

    def list_epochs(self) -> list[dict[str, Any]]:
        """GET /api/core/v1/epochs"""
        payload = self._request("GET", "/api/core/v1/epochs", None)
        data = payload.get("data")
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        return []

    def fetch_epoch(self, epoch_id: str) -> dict[str, Any]:
        """GET /api/core/v1/epochs/:epochID"""
        return self._request_optional_data("GET", f"/api/core/v1/epochs/{epoch_id}")

    def list_online_miners(self) -> list[dict[str, Any]]:
        """GET /api/mining/v1/miners/online"""
        payload = self._request("GET", "/api/mining/v1/miners/online", None)
        data = payload.get("data")
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        return []

    def list_online_validators(self) -> list[dict[str, Any]]:
        """GET /api/mining/v1/validators/online"""
        payload = self._request("GET", "/api/mining/v1/validators/online", None)
        data = payload.get("data")
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        return []

    def fetch_epoch_snapshot(self, epoch_id: str) -> dict[str, Any]:
        """GET /api/mining/v1/epochs/:id/snapshot"""
        return self._request_optional_data("GET", f"/api/mining/v1/epochs/{epoch_id}/snapshot")

    def fetch_epoch_settlement_results(self, epoch_id: str) -> dict[str, Any]:
        """GET /api/mining/v1/epochs/:id/settlement-results"""
        return self._request_optional_data("GET", f"/api/mining/v1/epochs/{epoch_id}/settlement-results")

    def fetch_profile(self, address: str) -> dict[str, Any]:
        """GET /api/mining/v1/profiles/:address — unified profile with miner+validator stats"""
        return self._request_optional_data("GET", f"/api/mining/v1/profiles/{quote(address, safe='')}")

    def fetch_miner_profile(self, address: str) -> dict[str, Any]:
        """GET /api/mining/v1/profiles/miners/:address — public miner profile"""
        return self._request_optional_data("GET", f"/api/mining/v1/profiles/miners/{quote(address, safe='')}")

    def fetch_miner_epoch_history(self, address: str) -> list[dict[str, Any]]:
        """GET /api/mining/v1/profiles/miners/:address/epochs — miner epoch history"""
        try:
            payload = self._request("GET", f"/api/mining/v1/profiles/miners/{quote(address, safe='')}/epochs", None)
        except (PlatformApiError, httpx.HTTPStatusError):
            return []
        data = payload.get("data")
        return [item for item in data if isinstance(item, dict)] if isinstance(data, list) else []

    def fetch_validator_epoch_history(self, address: str) -> list[dict[str, Any]]:
        """GET /api/mining/v1/profiles/validators/:address/epochs — validator epoch history"""
        try:
            payload = self._request("GET", f"/api/mining/v1/profiles/validators/{quote(address, safe='')}/epochs", None)
        except (PlatformApiError, httpx.HTTPStatusError):
            return []
        data = payload.get("data")
        return [item for item in data if isinstance(item, dict)] if isinstance(data, list) else []

    def fetch_signature_config(self) -> dict[str, Any]:
        """GET /api/public/v1/signature-config — EIP-712 signing parameters"""
        return self._request_optional_data("GET", "/api/public/v1/signature-config")
