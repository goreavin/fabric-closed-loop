from __future__ import annotations

import time
from typing import Union

import requests

BASE_URL = "https://api.fabric.microsoft.com/v1"

# Network-level exceptions that indicate a transient transport hiccup, not an
# application-level failure. These must be retried with backoff instead of
# propagating and killing the streamer (see bug
# `bugs/2026-04-11-submit-double-run-on-poll-timeout.md`).
_TRANSIENT_NETWORK_EXCEPTIONS: tuple[type[BaseException], ...] = (
    requests.Timeout,  # covers ReadTimeout + ConnectTimeout
    requests.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
)

# Split connect/read timeout. Connect should fail fast (bad routing, DNS,
# auth rewrites) but read is allowed up to 60s because Fabric's monitoring
# API occasionally stalls under load.
DEFAULT_TIMEOUT: tuple[float, float] = (10.0, 60.0)

TimeoutLike = Union[float, tuple[float, float]]


class FabricApiError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class FabricAuthExpired(FabricApiError):
    pass


class FabricApi:
    def __init__(
        self,
        token: str,
        *,
        max_retries: int = 4,
        retry_base_delay: float = 1.0,
        timeout: TimeoutLike = DEFAULT_TIMEOUT,
    ):
        self._token = token
        self._max_retries = max_retries
        self._retry_base = retry_base_delay
        self._timeout = timeout

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self._token}"}

    def _get(self, path: str, *, params: dict | None = None, raw: bool = False):
        url = f"{BASE_URL}/{path.lstrip('/')}"
        attempt = 0
        while True:
            try:
                resp = requests.get(
                    url,
                    params=params,
                    headers=self._headers(),
                    timeout=self._timeout,
                    stream=raw,
                )
            except _TRANSIENT_NETWORK_EXCEPTIONS as e:
                # Network-level hiccup (read/connect timeout, dropped TCP,
                # chunked transfer abort). Retry with the same backoff
                # schedule we use for HTTP 429/5xx; on final give-up raise
                # FabricApiError(status_code=None) so upstream code can
                # handle it uniformly with HTTP errors.
                if attempt >= self._max_retries:
                    raise FabricApiError(
                        f"GET {path} failed after {attempt + 1} attempts "
                        f"(network): {type(e).__name__}: {e}",
                        status_code=None,
                    ) from e
                delay = self._retry_base * (2 ** attempt)
                time.sleep(delay)
                attempt += 1
                continue
            if resp.status_code == 401 or resp.status_code == 403:
                raise FabricAuthExpired(
                    f"Fabric API rejected the bearer token (HTTP {resp.status_code}). "
                    "Run `fab auth login`.",
                    status_code=resp.status_code,
                )
            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt >= self._max_retries:
                    raise FabricApiError(
                        f"GET {path} failed after {attempt + 1} attempts: HTTP {resp.status_code}",
                        status_code=resp.status_code,
                    )
                retry_after = resp.headers.get("Retry-After")
                delay = (
                    float(retry_after)
                    if retry_after is not None
                    else self._retry_base * (2 ** attempt)
                )
                time.sleep(delay)
                attempt += 1
                continue
            if resp.status_code >= 400:
                raise FabricApiError(
                    f"GET {path} failed: HTTP {resp.status_code}",
                    status_code=resp.status_code,
                )
            return resp

    def list_livy_sessions(self, ws_id: str, item_id: str) -> list[dict]:
        resp = self._get(f"workspaces/{ws_id}/notebooks/{item_id}/livySessions")
        return resp.json().get("value", [])

    def get_livy_session(self, ws_id: str, item_id: str, livy_id: str) -> dict:
        resp = self._get(
            f"workspaces/{ws_id}/notebooks/{item_id}/livySessions/{livy_id}"
        )
        return resp.json()

    def driver_log_meta(
        self, ws_id: str, item_id: str, livy_id: str, app_id: str, file_name: str
    ) -> dict:
        resp = self._get(
            f"workspaces/{ws_id}/notebooks/{item_id}/livySessions/{livy_id}"
            f"/applications/{app_id}/logs",
            params={
                "type": "driver",
                "meta": "true",
                "fileName": file_name,
            },
        )
        return resp.json()

    def driver_log_range(
        self,
        ws_id: str,
        item_id: str,
        livy_id: str,
        app_id: str,
        file_name: str,
        *,
        offset: int,
        size: int,
    ) -> bytes:
        resp = self._get(
            f"workspaces/{ws_id}/notebooks/{item_id}/livySessions/{livy_id}"
            f"/applications/{app_id}/logs",
            params={
                "type": "driver",
                "fileName": file_name,
                "isDownload": "true",
                "isPartial": "true",
                "offset": offset,
                "size": size,
            },
            raw=True,
        )
        return b"".join(resp.iter_content(chunk_size=8192))

    def driver_log_full(
        self, ws_id: str, item_id: str, livy_id: str, app_id: str, file_name: str
    ) -> bytes:
        resp = self._get(
            f"workspaces/{ws_id}/notebooks/{item_id}/livySessions/{livy_id}"
            f"/applications/{app_id}/logs",
            params={
                "type": "driver",
                "fileName": file_name,
                "isDownload": "true",
            },
            raw=True,
        )
        return b"".join(resp.iter_content(chunk_size=8192))

    # --- Spark open-source Monitoring REST API (profiling data) ---
    # https://learn.microsoft.com/fabric/data-engineering/open-source-apis
    # https://spark.apache.org/docs/latest/monitoring.html#rest-api
    #
    # Server-side Spark History Server responses. No extra Entra scopes needed
    # beyond the bearer we already use for driver logs.

    def list_stages(
        self, ws_id: str, item_id: str, livy_id: str, app_id: str
    ) -> list[dict]:
        resp = self._get(
            f"workspaces/{ws_id}/notebooks/{item_id}/livySessions/{livy_id}"
            f"/applications/{app_id}/stages"
        )
        data = resp.json()
        return data if isinstance(data, list) else []

    def stage_task_summary(
        self,
        ws_id: str,
        item_id: str,
        livy_id: str,
        app_id: str,
        stage_id: int,
        stage_attempt_id: int = 0,
        *,
        quantiles: str = "0.05,0.25,0.5,0.75,0.95",
    ) -> dict:
        resp = self._get(
            f"workspaces/{ws_id}/notebooks/{item_id}/livySessions/{livy_id}"
            f"/applications/{app_id}/stages/{stage_id}/{stage_attempt_id}/taskSummary",
            params={"quantiles": quantiles},
        )
        return resp.json()

    def list_executors(
        self, ws_id: str, item_id: str, livy_id: str, app_id: str
    ) -> list[dict]:
        resp = self._get(
            f"workspaces/{ws_id}/notebooks/{item_id}/livySessions/{livy_id}"
            f"/applications/{app_id}/executors"
        )
        data = resp.json()
        return data if isinstance(data, list) else []
