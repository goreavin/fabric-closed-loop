from __future__ import annotations

import sys
import time
from typing import Iterable

from nbmon.fabric_api import FabricApiError

TERMINAL_STATES = {"Succeeded", "Failed", "Error", "Cancelled", "Dead", "Killed"}
TRANSIENT_STATUS_CODES = {404, 408, 429, 500, 502, 503, 504}

# Network-level failures surfaced by fabric_api._get as FabricApiError with
# status_code=None after exhausting its own retry budget. These are treated
# as transient at the streamer level so a short-lived Fabric/monitoring
# outage doesn't kill a long tail (see bug
# `bugs/2026-04-11-submit-double-run-on-poll-timeout.md`).
DEFAULT_POLL_BUDGET_SECONDS = 300.0
DEFAULT_WARN_INTERVAL_SECONDS = 30.0


class LivyPollingLost(FabricApiError):
    """Raised when `run_until_terminal` has been unable to make a single
    successful API call for longer than the polling budget.

    Distinguished from regular FabricApiError so the CLI can exit with a
    dedicated code (EXIT_POLLING_LOST = 6) instead of a generic transport
    error — the remote notebook may still be running, and the user should
    reconcile via `nbmon status` / `nbmon list` rather than assume failure.
    """


def _is_transient_fabric_error(err: FabricApiError) -> bool:
    """Treat HTTP 404/408/429/5xx *and* network-level (status_code=None)
    failures as transient. Anything else (400/401/403/409/…) propagates.
    """
    status = getattr(err, "status_code", None)
    if status is None:
        return True  # network-level after fabric_api retry budget
    return status in TRANSIENT_STATUS_CODES


class StreamState:
    def __init__(self, name: str, prefix: str, offset: int = 0):
        self.name = name
        self.prefix = prefix.encode("utf-8") if isinstance(prefix, str) else prefix
        self.offset = offset
        self.carry = b""

    def flush(self, out) -> None:
        if self.carry:
            out.write(self.prefix + self.carry + b"\n")
            self.carry = b""


def split_with_prefix(state: StreamState, chunk: bytes, out) -> None:
    if not chunk:
        return
    buf = state.carry + chunk
    lines = buf.split(b"\n")
    state.carry = lines.pop()  # last element is partial (or empty if buf ended with \n)
    if lines:
        out.write(b"".join(state.prefix + line + b"\n" for line in lines))


class LogStreamer:
    def __init__(self, api, ws_id: str, item_id: str, livy_id: str, app_id: str):
        self.api = api
        self.ws = ws_id
        self.item = item_id
        self.livy = livy_id
        self.app = app_id

    def tick(self, state: StreamState, out) -> int:
        meta = self.api.driver_log_meta(self.ws, self.item, self.livy, self.app, state.name)
        length = int(meta.get("containerLogMeta", {}).get("length", 0))
        if length <= state.offset:
            return 0
        size = length - state.offset
        chunk = self.api.driver_log_range(
            self.ws, self.item, self.livy, self.app, state.name,
            offset=state.offset, size=size,
        )
        split_with_prefix(state, chunk, out)
        state.offset = length
        return size

    def run_until_terminal(
        self,
        streams: Iterable[tuple[str, str]],
        out,
        *,
        poll: float = 5.0,
        since: int = 0,
        poll_budget_seconds: float = DEFAULT_POLL_BUDGET_SECONDS,
        warn_interval_seconds: float = DEFAULT_WARN_INTERVAL_SECONDS,
        stderr=None,
        clock=time.monotonic,
    ) -> str:
        states = [StreamState(name, prefix, offset=since) for name, prefix in streams]
        final_state = "Unknown"
        last_success_ts = clock()
        last_warn_ts = 0.0
        stderr = stderr if stderr is not None else sys.stderr

        def _note_success() -> None:
            nonlocal last_success_ts
            last_success_ts = clock()

        def _note_failure(err: FabricApiError, where: str) -> None:
            """Track stale time and emit rate-limited stderr notices.
            Raises LivyPollingLost if the budget is exhausted.
            """
            nonlocal last_warn_ts
            stale = clock() - last_success_ts
            if stale > poll_budget_seconds:
                raise LivyPollingLost(
                    f"nbmon poll budget ({poll_budget_seconds:.0f}s) exhausted "
                    f"on {where}: {err} — last successful API call "
                    f"{stale:.0f}s ago. Run `nbmon status` to reconcile; the "
                    "notebook may still be executing server-side.",
                )
            now = clock()
            if now - last_warn_ts >= warn_interval_seconds:
                last_warn_ts = now
                print(
                    f"[nbmon] transient API error on {where}: {err} "
                    f"(stale {stale:.0f}s, retrying)",
                    file=stderr,
                )

        while True:
            for s in states:
                try:
                    self.tick(s, out)
                except FabricApiError as e:
                    if _is_transient_fabric_error(e):
                        _note_failure(e, f"driver_log[{s.name}]")
                    else:
                        out.write(f"[nbmon] tick error on {s.name}: {e}\n".encode())
                else:
                    _note_success()
            try:
                session = self.api.get_livy_session(self.ws, self.item, self.livy)
            except FabricApiError as e:
                if _is_transient_fabric_error(e):
                    _note_failure(e, "get_livy_session")
                    if poll > 0:
                        time.sleep(poll)
                    continue
                raise
            else:
                _note_success()
            final_state = session.get("state", "Unknown")
            if final_state in TERMINAL_STATES:
                for s in states:
                    try:
                        self.tick(s, out)
                    except FabricApiError:
                        pass
                    s.flush(out)
                return final_state
            if poll > 0:
                time.sleep(poll)
