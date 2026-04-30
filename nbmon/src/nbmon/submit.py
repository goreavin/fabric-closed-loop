from __future__ import annotations

import json
import re
import time
from typing import Callable, Sequence

from nbmon.run_resolver import _default_fab_runner

JOB_INSTANCE_RE = re.compile(r"Job instance '([0-9a-fA-F-]{36})' created")


class SubmitError(RuntimeError):
    pass


def parse_job_instance_id(output: str) -> str:
    # Try strict JSON envelope first.
    try:
        start = output.index("{")
        end = output.rindex("}") + 1
        envelope = json.loads(output[start:end])
        msg = envelope.get("result", {}).get("message", "")
        m = JOB_INSTANCE_RE.search(msg)
        if m:
            return m.group(1)
    except (ValueError, json.JSONDecodeError):
        pass
    # Fallback: regex anywhere in the raw text.
    m = JOB_INSTANCE_RE.search(output)
    if m:
        return m.group(1)
    raise SubmitError(
        f"Could not parse job instance id from `fab job start` output:\n{output}"
    )


def submit_notebook(
    path: str,
    *,
    pool: str = "HighConcurrency",
    fab_runner: Callable[[Sequence[str]], str] | None = None,
) -> str:
    runner = fab_runner or _default_fab_runner
    args = ["job", "start", path, "--output_format", "json"]
    if pool == "HighConcurrency":
        args.extend(["-C", '{"sparkPool":{"type":"HighConcurrency"}}'])
    output = runner(args)
    return parse_job_instance_id(output)


def wait_for_session_with_app(
    api,
    ws_id: str,
    item_id: str,
    job_instance_id: str,
    *,
    poll: float = 2.0,
    timeout: float = 300.0,
) -> dict:
    deadline = time.monotonic() + timeout
    while True:
        sessions = api.list_livy_sessions(ws_id, item_id)
        for s in sessions:
            if s.get("jobInstanceId") != job_instance_id:
                continue
            if s.get("livyId") and s.get("sparkApplicationId"):
                return s
        if time.monotonic() >= deadline:
            raise SubmitError(
                f"Timed out waiting for sparkApplicationId on jobInstanceId {job_instance_id}"
            )
        if poll > 0:
            time.sleep(poll)
        else:
            time.sleep(0)
