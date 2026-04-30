from __future__ import annotations

import re
from datetime import datetime
from typing import Iterable

ADVISE_LINE_RE = re.compile(rb"Sending DriverError to Advise Hub: Map\((.*)\)\s*$")
KV_RE = re.compile(r"_(\w+) -> (.*?)(?=, _\w+ -> |$)")


def extract_advise(log_bytes: bytes) -> dict | None:
    for line in log_bytes.splitlines():
        m = ADVISE_LINE_RE.search(line)
        if not m:
            continue
        body = m.group(1).decode("utf-8", errors="replace")
        kv = {k: v.strip() for k, v in KV_RE.findall(body)}
        if "name" in kv:
            return {
                "name": kv["name"],
                "description": kv.get("description", ""),
                "level": kv.get("level", ""),
            }
    return None


def _parse_iso(s: str) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def compute_duration_seconds(session: dict) -> int | None:
    for key in ("runningDuration", "totalDuration"):
        d = session.get(key)
        if isinstance(d, dict) and d.get("value") is not None:
            return int(d["value"])
    start = _parse_iso(session.get("submittedDateTime", ""))
    end = _parse_iso(session.get("endDateTime") or session.get("endedDateTime") or "")
    if start is None or end is None:
        return None
    return int((end - start).total_seconds())


def format_banner(
    *,
    state: str,
    duration_seconds: int | None,
    livy_id: str,
    app_id: str | None,
    advise: dict | None,
) -> str:
    duration = f"{duration_seconds}s" if duration_seconds is not None else "?"
    head = (
        f"── {state} ({duration}) "
        f"livy={livy_id} app={app_id or '?'} ──"
    )
    if advise is None:
        return head
    desc = advise.get("description", "").strip()
    if len(desc) > 240:
        desc = desc[:237] + "..."
    return f"{head}\nspark_advise: {advise['name']}\n    {desc}"
