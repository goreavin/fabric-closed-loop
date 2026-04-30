"""
Render the rich status banner from session JSON + extracted stdout info +
optional Spark Advise dict, themed via nbmon.theme.

The renderer touches escape codes only via theme attributes, so swapping
themes is one-line in theme.py.
"""
from __future__ import annotations

from typing import Optional

from nbmon.banner import compute_duration_seconds
from nbmon.status_extractor import StatusInfo
from nbmon.theme import Theme, role_for_state

MAX_TRACEBACK_MSG_CHARS = 320
TERMINAL_LIKE_STATES = {"Failed", "Error", "Cancelled", "Dead", "Killed"}


def _truncate(s: str, n: int) -> str:
    if len(s) <= n:
        return s
    return s[: n - 1] + "…"


def render_status_banner(
    session: dict,
    status_info: StatusInfo,
    *,
    advise: Optional[dict],
    theme: Theme,
) -> str:
    state = session.get("state", "Unknown")
    livy_id = session.get("livyId", "?")
    app_id = session.get("sparkApplicationId") or "?"
    notebook_name = session.get("itemName") or "?"
    duration = compute_duration_seconds(session)
    duration_str = f"{duration}s" if duration is not None else "?"

    R = theme.reset

    lines: list[str] = []

    # Header line: "── State (Ns) ── notebook name"
    # The state word lives inside the box and inherits the box color — no
    # per-state distinction (the word itself is the signal).
    lines.append(
        f"{theme.box}── {state} ({duration_str}) ──{R} "
        f"{theme.notebook_name}{notebook_name}{R}"
    )

    # Identity line
    cell_str = ""
    if status_info.current_cell:
        cell_str = f"  {theme.label}cell={R}{theme.accent}{status_info.current_cell}{R}"
    lines.append(
        f"{theme.label}livy={R}{livy_id}  {theme.label}app={R}{app_id}{cell_str}"
    )

    # Last output (if any)
    if status_info.last_output:
        lines.append(f"{theme.label}last output:{R}")
        for line in status_info.last_output:
            lines.append(f"  {line}")

    # Python traceback (if extracted)
    tb = status_info.traceback
    if tb is not None:
        cell = tb.get("cell")
        line_num = tb.get("line")
        anchor = ""
        if cell:
            anchor = f" {theme.dim}({cell}"
            if line_num is not None:
                anchor += f", line {line_num}"
            anchor += f"){R}"
        lines.append(f"{theme.error_label}error:{R}{anchor}")
        msg = _truncate(tb["message"], MAX_TRACEBACK_MSG_CHARS)
        lines.append(
            f"  {theme.error_emphasis}{tb['exception_class']}{R}{theme.error}: {msg}{R}"
        )
    elif state in TERMINAL_LIKE_STATES:
        # Failed but no traceback extracted — direct user to the full driver log.
        lines.append(
            f"{theme.dim}(no python traceback in stdout — for full driver log: "
            f"nbmon attach <path> --run livy:{livy_id} --once){R}"
        )

    # Spark Advise (if present, terminal failures only)
    if advise is not None:
        lines.append(f"{theme.label}spark_advise:{R} {theme.advise}{advise['name']}{R}")
        desc = advise.get("description", "").strip()
        if desc:
            desc = _truncate(desc, 240)
            lines.append(f"  {theme.dim}{desc}{R}")

    return "\n".join(lines)
