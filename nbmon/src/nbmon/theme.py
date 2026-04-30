"""
Color theme palette for nbmon output.

Add a new theme by:
  1. Defining a new `Theme(...)` instance below.
  2. Adding it to the `THEMES` dict.
  3. Pass `--theme NAME` on the command line.

The renderer never references raw escape codes — only Theme attributes
(by semantic role). This means swapping/iterating themes only requires
editing this one file.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, fields
from typing import Optional

ESC = "\x1b"
ORANGE = f"{ESC}[38;2;217;119;87m"   # Claude brand orange #d97757 (truecolor)
RED = f"{ESC}[31m"
GREEN = f"{ESC}[32m"
YELLOW = f"{ESC}[33m"
BLUE = f"{ESC}[34m"
BOLD = f"{ESC}[1m"
DIM = f"{ESC}[2m"
RESET = f"{ESC}[0m"


@dataclass(frozen=True)
class Theme:
    name: str
    # State word colors
    state_succeeded: str
    state_failed: str
    state_running: str
    state_neutral: str  # NotStarted, Submitted, Unknown, etc.
    # Structural elements
    box: str            # the ── … ── frame
    accent: str         # signature color (Claude orange)
    notebook_name: str  # bold/title for notebook itself
    # Labels and values in the body
    label: str          # "livy=", "app=", "last output:", "spark_advise:" — dim grey
    value: str
    value_strong: str
    # Error/diagnostic
    error_label: str    # "error:" specifically — accent color
    error: str          # exception message body
    error_emphasis: str # exception class within the message
    advise: str         # advise category name
    # Resets / generics (kept here so renderer never imports raw escapes)
    reset: str
    bold: str
    dim: str


# Tuple of role-attribute names — used by tests/render code that wants to
# enumerate roles without hardcoding the dataclass fields.
ROLES = tuple(f.name for f in fields(Theme) if f.name != "name")


CLAUDE = Theme(
    name="claude",
    # Box header (── Failed (21s) ──) and the "error:" label are the ONLY
    # accent-colored elements, both in Claude orange. State words inside the
    # box inherit the box color (no per-state distinction — the word itself
    # is the signal).
    box=BOLD + ORANGE,
    error_label=BOLD + ORANGE,
    # State role attributes are empty so the state word inherits the box color
    # rather than reverting to red/green/yellow.
    state_succeeded="",
    state_failed="",
    state_running="",
    state_neutral="",
    # Dim grey for secondary labels (livy=, app=, spark_advise:) and anchors
    # ((Cell In[N], line M)).
    label=DIM,
    dim=DIM,
    # White (default terminal color) for body content
    accent="",
    notebook_name="",
    value="",
    value_strong="",
    error="",
    error_emphasis="",
    advise="",
    bold="",
    reset=RESET,
)


PLAIN = Theme(
    name="plain",
    state_succeeded="",
    state_failed="",
    state_running="",
    state_neutral="",
    box="",
    accent="",
    notebook_name="",
    label="",
    value="",
    value_strong="",
    error_label="",
    error="",
    error_emphasis="",
    advise="",
    reset="",
    bold="",
    dim="",
)


THEMES: dict[str, Theme] = {
    "claude": CLAUDE,
    "plain": PLAIN,
}


def get_theme(name: Optional[str] = "claude", *, is_tty: bool) -> Theme:
    """
    Resolve a theme by name, taking TTY/NO_COLOR into account.

    - If `is_tty` is False → PLAIN (so Claude's Bash tool / pipes get clean text).
    - If `NO_COLOR` env var is set → PLAIN (https://no-color.org/).
    - If `name == "plain"` → PLAIN.
    - Otherwise return THEMES[name], falling back to CLAUDE for unknown names.
    """
    if not is_tty:
        return PLAIN
    if os.environ.get("NO_COLOR"):
        return PLAIN
    if name == "plain":
        return PLAIN
    return THEMES.get(name or "claude", CLAUDE)


_STATE_TO_ROLE = {
    "Succeeded": "state_succeeded",
    "Failed": "state_failed",
    "Error": "state_failed",
    "Cancelled": "state_failed",
    "Dead": "state_failed",
    "Killed": "state_failed",
    "Running": "state_running",
    "InProgress": "state_running",
    "Busy": "state_running",
}


def role_for_state(state: str, theme: Theme) -> str:
    role = _STATE_TO_ROLE.get(state, "state_neutral")
    return getattr(theme, role)
