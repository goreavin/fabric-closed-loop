"""
Parse Fabric driver stdout for cell-level execution context:
- which Statement (cell) was last executing
- last few "meaningful" output lines (the cell prints / DataFrame.show output)
- final Python traceback if the run died with an exception

Why driver stdout: the Fabric Spark monitoring portal's "Item snapshots" tab
is served by an internal API the public token cannot reach. But the same
content (cell prints, df.show output, Python tracebacks) is written to the
driver stdout file, which IS exposed by the public Fabric REST API.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

# "Statement0", "Statement0-foo: ...", "Statement0 completed in ..."
STATEMENT_RE = re.compile(rb"^Statement(\d+)(?:[- ]|$)")

# Lines that come from the YARN log wrapper, not from user code.
LOG_WRAPPER_PREFIXES = (
    b"Container:",
    b"LogAggregationType:",
    b"=====",
    b"LogType:",
    b"LogLastModifiedTime:",
    b"LogLength:",
    b"LogContents:",
    b"End of LogType",
    b"*****",
)

# Bootstrap chatter from the Fabric/Spark init + Java shutdown noise that's
# not user-meaningful.
BOOTSTRAP_PATTERNS = (
    re.compile(rb"^InMemoryCacheClient class found"),
    re.compile(rb"^ZookeeperCache class found"),
    re.compile(rb"^Statement\d+"),
    re.compile(rb"^\[Python\] Insert .* to sys\.path"),
    re.compile(rb"^---------+$"),                  # Jupyter rich-tb separator
    re.compile(rb"^\s+at [\w.$]+\("),               # Java stack frame
    re.compile(rb"^\s*\.\.\. \d+ more\s*$"),        # ... NN more
    re.compile(rb"^Caused by: "),
    re.compile(rb"^\s+Suppressed: "),
    re.compile(rb"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d.* (ERROR|WARN) "),  # post-shutdown logs
    re.compile(rb"^Calling LA appender"),                              # post-cell shutdown
)

# Python traceback start markers (covers both rich Jupyter-style and plain).
TRACEBACK_START_RE = re.compile(
    rb"^(\w+(?:Exception|Error))\s+Traceback \(most recent call last\)$"
)
PLAIN_TRACEBACK_RE = re.compile(rb"^Traceback \(most recent call last\):$")

# Final exception line: e.g. "AnalysisException: [PATH_NOT_FOUND] Path does not exist: ..."
EXCEPTION_LINE_RE = re.compile(rb"^(\w+(?:Exception|Error)):\s*(.+)$")

# Jupyter "Cell In[N], line M" — anchors traceback to a specific cell.
CELL_LINE_RE = re.compile(rb"^Cell In\[(\d+)\], line (\d+)\s*$")


@dataclass(frozen=True)
class StatusInfo:
    current_cell: Optional[str] = None
    last_output: tuple = field(default_factory=tuple)
    traceback: Optional[dict] = None


def latest_statement(blob: bytes) -> Optional[str]:
    """Return the highest StatementN marker seen, INCLUDING Statement0 bootstrap.

    Note: `extract_status` further suppresses the result when it's only
    Statement0, since that just means the Fabric session bootstrap (no real
    user cell wrapping). Callers wanting raw markers can use this directly.
    """
    highest = -1
    for line in blob.splitlines():
        m = STATEMENT_RE.match(line)
        if m:
            n = int(m.group(1))
            if n > highest:
                highest = n
    return f"Statement{highest}" if highest >= 0 else None


def _is_meaningful(line: bytes) -> bool:
    stripped = line.lstrip()
    if not stripped.strip():
        return False
    # Java/Spark shutdown noise — match against the dedented form so nested
    # exception indentation (1 tab vs 2 tabs vs spaces) is handled.
    if (
        stripped.startswith(b"at ")
        or stripped.startswith(b"Caused by:")
        or stripped.startswith(b"Suppressed:")
        or stripped.startswith(b"... ")
    ):
        return False
    for prefix in LOG_WRAPPER_PREFIXES:
        if line.startswith(prefix):
            return False
    for pat in BOOTSTRAP_PATTERNS:
        if pat.match(line):
            return False
    return True


def tail_meaningful_lines(
    blob: bytes, n: int = 3, *, max_line_chars: int = 200
) -> list[str]:
    """Last `n` non-noise lines of a stdout blob, with consecutive duplicates
    collapsed and overlong lines truncated to `max_line_chars` (with `…` marker).
    """
    out: list[str] = []
    last: str | None = None
    for raw in blob.splitlines():
        if not _is_meaningful(raw):
            continue
        decoded = raw.decode("utf-8", errors="replace")
        if decoded == last:
            continue  # collapse consecutive duplicates
        last = decoded
        if len(decoded) > max_line_chars:
            decoded = decoded[: max_line_chars - 1] + "…"
        out.append(decoded)
    return out[-n:]


def extract_traceback(blob: bytes) -> Optional[dict]:
    if not blob:
        return None
    lines = blob.splitlines()
    # Walk lines top-to-bottom, find the LAST traceback start, then collect
    # until the final "ExceptionClass: message" line that follows it.
    last_tb_start = -1
    last_exc_class = None
    for idx, line in enumerate(lines):
        m = TRACEBACK_START_RE.match(line)
        if m:
            last_tb_start = idx
            last_exc_class = m.group(1).decode("utf-8")
            continue
        if PLAIN_TRACEBACK_RE.match(line):
            last_tb_start = idx
            last_exc_class = None
            continue

    if last_tb_start < 0:
        return None

    # Walk forward from the traceback start collecting:
    #   - the literal "ExceptionClass: message" line
    #   - the first "Cell In[N], line M" frame (so the banner can show the cell)
    exc_class = last_exc_class
    cell: Optional[str] = None
    line_num: Optional[int] = None
    message_parts: list[bytes] = []
    for line in lines[last_tb_start + 1 :]:
        cell_m = CELL_LINE_RE.match(line)
        if cell_m and cell is None:
            cell = f"Cell In[{cell_m.group(1).decode()}]"
            line_num = int(cell_m.group(2))
            continue
        m = EXCEPTION_LINE_RE.match(line)
        if m:
            exc_class = m.group(1).decode("utf-8")
            message_parts = [m.group(2)]
            continue
        if message_parts and line.startswith(b" "):
            message_parts.append(line.strip())

    if exc_class is None:
        return None
    message = b" ".join(message_parts).decode("utf-8", errors="replace").strip()
    if not message:
        return None
    return {
        "exception_class": exc_class,
        "message": message,
        "cell": cell,
        "line": line_num,
    }


def _split_at_traceback(blob: bytes) -> bytes:
    """Return the prefix of `blob` up to (but not including) the first
    Python traceback header. Used so 'last cell output' shows only what
    the cell printed BEFORE crashing, not the post-crash Java stacktrace.
    """
    lines = blob.splitlines(keepends=True)
    for idx, line in enumerate(lines):
        # strip trailing \n for matching
        stripped = line.rstrip(b"\r\n")
        if TRACEBACK_START_RE.match(stripped) or PLAIN_TRACEBACK_RE.match(stripped):
            return b"".join(lines[:idx])
    return blob


def extract_status(blob: bytes) -> StatusInfo:
    pre_tb = _split_at_traceback(blob)
    raw_stmt = latest_statement(blob)
    # Statement0 is the Fabric bootstrap wrapper, not a real user cell.
    # Suppress it so callers don't display "cell=Statement0" for every run.
    current_cell = raw_stmt if raw_stmt and raw_stmt != "Statement0" else None
    return StatusInfo(
        current_cell=current_cell,
        last_output=tuple(tail_meaningful_lines(pre_tb, n=3)),
        traceback=extract_traceback(blob),
    )
