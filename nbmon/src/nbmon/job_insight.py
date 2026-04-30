"""
Fabric Job Insight Library client — runs forensics on a completed Spark job
via a persistent Livy Scala session, without creating a notebook artifact.

The Job Insight Library (JIL) is Scala-only and ships with the Fabric Spark
runtime. Docs: https://learn.microsoft.com/en-us/fabric/data-engineering/job-insight-library

This module submits a single multi-section Scala statement to a warm Livy
session, parses the `show(truncate=false)` output tables with section markers,
and returns a structured result ready to be rendered alongside the `/nbmon`
rich banner.

Session reuse: we cache the Livy session id in a dedicated file so we don't
clash with the PySpark session used by the `fabric-cli-livy-session` skill.
Idle timeout on Fabric is 20 minutes — we re-create on dead/error.

Known risks (flagged here so the caller can degrade gracefully):
  * JIL capacity constraint — the Livy session *must* run in the same Fabric
    capacity as the target job. We use the target's workspace by default,
    which satisfies this in practice.
  * Storage scope — JIL reads the target's event log via Fabric backend
    services, not the user's delegated storage scope, so the "unable to fetch
    mwc token" blocker that hits Hive table reads shouldn't apply. If it
    does, the Livy error surfaces as LivyStatementError and the caller can
    fall back to the REST monitoring API.
"""
from __future__ import annotations

import json
import os
import pathlib
import re
import time
from dataclasses import dataclass, field
from typing import Optional

import requests

from nbmon.fabric_api import BASE_URL, FabricApiError, FabricAuthExpired

DEFAULT_SESSION_CACHE = pathlib.Path(
    "~/.config/nbmon/livy-jobinsight-session-id"
)

# HC Livy cache is a separate file with a colon-delimited triple
# (hc_id:sessionId:replId) so we don't collide with the legacy single-id cache.
DEFAULT_HC_SESSION_CACHE = pathlib.Path(
    "~/.config/nbmon/livy-hc-session-id"
)
DEFAULT_HC_SESSION_TAG = "nbmon-jil"

# your default workspace + MY_LAKEHOUSE lakehouse — matches the defaults in
# the fabric-cli-livy-session skill. Override via CLI args when targeting a
# different capacity.
DEFAULT_LIVY_WS = "<your-workspace-guid>"
DEFAULT_LIVY_LH = "<your-lakehouse-guid>"

LIVY_API_VERSION = "2023-12-01"

SESSION_IDLE_STATES = {"idle", "busy"}
# Apache Livy session lifecycle states that are transient on the way to idle.
# "not_started" / "starting" / "recovering" all appear during session boot and
# must be waited through, not treated as failure.
SESSION_TRANSIENT_STATES = {"not_started", "starting", "recovering"}
SESSION_DEAD_STATES = {"dead", "error", "killed", "shutting_down"}

# HC Livy state machine — observed live 2026-04-11 against MY_LAKEHOUSE:
# NotStarted → AcquiringHighConcurrencySession → Idle. `sessionId` may appear
# during acquisition but `replId` is only populated once the session reaches
# Idle, and both must be present before the statement URL can be built.
HC_ACQUIRING_STATES = {
    "notstarted",
    "starting",
    "acquiringhighconcurrencysession",
    "recovering",
}
HC_IDLE_STATE = "idle"
HC_DEAD_STATES = {"dead", "killed", "failed", "error", "shutting_down"}

STATEMENT_DONE_STATES = {"available", "error", "cancelled"}

SECTION_MARKER_PREFIX = "===NBMON_JI_MARKER="
SECTION_DONE_MARKER = f"{SECTION_MARKER_PREFIX}done==="


class LivyError(FabricApiError):
    """Raised when a Livy session / statement call fails."""


class LivyStatementError(LivyError):
    """Raised when a Scala statement runs but returns status=error.

    Holds the remote ename/evalue/traceback so callers can surface a useful
    message instead of a generic transport error.
    """

    def __init__(self, ename: str, evalue: str, traceback: list[str]):
        super().__init__(f"{ename}: {evalue}")
        self.ename = ename
        self.evalue = evalue
        self.traceback = traceback


@dataclass
class ForensicsResult:
    """Parsed output of one `SparkDiagnostic.analyze` + top-N forensics run."""

    top_stages: list[dict] = field(default_factory=list)
    top_tasks_spill: list[dict] = field(default_factory=list)
    executors: list[dict] = field(default_factory=list)
    top_queries: list[dict] = field(default_factory=list)
    raw_output: str = ""


# ---------------------------------------------------------------------------
# Livy client
# ---------------------------------------------------------------------------


class LivyClient:
    """Thin Livy HTTP wrapper scoped to one (workspace, lakehouse) endpoint.

    Separate from FabricApi because the Livy API lives under a different URL
    template (`.../lakehouses/{LH}/livyapi/...`) and its state/polling model
    differs from the Monitoring API.
    """

    def __init__(
        self,
        token: str,
        *,
        workspace_id: str,
        lakehouse_id: str,
        session_cache: pathlib.Path = DEFAULT_SESSION_CACHE,
        timeout: float = 30.0,
        environment_id: Optional[str] = None,
    ):
        self._token = token
        self._ws = workspace_id
        self._lh = lakehouse_id
        self._cache = session_cache
        self._timeout = timeout
        self._environment_id = environment_id

    def _base(self) -> str:
        return (
            f"{BASE_URL}/workspaces/{self._ws}/lakehouses/{self._lh}"
            f"/livyapi/versions/{LIVY_API_VERSION}"
        )

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    def _request(self, method: str, url: str, *, json_body: dict | None = None):
        resp = requests.request(
            method,
            url,
            headers=self._headers(),
            json=json_body,
            timeout=self._timeout,
        )
        if resp.status_code in (401, 403):
            raise FabricAuthExpired(
                f"Livy API rejected bearer token (HTTP {resp.status_code}). "
                "Run `fab auth login`.",
                status_code=resp.status_code,
            )
        if resp.status_code >= 400:
            raise LivyError(
                f"{method} {url} failed: HTTP {resp.status_code} {resp.text[:400]}",
                status_code=resp.status_code,
            )
        return resp

    def get_session(self, session_id: str) -> dict:
        resp = self._request("GET", f"{self._base()}/sessions/{session_id}")
        return resp.json()

    def create_session(self) -> str:
        # Default kind is Scala, which is exactly what JIL needs. When an
        # environment_id is provided, we pin the session to that Fabric
        # environment via the `spark.fabric.environmentDetails` conf key —
        # the value must be a *stringified* JSON object, not a nested dict
        # (Fabric Livy docs, "Integration with Fabric Environments"). This
        # is how we force Runtime 1.3+ even when the workspace default is
        # pinned to an older runtime like 1.2.
        body: dict = {}
        if self._environment_id:
            body["conf"] = {
                "spark.fabric.environmentDetails": json.dumps(
                    {"id": self._environment_id}
                )
            }
        resp = self._request("POST", f"{self._base()}/sessions", json_body=body)
        sid = resp.json().get("id")
        if not sid:
            raise LivyError(f"Livy create_session returned no id: {resp.text[:400]}")
        return sid

    def wait_for_idle(self, session_id: str, *, timeout: float = 300.0, poll: float = 5.0) -> str:
        """Block until the session reaches idle, or fail if it dies."""
        deadline = time.monotonic() + timeout
        while True:
            state = (self.get_session(session_id).get("state") or "").lower()
            if state in SESSION_IDLE_STATES:
                return state
            if state in SESSION_DEAD_STATES:
                raise LivyError(
                    f"Livy session {session_id} entered terminal state {state!r} "
                    "while waiting for idle."
                )
            if time.monotonic() >= deadline:
                raise LivyError(
                    f"Timed out waiting for Livy session {session_id} (last state={state!r})"
                )
            time.sleep(poll)

    def ensure_session(self, *, startup_timeout: float = 300.0) -> str:
        """Return a usable session id: reuse cached if alive, else create."""
        sid = self._read_cache()
        if sid:
            try:
                state = (self.get_session(sid).get("state") or "").lower()
                if state in SESSION_IDLE_STATES:
                    return sid
                if state in SESSION_TRANSIENT_STATES:
                    # Session still booting — wait for it, don't abandon.
                    self.wait_for_idle(sid, timeout=startup_timeout)
                    return sid
            except LivyError:
                # Cached id points to a gone/404'd session — ignore and create.
                pass
        sid = self.create_session()
        self._write_cache(sid)
        self.wait_for_idle(sid, timeout=startup_timeout)
        return sid

    def submit_statement(self, session_id: str, code: str, *, kind: str = "spark") -> int:
        """Submit a statement and return its numeric id. kind=spark → Scala."""
        resp = self._request(
            "POST",
            f"{self._base()}/sessions/{session_id}/statements",
            json_body={"code": code, "kind": kind},
        )
        sid = resp.json().get("id")
        if sid is None:
            raise LivyError(
                f"Livy submit_statement returned no id: {resp.text[:400]}"
            )
        return int(sid)

    def get_statement(self, session_id: str, statement_id: int) -> dict:
        resp = self._request(
            "GET",
            f"{self._base()}/sessions/{session_id}/statements/{statement_id}",
        )
        return resp.json()

    def wait_for_statement(
        self,
        session_id: str,
        statement_id: int,
        *,
        timeout: float = 600.0,
        poll: float = 3.0,
    ) -> dict:
        """Poll until statement is available/error/cancelled. Returns the payload."""
        deadline = time.monotonic() + timeout
        while True:
            payload = self.get_statement(session_id, statement_id)
            state = (payload.get("state") or "").lower()
            if state in STATEMENT_DONE_STATES:
                return payload
            if time.monotonic() >= deadline:
                raise LivyError(
                    f"Timed out waiting for Livy statement {statement_id} "
                    f"(last state={state!r})"
                )
            time.sleep(poll)

    # --- HC Livy (highConcurrencySessions + sessionTag REPL packing) ---

    def _hc_base(self) -> str:
        return f"{self._base()}/highConcurrencySessions"

    def create_hc_session(self, session_tag: str) -> dict:
        """POST a new HC session. Returns the full 202 response payload.

        Applies the same `spark.fabric.environmentDetails` conf key as the
        legacy `/sessions` path so HC sessions can be pinned to a specific
        runtime via an environment artifact — otherwise HC lands on the
        workspace default runtime, which blocks JIL when that default is 1.2.
        """
        body: dict = {"sessionTag": session_tag}
        if self._environment_id:
            body["conf"] = {
                "spark.fabric.environmentDetails": json.dumps(
                    {"id": self._environment_id}
                )
            }
        return self._request("POST", self._hc_base(), json_body=body).json()

    def get_hc_session(self, hc_id: str) -> dict:
        return self._request("GET", f"{self._hc_base()}/{hc_id}").json()

    def wait_for_hc_idle(
        self,
        hc_id: str,
        *,
        timeout: float = 300.0,
        poll: float = 5.0,
    ) -> tuple[str, str]:
        """Block until the HC session reaches Idle AND has both sessionId +
        replId populated. Returns `(sessionId, replId)`.

        Raises LivyError on terminal states or timeout. Note that `sessionId`
        may appear during `AcquiringHighConcurrencySession`, but `replId`
        lags until `Idle` — both are required for the statement URL.
        """
        deadline = time.monotonic() + timeout
        while True:
            payload = self.get_hc_session(hc_id)
            state = (payload.get("state") or "").lower()
            sid = payload.get("sessionId") or ""
            rid = payload.get("replId") or ""
            if state == HC_IDLE_STATE and sid and rid:
                return sid, rid
            if state in HC_DEAD_STATES:
                raise LivyError(
                    f"HC session {hc_id} entered terminal state {state!r} "
                    "while waiting for idle."
                )
            if time.monotonic() >= deadline:
                raise LivyError(
                    f"Timed out waiting for HC session {hc_id} "
                    f"(last state={state!r}, sessionId={sid or 'null'}, "
                    f"replId={rid or 'null'})"
                )
            time.sleep(poll)

    def ensure_hc_session(
        self,
        session_tag: str,
        *,
        startup_timeout: float = 300.0,
        cache: pathlib.Path = DEFAULT_HC_SESSION_CACHE,
    ) -> tuple[str, str, str]:
        """Return `(hc_id, sessionId, replId)` — reuse cache if alive,
        otherwise acquire a new HC session with the given `session_tag`.

        The service packs concurrent acquires with the same tag onto one
        underlying Livy session (~5 REPLs max), so a cache miss after the
        first acquire is cheap — the new REPL joins the already-warm Spark.
        """
        triple = self._read_hc_cache(cache)
        if triple:
            hc_id, cached_sid, cached_rid = triple
            try:
                payload = self.get_hc_session(hc_id)
                state = (payload.get("state") or "").lower()
                if state == HC_IDLE_STATE:
                    sid = payload.get("sessionId") or cached_sid
                    rid = payload.get("replId") or cached_rid
                    if sid and rid:
                        return hc_id, sid, rid
                elif state in HC_ACQUIRING_STATES:
                    sid, rid = self.wait_for_hc_idle(
                        hc_id, timeout=startup_timeout
                    )
                    self._write_hc_cache(cache, hc_id, sid, rid)
                    return hc_id, sid, rid
                # Terminal state → fall through to create a new one.
            except LivyError:
                # 404 or other transport — cache is stale, acquire fresh.
                pass

        resp = self.create_hc_session(session_tag)
        hc_id = resp.get("id") or ""
        if not hc_id:
            raise LivyError(
                f"HC create_hc_session returned no id: {str(resp)[:400]}"
            )
        sid, rid = self.wait_for_hc_idle(hc_id, timeout=startup_timeout)
        self._write_hc_cache(cache, hc_id, sid, rid)
        return hc_id, sid, rid

    def submit_hc_statement(
        self,
        session_id: str,
        repl_id: str,
        code: str,
        *,
        kind: str = "spark",
    ) -> int:
        """Submit a statement to an HC REPL. Note the statement URL uses the
        underlying Livy `sessionId` + `replId`, NOT the HC session id.
        `kind=spark` means Scala, matching `submit_statement`.
        """
        resp = self._request(
            "POST",
            f"{self._hc_base()}/{session_id}/repls/{repl_id}/statements",
            json_body={"code": code, "kind": kind},
        )
        stmt_id = resp.json().get("id")
        if stmt_id is None:
            raise LivyError(
                f"HC submit_hc_statement returned no id: {resp.text[:400]}"
            )
        return int(stmt_id)

    def get_hc_statement(
        self, session_id: str, repl_id: str, statement_id: int
    ) -> dict:
        return self._request(
            "GET",
            f"{self._hc_base()}/{session_id}/repls/{repl_id}"
            f"/statements/{statement_id}",
        ).json()

    def wait_for_hc_statement(
        self,
        session_id: str,
        repl_id: str,
        statement_id: int,
        *,
        timeout: float = 600.0,
        poll: float = 3.0,
    ) -> dict:
        """Poll an HC statement until available/error/cancelled."""
        deadline = time.monotonic() + timeout
        while True:
            payload = self.get_hc_statement(session_id, repl_id, statement_id)
            state = (payload.get("state") or "").lower()
            if state in STATEMENT_DONE_STATES:
                return payload
            if time.monotonic() >= deadline:
                raise LivyError(
                    f"Timed out waiting for HC statement {statement_id} "
                    f"(last state={state!r})"
                )
            time.sleep(poll)

    # --- session cache ---

    def _read_cache(self) -> Optional[str]:
        try:
            content = self._cache.read_text().strip()
        except FileNotFoundError:
            return None
        except OSError:
            return None
        return content or None

    def _write_cache(self, session_id: str) -> None:
        try:
            self._cache.parent.mkdir(parents=True, exist_ok=True)
            self._cache.write_text(session_id)
        except OSError:
            # Cache write is best-effort — next run will just re-create.
            pass

    @staticmethod
    def _read_hc_cache(
        cache: pathlib.Path,
    ) -> Optional[tuple[str, str, str]]:
        try:
            content = cache.read_text().strip()
        except (FileNotFoundError, OSError):
            return None
        parts = content.split(":")
        if len(parts) != 3 or not all(parts):
            return None
        return parts[0], parts[1], parts[2]

    @staticmethod
    def _write_hc_cache(
        cache: pathlib.Path, hc_id: str, sid: str, rid: str
    ) -> None:
        try:
            cache.parent.mkdir(parents=True, exist_ok=True)
            cache.write_text(f"{hc_id}:{sid}:{rid}")
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Scala statement builder
# ---------------------------------------------------------------------------


def _scala_str(s: str) -> str:
    """Quote a Python string for safe inclusion in a Scala double-quoted literal.

    JIL takes UUIDs and plain ABFSS paths, which don't contain backslashes or
    quotes in normal use. We still escape defensively so a malformed
    parameter can't break out of the literal.
    """
    return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'


def build_scala_statement(
    *,
    workspace_id: str,
    artifact_id: str,
    livy_id: str,
    job_type: str,
    state_store_path: str,
    attempt_id: int,
) -> str:
    """Compose the one-shot JIL forensics Scala statement.

    The statement prints section markers (`===NBMON_JI_MARKER=<name>===`)
    before each `show()` call so the caller can split the text/plain output
    into labelled chunks before parsing the ASCII tables.
    """
    ws = _scala_str(workspace_id)
    item = _scala_str(artifact_id)
    livy = _scala_str(livy_id)
    jt = _scala_str(job_type)
    ssp = _scala_str(state_store_path)
    return f"""import com.microsoft.jobinsight.diagnostic.SparkDiagnostic
import org.apache.spark.sql.functions._

val ji = SparkDiagnostic.analyze(
  {ws},
  {item},
  {livy},
  {jt},
  {ssp},
  {attempt_id}
)

println("{SECTION_MARKER_PREFIX}top_stages===")
ji.stages
  .select("stageId", "name", "duration", "numTasks", "shuffleWriteBytes", "diskSpilled")
  .orderBy(desc("duration"))
  .limit(5)
  .show(truncate = false)

println("{SECTION_MARKER_PREFIX}top_tasks_spill===")
ji.tasks
  .select("taskId", "stageId", "executorId", "duration", "diskBytesSpilled", "memoryBytesSpilled")
  .orderBy(desc("diskBytesSpilled"))
  .limit(5)
  .show(truncate = false)

println("{SECTION_MARKER_PREFIX}executors===")
ji.executors
  .select("executorId", "totalGCTime", "totalDuration", "totalTasks", "failedTasks")
  .orderBy(desc("totalGCTime"))
  .show(truncate = false)

println("{SECTION_MARKER_PREFIX}top_queries===")
ji.queries
  .select("executionId", "description", "duration")
  .orderBy(desc("duration"))
  .limit(5)
  .show(truncate = false)

println("{SECTION_DONE_MARKER}")
"""


# ---------------------------------------------------------------------------
# show() output parser
# ---------------------------------------------------------------------------


_TABLE_SEP_RE = re.compile(r"^\+[-+]+\+$")


def parse_show_table(block: str) -> list[dict]:
    """Parse a single Spark `show(truncate=false)` ASCII table.

    Structure:
        +---+---+
        |col|col|
        +---+---+
        |val|val|
        +---+---+

    Returns one dict per data row, keyed by the header column names (whitespace
    stripped). Empty tables (zero data rows) return []. Non-table input
    returns [] rather than raising — caller decides how strict to be.
    """
    lines = [ln.rstrip() for ln in block.splitlines()]
    # Find the first separator; header is the next `|...|` line.
    sep_idxs = [i for i, ln in enumerate(lines) if _TABLE_SEP_RE.match(ln)]
    if len(sep_idxs) < 2:
        return []
    header_start = sep_idxs[0] + 1
    if header_start >= len(lines) or not lines[header_start].startswith("|"):
        return []
    headers = [c.strip() for c in lines[header_start].strip("|").split("|")]
    # Data rows live between the 2nd and 3rd separator. If only 2 separators
    # exist, the table is empty.
    if len(sep_idxs) < 3:
        return []
    data_start = sep_idxs[1] + 1
    data_end = sep_idxs[2]
    rows: list[dict] = []
    for ln in lines[data_start:data_end]:
        if not ln.startswith("|"):
            continue
        values = [c.strip() for c in ln.strip("|").split("|")]
        if len(values) != len(headers):
            continue
        rows.append(dict(zip(headers, values)))
    return rows


def split_sections(output: str) -> dict[str, str]:
    """Split combined statement output by NBMON_JI_MARKER= section headers.

    Returns a dict {section_name: text_block}. Content before the first
    marker is discarded. Content in the optional `done` section is not
    returned.
    """
    sections: dict[str, str] = {}
    current_name: Optional[str] = None
    current_buf: list[str] = []
    for raw_line in output.splitlines():
        line = raw_line.rstrip()
        if line.startswith(SECTION_MARKER_PREFIX) and line.endswith("==="):
            if current_name is not None:
                sections[current_name] = "\n".join(current_buf)
            name = line[len(SECTION_MARKER_PREFIX) : -3]
            if name == "done":
                current_name = None
                current_buf = []
                continue
            current_name = name
            current_buf = []
            continue
        if current_name is not None:
            current_buf.append(raw_line)
    if current_name is not None:
        sections[current_name] = "\n".join(current_buf)
    return sections


def parse_forensics_output(output: str) -> ForensicsResult:
    """Parse the full text/plain output of one forensics statement."""
    sections = split_sections(output)
    return ForensicsResult(
        top_stages=parse_show_table(sections.get("top_stages", "")),
        top_tasks_spill=parse_show_table(sections.get("top_tasks_spill", "")),
        executors=parse_show_table(sections.get("executors", "")),
        top_queries=parse_show_table(sections.get("top_queries", "")),
        raw_output=output,
    )


def extract_text_plain(statement_payload: dict) -> str:
    """Pull the text/plain body out of a Livy statement response.

    Raises LivyStatementError if the statement's status is `error`.
    """
    out = statement_payload.get("output") or {}
    status = out.get("status")
    if status == "error":
        raise LivyStatementError(
            out.get("ename", "UnknownError"),
            out.get("evalue", ""),
            out.get("traceback") or [],
        )
    data = out.get("data") or {}
    text = data.get("text/plain")
    if text is None:
        return ""
    # Livy returns text/plain as a plain string; some implementations return
    # a list of strings. Handle both.
    if isinstance(text, list):
        return "".join(text)
    return str(text)


# ---------------------------------------------------------------------------
# High-level entry point
# ---------------------------------------------------------------------------


def default_state_store_path(
    *, session_ws_id: str, session_lh_id: str, livy_id: str
) -> str:
    """Build a OneLake ABFSS state store path scoped to the target run.

    OneLake accepts workspace-GUID@onelake.dfs.fabric.microsoft.com as the
    container and `<lakehouse-guid>/Files/...` as the path. Scoping under
    the target livy_id keeps multiple runs' intermediate state isolated.
    """
    return (
        f"abfss://{session_ws_id}@onelake.dfs.fabric.microsoft.com/"
        f"{session_lh_id}/Files/jobinsight/state/{livy_id}"
    )


def run_forensics(
    token: str,
    *,
    workspace_id: str,
    artifact_id: str,
    livy_id: str,
    # jobType is a literal Livy resource name — "sessions" for notebooks,
    # "batches" for Spark Job Definitions. NOT the Fabric item-type name.
    # attempt_id is 1-indexed per the JIL sample notebook ("default is 1").
    job_type: str = "sessions",
    attempt_id: int = 1,
    session_ws_id: Optional[str] = None,
    session_lh_id: Optional[str] = None,
    state_store_path: Optional[str] = None,
    session_cache: pathlib.Path = DEFAULT_SESSION_CACHE,
    startup_timeout: float = 300.0,
    statement_timeout: float = 600.0,
    environment_id: Optional[str] = None,
    use_hc: bool = False,
    hc_session_tag: str = DEFAULT_HC_SESSION_TAG,
    hc_session_cache: pathlib.Path = DEFAULT_HC_SESSION_CACHE,
) -> ForensicsResult:
    """Run JIL forensics against a completed Spark job via Livy.

    Args:
        token:          MSAL bearer (from auth.get_fabric_token()).
        workspace_id:   Target job's workspace UUID.
        artifact_id:    Target notebook/item UUID.
        livy_id:        Target Livy session UUID (from nbmon list/status).
        job_type:       JIL job type (NotebookRun / SparkJobDef / Lakehouse).
        attempt_id:     Run attempt number (0 for the first).
        session_ws_id:  Workspace UUID for the Livy session hosting JIL.
                        Defaults to `workspace_id` (guarantees same capacity).
        session_lh_id:  Lakehouse UUID for the Livy session endpoint. Any
                        lakehouse in the same capacity works. Defaults to
                        env `NBMON_LIVY_LAKEHOUSE_ID` or the default
                        Dev MY_LAKEHOUSE id.
        state_store_path: ABFSS path JIL will use as its intermediate state
                        area. Defaults to a per-livy subfolder under the
                        session lakehouse's Files area.
        environment_id: Fabric Environment artifact GUID. When set, the
                        Livy session is pinned to that environment's
                        runtime + compute via the Fabric
                        `spark.fabric.environmentDetails` conf key.
                        Required when the workspace default runtime is
                        older than 1.3 (which is when JIL is unavailable).
                        Defaults to env `NBMON_JI_ENVIRONMENT_ID`.

    Returns:
        ForensicsResult with top-N stages, spill tasks, executors and queries.
    """
    if session_ws_id is None:
        session_ws_id = workspace_id
    if session_lh_id is None:
        session_lh_id = os.environ.get("NBMON_LIVY_LAKEHOUSE_ID", DEFAULT_LIVY_LH)
    if environment_id is None:
        environment_id = os.environ.get("NBMON_JI_ENVIRONMENT_ID") or None
    if state_store_path is None:
        state_store_path = default_state_store_path(
            session_ws_id=session_ws_id,
            session_lh_id=session_lh_id,
            livy_id=livy_id,
        )

    client = LivyClient(
        token,
        workspace_id=session_ws_id,
        lakehouse_id=session_lh_id,
        session_cache=session_cache,
        environment_id=environment_id,
    )
    code = build_scala_statement(
        workspace_id=workspace_id,
        artifact_id=artifact_id,
        livy_id=livy_id,
        job_type=job_type,
        state_store_path=state_store_path,
        attempt_id=attempt_id,
    )
    if use_hc:
        _, sid, rid = client.ensure_hc_session(
            hc_session_tag,
            startup_timeout=startup_timeout,
            cache=hc_session_cache,
        )
        statement_id = client.submit_hc_statement(sid, rid, code, kind="spark")
        payload = client.wait_for_hc_statement(
            sid, rid, statement_id, timeout=statement_timeout
        )
    else:
        session_id = client.ensure_session(startup_timeout=startup_timeout)
        statement_id = client.submit_statement(session_id, code, kind="spark")
        payload = client.wait_for_statement(
            session_id, statement_id, timeout=statement_timeout
        )
    output = extract_text_plain(payload)
    return parse_forensics_output(output)
