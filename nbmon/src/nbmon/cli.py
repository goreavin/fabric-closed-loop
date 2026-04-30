from __future__ import annotations

import argparse
import os
import sys
from typing import Sequence

from nbmon import auth, run_resolver, theme
from nbmon.banner import compute_duration_seconds, extract_advise, format_banner
from nbmon.fabric_api import FabricApi, FabricApiError, FabricAuthExpired
from nbmon.job_insight import (
    ForensicsResult,
    LivyError,
    LivyStatementError,
    run_forensics,
)
from nbmon.profile import ProfileResult, StageSkew, run_profile
from nbmon.log_streamer import LivyPollingLost, LogStreamer
from nbmon.run_resolver import RunNotFound, RunSpec
from nbmon.status_extractor import extract_status
from nbmon.status_renderer import render_status_banner
from nbmon.submit import SubmitError, submit_notebook, wait_for_session_with_app

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_CANCELLED = 2
EXIT_AUTH = 3
EXIT_TRANSPORT = 4
# Polling budget exhausted mid-run. The notebook may still be executing
# server-side — callers should reconcile via `nbmon status`, not assume
# failure. See bug `2026-04-11-submit-double-run-on-poll-timeout.md`.
EXIT_POLLING_LOST = 6
EXIT_SIGINT = 130

# Window (seconds) around a tracked livy's submittedDateTime within which
# any OTHER livy session on the same item is flagged as a suspected
# duplicate. Fabric's scheduler occasionally spawns a retry run that nbmon
# cannot prevent; Layer 3 of the bug fix makes these visible after the
# fact rather than silently colliding on Delta writes.
LIVY_DUPLICATE_WINDOW_SECONDS = 120.0

TERMINAL_STATES = {
    "Succeeded": EXIT_OK,
    "Failed": EXIT_FAILED,
    "Error": EXIT_FAILED,
    "Cancelled": EXIT_CANCELLED,
    "Dead": EXIT_CANCELLED,
    "Killed": EXIT_CANCELLED,
}


def state_to_exit_code(state: str) -> int | None:
    return TERMINAL_STATES.get(state)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="nbmon",
        description="Tail Fabric notebook driver stdout/stderr live.",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    def _add_stream_args(sp):
        sp.add_argument(
            "--stream",
            default="stderr",
            help="Comma-separated streams to tail: stdout,stderr (default: stderr)",
        )
        sp.add_argument("--poll", type=float, default=5.0)
        sp.add_argument("--since", type=int, default=0)

    attach = sub.add_parser("attach", help="Attach to a notebook run and tail its driver log")
    attach.add_argument("path", help="Workspace path: '<ws>.Workspace/<item>.Notebook'")
    attach.add_argument(
        "--run",
        default="latest",
        help="latest | livy:UUID | jobInstance:UUID (default: latest)",
    )
    _add_stream_args(attach)
    attach.add_argument(
        "--once",
        action="store_true",
        help="One-shot: print current log content and exit (Phase A behaviour).",
    )

    submit = sub.add_parser(
        "submit", help="Submit a notebook with `fab job start` and tail its driver log"
    )
    submit.add_argument("path")
    submit.add_argument(
        "--pool",
        default="HighConcurrency",
        choices=["HighConcurrency", "Starter"],
    )
    _add_stream_args(submit)
    submit.add_argument(
        "--wait-timeout",
        type=float,
        default=300.0,
        help="Seconds to wait for sparkApplicationId after submit (default: 300)",
    )

    list_cmd = sub.add_parser("list", help="List recent runs of a notebook")
    list_cmd.add_argument("path")
    list_cmd.add_argument("--limit", type=int, default=20)

    status_cmd = sub.add_parser(
        "status",
        help="Print the rich status banner only (no log dump). Default for /nbmon.",
    )
    status_cmd.add_argument("path")
    status_cmd.add_argument("--run", default="latest")
    status_cmd.add_argument(
        "--theme", default="claude", choices=sorted(theme.THEMES.keys()),
    )
    status_cmd.add_argument(
        "--color", default="auto", choices=["auto", "always", "never"],
    )

    forensics_cmd = sub.add_parser(
        "forensics",
        help=(
            "Run Fabric Job Insight Library forensics on a completed run via "
            "a warm Livy Scala session. Prints top stages / spill tasks / "
            "executor GC / slow queries."
        ),
    )
    forensics_cmd.add_argument("path")
    forensics_cmd.add_argument("--run", default="latest")
    forensics_cmd.add_argument(
        "--job-type",
        default="sessions",
        choices=["sessions", "batches"],
        help=(
            "Livy resource name: 'sessions' for notebook runs, 'batches' "
            "for Spark Job Definitions."
        ),
    )
    forensics_cmd.add_argument(
        "--attempt-id",
        type=int,
        default=1,
        help="JIL attempt number (1-indexed per MS sample; default 1).",
    )
    forensics_cmd.add_argument(
        "--livy-workspace",
        default=None,
        help=(
            "Workspace UUID for the Livy session hosting JIL (defaults to the "
            "target notebook's workspace — same capacity guaranteed)."
        ),
    )
    forensics_cmd.add_argument(
        "--livy-lakehouse",
        default=None,
        help=(
            "Lakehouse UUID for the Livy endpoint. Defaults to env "
            "NBMON_LIVY_LAKEHOUSE_ID or the default lakehouse id."
        ),
    )
    forensics_cmd.add_argument(
        "--state-store-path",
        default=None,
        help=(
            "ABFSS path for JIL intermediate state. Defaults to a per-livy "
            "subfolder under the session lakehouse's Files area."
        ),
    )
    forensics_cmd.add_argument(
        "--environment-id",
        default=None,
        help=(
            "Fabric Environment artifact GUID to pin the Livy session to "
            "(forces a specific runtime version). Required when the "
            "workspace default runtime is older than 1.3. Defaults to env "
            "NBMON_JI_ENVIRONMENT_ID."
        ),
    )
    forensics_cmd.add_argument(
        "--startup-timeout",
        type=float,
        default=300.0,
        help="Seconds to wait for a cold Livy session to reach idle.",
    )
    forensics_cmd.add_argument(
        "--statement-timeout",
        type=float,
        default=600.0,
        help="Seconds to wait for the JIL Scala statement to complete.",
    )
    forensics_cmd.add_argument(
        "--hc",
        action="store_true",
        default=bool(os.environ.get("NBMON_USE_HC")),
        help=(
            "Use HighConcurrency Livy (/highConcurrencySessions with a "
            "sessionTag) instead of a dedicated /sessions session. REPLs "
            "with the same tag pack onto one warm Spark session — first "
            "acquire pays the cold-start, subsequent REPLs join instantly. "
            "Opt in with --hc or env NBMON_USE_HC=1."
        ),
    )
    forensics_cmd.add_argument(
        "--hc-session-tag",
        default=None,
        help=(
            "Override the HC sessionTag (default: nbmon-jil). Only used "
            "when --hc is set."
        ),
    )
    forensics_cmd.add_argument(
        "--theme", default="claude", choices=sorted(theme.THEMES.keys()),
    )
    forensics_cmd.add_argument(
        "--color", default="auto", choices=["auto", "always", "never"],
    )

    profile_cmd = sub.add_parser(
        "profile",
        help=(
            "Profile a completed Spark run via the Fabric Spark Monitoring "
            "REST API. Surfaces top-N slow stages, task-time skew (p95/p50), "
            "disk spill, and executor GC hotspots. Server-side — no Livy "
            "session, no runtime pin, no extra Entra scopes."
        ),
    )
    profile_cmd.add_argument("path")
    profile_cmd.add_argument("--run", default="latest")
    profile_cmd.add_argument(
        "--top-n",
        type=int,
        default=5,
        help="How many stages to sample for task-summary (default 5).",
    )
    profile_cmd.add_argument(
        "--skew-threshold",
        type=float,
        default=2.0,
        help=(
            "p95/p50 task-time ratio above which a stage is flagged as "
            "skewed (default 2.0; Spark History Server Diagnosis defaults "
            "to 3.0)."
        ),
    )
    profile_cmd.add_argument(
        "--theme", default="claude", choices=sorted(theme.THEMES.keys()),
    )
    profile_cmd.add_argument(
        "--color", default="auto", choices=["auto", "always", "never"],
    )

    return p


def _emit_error(msg: str) -> None:
    print(f"nbmon: {msg}", file=sys.stderr)


def _print_banner(api, ws_id, item_id, session, final_state):
    livy_id = session.get("livyId", "?")
    app_id = session.get("sparkApplicationId")
    duration = compute_duration_seconds(session)
    advise = None
    if app_id and final_state in {"Failed", "Error"}:
        try:
            blob = api.driver_log_full(ws_id, item_id, livy_id, app_id, "stderr")
            advise = extract_advise(blob)
        except FabricApiError:
            advise = None
    banner = format_banner(
        state=final_state,
        duration_seconds=duration,
        livy_id=livy_id,
        app_id=app_id,
        advise=advise,
    )
    print(banner, file=sys.stderr)


def _streams_from_arg(arg: str) -> list[tuple[str, str]]:
    names = [s.strip() for s in arg.split(",") if s.strip()]
    prefixes = {"stdout": "[OUT] ", "stderr": "[ERR] "}
    return [(n, prefixes.get(n, f"[{n}] ")) for n in names]


def _parse_iso8601(ts: str | None) -> float | None:
    """Best-effort ISO 8601 → epoch seconds. Returns None on failure."""
    if not ts:
        return None
    import datetime as _dt

    try:
        # Fabric returns Z-suffixed UTC timestamps. Python's fromisoformat
        # handles `+00:00` directly; swap Z for that to stay 3.10-compatible.
        return _dt.datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def _check_duplicate_livy(
    api,
    ws_id: str,
    item_id: str,
    tracked_livy: str,
    tracked_submitted_at: str | None,
    *,
    window_seconds: float = LIVY_DUPLICATE_WINDOW_SECONDS,
) -> list[dict]:
    """Return other livy sessions on the same item within `window_seconds`
    of the tracked livy's submitted timestamp. Empty list when there's
    nothing to flag.

    Used defensively after a streamer returns: if Fabric's scheduler (or
    another client) spawned a second run for the same notebook around the
    same time, both runs may have raced on Delta writes. nbmon can't
    prevent that, but it can surface it so the user knows to reconcile.
    """
    try:
        sessions = api.list_livy_sessions(ws_id, item_id)
    except FabricApiError:
        return []
    tracked_ts = _parse_iso8601(tracked_submitted_at)
    # If we don't know when ours was submitted, fall back to listing any
    # session other than the tracked one that finished recently enough to
    # still be interesting. Only flag if the list itself contains another
    # entry whose submission we CAN place within the window — otherwise
    # there's no reliable signal.
    dupes: list[dict] = []
    for s in sessions:
        other_livy = s.get("livyId")
        if not other_livy or other_livy == tracked_livy:
            continue
        other_ts = _parse_iso8601(s.get("submittedDateTime"))
        if tracked_ts is None or other_ts is None:
            continue
        if abs(other_ts - tracked_ts) <= window_seconds:
            dupes.append(s)
    return dupes


def _warn_duplicate_livy(dupes: list[dict], tracked_livy: str) -> None:
    """Render a red warning banner listing duplicate livy sessions.

    Always goes to stderr with ANSI escapes so it stands out even when
    stdout is piped/redirected. We don't use the Theme here because the
    warning must pop regardless of --color/TTY detection.
    """
    if not dupes:
        return
    RED = "\x1b[1;31m"
    DIM = "\x1b[2m"
    RST = "\x1b[0m"
    print(
        f"{RED}!! WARNING: {len(dupes) + 1} livy sessions for this notebook "
        f"within {int(LIVY_DUPLICATE_WINDOW_SECONDS)}s of each other.{RST}",
        file=sys.stderr,
    )
    print(f"   Tracking: {tracked_livy}", file=sys.stderr)
    for d in dupes:
        other_livy = d.get("livyId", "?")
        state = d.get("state", "?")
        submitted = d.get("submittedDateTime", "?")
        print(
            f"   Also ran: {other_livy}  "
            f"state={state}  submitted={submitted}  "
            f"{DIM}(NOT started by this nbmon process){RST}",
            file=sys.stderr,
        )
    print(
        f"   {DIM}Managed Delta tables written by both runs may have "
        f"collided. Inspect _delta_log for ProtocolChangedException and "
        f"reconcile before trusting the output.{RST}",
        file=sys.stderr,
    )


def _stream_session(api, ws_id, item_id, session, args) -> int:
    livy_id = session["livyId"]
    app_id = session.get("sparkApplicationId")
    state = session.get("state", "Unknown")
    tracked_submitted_at = session.get("submittedDateTime")
    if app_id is None:
        _emit_error(
            f"Session {livy_id} has no sparkApplicationId yet (state={state}); "
            "cannot stream driver logs."
        )
        return EXIT_FAILED

    streamer = LogStreamer(api, ws_id, item_id, livy_id, app_id)
    streams = _streams_from_arg(args.stream)
    try:
        final_state = streamer.run_until_terminal(
            streams, out=sys.stdout.buffer, poll=args.poll, since=args.since
        )
    except FabricAuthExpired as e:
        _emit_error(str(e))
        return EXIT_AUTH
    except LivyPollingLost as e:
        _emit_error(str(e))
        # Even on polling-lost we still try to reconcile duplicates — the
        # remote notebook may have completed (and a duplicate may exist)
        # even though we couldn't observe the transition live.
        dupes = _check_duplicate_livy(
            api, ws_id, item_id, livy_id, tracked_submitted_at
        )
        _warn_duplicate_livy(dupes, livy_id)
        return EXIT_POLLING_LOST
    except FabricApiError as e:
        _emit_error(str(e))
        return EXIT_TRANSPORT
    sys.stdout.flush()
    final_session = api.get_livy_session(ws_id, item_id, livy_id)
    _print_banner(api, ws_id, item_id, final_session, final_state)
    dupes = _check_duplicate_livy(
        api, ws_id, item_id, livy_id, tracked_submitted_at
    )
    _warn_duplicate_livy(dupes, livy_id)
    rc = state_to_exit_code(final_state)
    return rc if rc is not None else EXIT_OK


def _cmd_attach(args) -> int:
    try:
        token = auth.get_fabric_token()
    except auth.AuthError as e:
        _emit_error(str(e))
        return EXIT_AUTH

    api = FabricApi(token)
    try:
        ws_id, item_id = run_resolver.resolve_workspace_item(args.path)
        spec = RunSpec.parse(args.run)
        session = run_resolver.select_run(api, ws_id, item_id, spec)
    except RunNotFound as e:
        _emit_error(str(e))
        return EXIT_FAILED
    except FabricAuthExpired as e:
        _emit_error(str(e))
        return EXIT_AUTH
    except FabricApiError as e:
        _emit_error(str(e))
        return EXIT_TRANSPORT

    livy_id = session["livyId"]
    app_id = session.get("sparkApplicationId")
    state = session.get("state", "Unknown")

    if args.once:
        if app_id is None:
            _emit_error(
                f"Session {livy_id} has no sparkApplicationId yet (state={state}); "
                "nothing to print in --once mode."
            )
            return EXIT_FAILED
        once_stream = args.stream.split(",")[0].strip() or "stderr"
        try:
            blob = api.driver_log_full(ws_id, item_id, livy_id, app_id, once_stream)
        except FabricAuthExpired as e:
            _emit_error(str(e))
            return EXIT_AUTH
        except FabricApiError as e:
            _emit_error(str(e))
            return EXIT_TRANSPORT
        sys.stdout.buffer.write(blob)
        sys.stdout.flush()
        if state in TERMINAL_STATES:
            advise = extract_advise(blob) if state in {"Failed", "Error"} else None
            banner = format_banner(
                state=state,
                duration_seconds=compute_duration_seconds(session),
                livy_id=livy_id,
                app_id=app_id,
                advise=advise,
            )
            print(banner, file=sys.stderr)
        rc = state_to_exit_code(state)
        return rc if rc is not None else EXIT_OK

    return _stream_session(api, ws_id, item_id, session, args)


def _cmd_submit(args) -> int:
    try:
        token = auth.get_fabric_token()
    except auth.AuthError as e:
        _emit_error(str(e))
        return EXIT_AUTH

    api = FabricApi(token)
    try:
        ws_id, item_id = run_resolver.resolve_workspace_item(args.path)
    except FabricApiError as e:
        _emit_error(str(e))
        return EXIT_TRANSPORT

    print(f"nbmon: submitting `{args.path}` (pool={args.pool})...", file=sys.stderr)
    try:
        job_instance_id = submit_notebook(args.path, pool=args.pool)
    except SubmitError as e:
        _emit_error(str(e))
        return EXIT_TRANSPORT

    print(f"nbmon: jobInstance={job_instance_id} — waiting for Spark app...", file=sys.stderr)
    try:
        session = wait_for_session_with_app(
            api, ws_id, item_id, job_instance_id, poll=2.0, timeout=args.wait_timeout
        )
    except SubmitError as e:
        _emit_error(str(e))
        return EXIT_TRANSPORT
    except FabricAuthExpired as e:
        _emit_error(str(e))
        return EXIT_AUTH
    except FabricApiError as e:
        _emit_error(str(e))
        return EXIT_TRANSPORT

    print(
        f"nbmon: livy={session['livyId']} app={session['sparkApplicationId']} — streaming...",
        file=sys.stderr,
    )
    return _stream_session(api, ws_id, item_id, session, args)


def _cmd_list(args) -> int:
    try:
        token = auth.get_fabric_token()
    except auth.AuthError as e:
        _emit_error(str(e))
        return EXIT_AUTH
    api = FabricApi(token)
    try:
        ws_id, item_id = run_resolver.resolve_workspace_item(args.path)
        sessions = api.list_livy_sessions(ws_id, item_id)
    except FabricAuthExpired as e:
        _emit_error(str(e))
        return EXIT_AUTH
    except FabricApiError as e:
        _emit_error(str(e))
        return EXIT_TRANSPORT
    sessions = sorted(sessions, key=lambda s: s.get("submittedDateTime", ""), reverse=True)
    for s in sessions[: args.limit]:
        print(
            f"{s.get('submittedDateTime', '?'):25}  "
            f"{s.get('state', '?'):12}  "
            f"livy={s.get('livyId', '?')}  "
            f"job={s.get('jobInstanceId', '?')}  "
            f"app={s.get('sparkApplicationId', '?')}"
        )
    return EXIT_OK


def _resolve_theme(args) -> "theme.Theme":
    color_mode = getattr(args, "color", "auto")
    if color_mode == "always":
        is_tty = True
    elif color_mode == "never":
        is_tty = False
    else:
        is_tty = sys.stdout.isatty()
    return theme.get_theme(name=getattr(args, "theme", "claude"), is_tty=is_tty)


def _cmd_status(args) -> int:
    try:
        token = auth.get_fabric_token()
    except auth.AuthError as e:
        _emit_error(str(e))
        return EXIT_AUTH

    api = FabricApi(token)
    try:
        ws_id, item_id = run_resolver.resolve_workspace_item(args.path)
        spec = RunSpec.parse(args.run)
        session = run_resolver.select_run(api, ws_id, item_id, spec)
    except RunNotFound as e:
        _emit_error(str(e))
        return EXIT_FAILED
    except FabricAuthExpired as e:
        _emit_error(str(e))
        return EXIT_AUTH
    except FabricApiError as e:
        _emit_error(str(e))
        return EXIT_TRANSPORT

    livy_id = session["livyId"]
    app_id = session.get("sparkApplicationId")
    state = session.get("state", "Unknown")

    status_info = None
    advise = None
    if app_id is not None:
        try:
            stdout_blob = api.driver_log_full(ws_id, item_id, livy_id, app_id, "stdout")
            status_info = extract_status(stdout_blob)
        except FabricApiError:
            from nbmon.status_extractor import StatusInfo

            status_info = StatusInfo()
        if state in {"Failed", "Error", "Cancelled", "Dead", "Killed"}:
            try:
                stderr_blob = api.driver_log_full(
                    ws_id, item_id, livy_id, app_id, "stderr"
                )
                advise = extract_advise(stderr_blob)
            except FabricApiError:
                advise = None
    else:
        from nbmon.status_extractor import StatusInfo

        status_info = StatusInfo()

    selected_theme = _resolve_theme(args)
    banner = render_status_banner(
        session, status_info, advise=advise, theme=selected_theme
    )
    print(banner)

    rc = state_to_exit_code(state)
    return rc if rc is not None else EXIT_OK


def _fmt_bytes(raw: str) -> str:
    """Format a numeric string as KB/MB/GB/TB, pass-through on parse failure."""
    try:
        n = float(raw)
    except (TypeError, ValueError):
        return raw
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024.0 or unit == "TB":
            if unit == "B":
                return f"{int(n)} B"
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return raw


def _fmt_ms(raw: str) -> str:
    """Format a millisecond count as `N.Ns`, pass-through on parse failure."""
    try:
        n = float(raw)
    except (TypeError, ValueError):
        return raw
    if n >= 1000.0:
        return f"{n / 1000.0:.1f}s"
    return f"{int(n)}ms"


def _render_forensics(result: ForensicsResult, selected_theme) -> str:
    """Compact themed summary — four lines, one per section.

    Each line drops empty sections silently. Byte and time columns are
    formatted human-friendly; raw strings pass through if parsing fails
    (keeps us resilient to JIL schema changes).
    """
    R = selected_theme.reset
    lines: list[str] = [
        f"{selected_theme.box}── Job Insight Forensics ──{R}"
    ]

    if result.top_stages:
        row = result.top_stages[0]
        lines.append(
            f"{selected_theme.label}top stage  :{R} "
            f"#{row.get('stageId', '?')} {row.get('name', '?')}  "
            f"dur={_fmt_ms(row.get('duration', ''))}  "
            f"tasks={row.get('numTasks', '?')}  "
            f"shuffleW={_fmt_bytes(row.get('shuffleWriteBytes', ''))}  "
            f"spill={_fmt_bytes(row.get('diskSpilled', ''))}"
        )
    if result.top_tasks_spill:
        row = result.top_tasks_spill[0]
        disk = _fmt_bytes(row.get("diskBytesSpilled", ""))
        mem = _fmt_bytes(row.get("memoryBytesSpilled", ""))
        lines.append(
            f"{selected_theme.label}top spill  :{R} "
            f"task {row.get('taskId', '?')}  "
            f"stage {row.get('stageId', '?')}  "
            f"exec {row.get('executorId', '?')}  "
            f"disk={disk}  mem={mem}  "
            f"dur={_fmt_ms(row.get('duration', ''))}"
        )
    if result.executors:
        row = result.executors[0]
        lines.append(
            f"{selected_theme.label}gc hotspot :{R} "
            f"exec {row.get('executorId', '?')}  "
            f"gc={_fmt_ms(row.get('totalGCTime', ''))}  "
            f"dur={_fmt_ms(row.get('totalDuration', ''))}  "
            f"tasks={row.get('totalTasks', '?')}  "
            f"failed={row.get('failedTasks', '?')}"
        )
    if result.top_queries:
        row = result.top_queries[0]
        desc = row.get("description", "")
        if len(desc) > 60:
            desc = desc[:59] + "…"
        lines.append(
            f"{selected_theme.label}slow query :{R} "
            f"id={row.get('executionId', '?')}  "
            f"dur={_fmt_ms(row.get('duration', ''))}  "
            f"{selected_theme.dim}{desc}{R}"
        )

    if len(lines) == 1:
        lines.append(
            f"{selected_theme.dim}(no forensics rows parsed — see raw_output for debugging){R}"
        )
    return "\n".join(lines)


def _cmd_forensics(args) -> int:
    try:
        token = auth.get_fabric_token()
    except auth.AuthError as e:
        _emit_error(str(e))
        return EXIT_AUTH

    api = FabricApi(token)
    try:
        ws_id, item_id = run_resolver.resolve_workspace_item(args.path)
        spec = RunSpec.parse(args.run)
        session = run_resolver.select_run(api, ws_id, item_id, spec)
    except RunNotFound as e:
        _emit_error(str(e))
        return EXIT_FAILED
    except FabricAuthExpired as e:
        _emit_error(str(e))
        return EXIT_AUTH
    except FabricApiError as e:
        _emit_error(str(e))
        return EXIT_TRANSPORT

    livy_id = session.get("livyId")
    state = session.get("state", "Unknown")
    if not livy_id:
        _emit_error(f"Run has no livyId (state={state}) — cannot run forensics.")
        return EXIT_FAILED
    if state not in TERMINAL_STATES:
        _emit_error(
            f"Run is still {state!r}. Job Insight forensics needs a completed "
            "run — wait for termination then retry."
        )
        return EXIT_FAILED

    session_ws_id = args.livy_workspace or ws_id
    session_lh_id = args.livy_lakehouse  # may be None → env/default fallback

    mode = "HC Livy" if args.hc else "Livy /sessions"
    print(
        f"nbmon: forensics livy={livy_id} ({mode}, warming Scala session, "
        "first call may take ~60-120s)",
        file=sys.stderr,
    )
    run_forensics_kwargs: dict = {
        "workspace_id": ws_id,
        "artifact_id": item_id,
        "livy_id": livy_id,
        "job_type": args.job_type,
        "attempt_id": args.attempt_id,
        "session_ws_id": session_ws_id,
        "session_lh_id": session_lh_id,
        "state_store_path": args.state_store_path,
        "startup_timeout": args.startup_timeout,
        "statement_timeout": args.statement_timeout,
        "environment_id": args.environment_id,
        "use_hc": args.hc,
    }
    if args.hc_session_tag:
        run_forensics_kwargs["hc_session_tag"] = args.hc_session_tag
    try:
        result = run_forensics(token, **run_forensics_kwargs)
    except LivyStatementError as e:
        _emit_error(
            f"Job Insight Scala statement failed: {e.ename}: {e.evalue}"
        )
        for line in (e.traceback or [])[:10]:
            print(f"  {line}", file=sys.stderr)
        return EXIT_FAILED
    except FabricAuthExpired as e:
        _emit_error(str(e))
        return EXIT_AUTH
    except LivyError as e:
        _emit_error(str(e))
        return EXIT_TRANSPORT

    selected_theme = _resolve_theme(args)
    print(_render_forensics(result, selected_theme))
    return EXIT_OK


# ---------------------------------------------------------------------------
# profile — Spark Monitoring REST API
# ---------------------------------------------------------------------------


def _fmt_ms_compact(ms: int | float) -> str:
    """Format milliseconds compactly: 900ms, 12.3s, 4.5m, 2.1h."""
    try:
        n = float(ms)
    except (TypeError, ValueError):
        return str(ms)
    if n < 1000:
        return f"{int(n)}ms"
    if n < 60_000:
        return f"{n / 1000.0:.1f}s"
    if n < 3_600_000:
        return f"{n / 60_000.0:.1f}m"
    return f"{n / 3_600_000.0:.1f}h"


def _fmt_stage_line(s: StageSkew) -> str:
    """One-line stage summary: #id  dur  tasks  skew  [spill]  user-code."""
    parts = [
        f"#{s.stage_id}",
        _fmt_ms_compact(s.executor_run_time_ms),
        f"{s.num_tasks}t",
        f"skew={s.skew_ratio:.2f}x" if s.task_time_quantiles else "skew=-",
    ]
    if s.disk_bytes_spilled > 0:
        parts.append(f"spill={_fmt_bytes(str(s.disk_bytes_spilled))}")
    label = s.details or s.name
    return "  ".join(parts) + f"  {label[:72]}"


def _render_profile(result: ProfileResult, t) -> str:
    R = t.reset
    lines: list[str] = [
        f"{t.box}── Spark Profile — {result.num_stages_total} stages, "
        f"{result.num_executors_total} executors ──{R}"
    ]

    if result.top_stages_by_duration:
        lines.append(f"{t.label}top by duration:{R}")
        for s in result.top_stages_by_duration:
            lines.append(f"  {_fmt_stage_line(s)}")
    else:
        lines.append(
            f"{t.dim}(no completed stages with metrics — app may still be running){R}"
        )

    if result.skewed_stages:
        lines.append(f"{t.label}skewed (p95/p50 ≥ threshold):{R}")
        for s in result.skewed_stages:
            q = s.task_time_quantiles
            if len(q) >= 5:
                lines.append(
                    f"  {t.error_label}#{s.stage_id}{R}  "
                    f"p50={_fmt_ms_compact(q[2])} p95={_fmt_ms_compact(q[4])}  "
                    f"ratio={s.skew_ratio:.2f}x  "
                    f"{t.dim}{(s.details or s.name)[:60]}{R}"
                )
    else:
        lines.append(f"{t.dim}skewed: none above threshold{R}")

    if result.spill_stages:
        lines.append(f"{t.label}spill:{R}")
        for s in result.spill_stages:
            lines.append(
                f"  #{s.stage_id}  "
                f"disk={_fmt_bytes(str(s.disk_bytes_spilled))}  "
                f"mem={_fmt_bytes(str(s.memory_bytes_spilled))}  "
                f"{t.dim}{(s.details or s.name)[:60]}{R}"
            )

    if result.executor_hotspots:
        lines.append(f"{t.label}executor GC hotspots:{R}")
        for e in result.executor_hotspots:
            lines.append(
                f"  exec {e.executor_id}  "
                f"gc={_fmt_ms_compact(e.total_gc_time_ms)}  "
                f"dur={_fmt_ms_compact(e.total_duration_ms)}  "
                f"gc%={e.gc_fraction * 100:.1f}  "
                f"tasks={e.total_tasks}  failed={e.failed_tasks}"
            )

    return "\n".join(lines)


def _cmd_profile(args) -> int:
    try:
        token = auth.get_fabric_token()
    except auth.AuthError as e:
        _emit_error(str(e))
        return EXIT_AUTH

    api = FabricApi(token)
    try:
        ws_id, item_id = run_resolver.resolve_workspace_item(args.path)
        spec = RunSpec.parse(args.run)
        session = run_resolver.select_run(api, ws_id, item_id, spec)
    except RunNotFound as e:
        _emit_error(str(e))
        return EXIT_FAILED
    except FabricAuthExpired as e:
        _emit_error(str(e))
        return EXIT_AUTH
    except FabricApiError as e:
        _emit_error(str(e))
        return EXIT_TRANSPORT

    livy_id = session.get("livyId")
    app_id = session.get("sparkApplicationId")
    state = session.get("state", "Unknown")
    if not livy_id or not app_id:
        _emit_error(
            f"Run has no livy/app id (livy={livy_id}, app={app_id}, state={state}) "
            "— profile needs a run that reached Spark-app startup."
        )
        return EXIT_FAILED
    if state not in TERMINAL_STATES:
        # Unlike forensics, profile CAN work on an in-flight run — Spark History
        # Server surfaces stages as they complete. Just warn the user.
        print(
            f"nbmon: profile: run is still {state!r} — showing stages completed so far.",
            file=sys.stderr,
        )

    try:
        result = run_profile(
            api,
            ws_id=ws_id,
            item_id=item_id,
            livy_id=livy_id,
            app_id=app_id,
            top_n=args.top_n,
            skew_threshold=args.skew_threshold,
        )
    except FabricAuthExpired as e:
        _emit_error(str(e))
        return EXIT_AUTH
    except FabricApiError as e:
        _emit_error(str(e))
        return EXIT_TRANSPORT

    selected_theme = _resolve_theme(args)
    print(_render_profile(result, selected_theme))
    return EXIT_OK


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        if args.cmd == "attach":
            return _cmd_attach(args)
        if args.cmd == "submit":
            return _cmd_submit(args)
        if args.cmd == "list":
            return _cmd_list(args)
        if args.cmd == "status":
            return _cmd_status(args)
        if args.cmd == "forensics":
            return _cmd_forensics(args)
        if args.cmd == "profile":
            return _cmd_profile(args)
        parser.error(f"unknown command: {args.cmd}")
        return EXIT_TRANSPORT
    except KeyboardInterrupt:
        return EXIT_SIGINT


if __name__ == "__main__":
    sys.exit(main())
