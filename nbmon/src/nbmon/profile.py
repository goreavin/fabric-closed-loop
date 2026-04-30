"""
Spark run profiling via the Fabric Monitoring REST API.

Consumes `/applications/{appId}/stages`, `/taskSummary`, and `/executors` to
surface:

  - Top-N stages by wall-clock task time (`executorRunTime`)
  - Stages showing task-time skew (p95/p50 ratio of `executorRunTime`)
  - Stages with disk spill
  - Executor GC hotspots

This is the non-JIL replacement for `nbmon forensics`. It runs entirely against
the server-side Spark History Server — no Livy session, no runtime pin, no
`Code.AccessStorage.All` consent. Only needs the same bearer token
`nbmon status` already uses.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from nbmon.fabric_api import FabricApi


@dataclass
class StageSkew:
    """Per-stage skew data computed from taskSummary quantiles."""

    stage_id: int
    attempt_id: int
    name: str
    details: str  # first line of user-code call site, if present
    num_tasks: int
    executor_run_time_ms: int
    disk_bytes_spilled: int
    memory_bytes_spilled: int
    shuffle_read_bytes: int
    shuffle_write_bytes: int
    # Task-level quantiles for executorRunTime (ms): [p05, p25, p50, p75, p95]
    task_time_quantiles: list[float]
    # p95 / p50 — 1.0 means no skew, >2 meaningful, >5 severe
    skew_ratio: float


@dataclass
class ExecutorLoad:
    executor_id: str
    total_tasks: int
    total_duration_ms: int
    total_gc_time_ms: int
    failed_tasks: int
    gc_fraction: float  # totalGCTime / totalDuration — >0.1 is noteworthy


@dataclass
class ProfileResult:
    app_id: str
    top_stages_by_duration: list[StageSkew] = field(default_factory=list)
    skewed_stages: list[StageSkew] = field(default_factory=list)
    spill_stages: list[StageSkew] = field(default_factory=list)
    executor_hotspots: list[ExecutorLoad] = field(default_factory=list)
    # Useful for the caller to know what was sampled vs not.
    num_stages_total: int = 0
    num_executors_total: int = 0


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _first_line(text: Optional[str]) -> str:
    if not text:
        return ""
    for line in text.splitlines():
        s = line.strip()
        if s:
            return s
    return ""


def _stage_skew_from_raw(stage: dict, summary: Optional[dict]) -> StageSkew:
    """Build a StageSkew from a /stages element and an optional taskSummary."""
    quantiles: list[float] = []
    skew = 1.0
    if summary:
        ert = summary.get("executorRunTime") or []
        if isinstance(ert, list) and len(ert) >= 3:
            quantiles = [float(x) for x in ert]
            # With quantiles "0.05,0.25,0.5,0.75,0.95" → indices 0..4
            # Use p50 (index 2) and p95 (index 4).
            p50 = quantiles[2] if len(quantiles) > 2 else 0.0
            p95 = quantiles[4] if len(quantiles) > 4 else quantiles[-1]
            skew = (p95 / p50) if p50 > 0 else 1.0
    return StageSkew(
        stage_id=int(stage.get("stageId", 0)),
        attempt_id=int(stage.get("attemptId", 0)),
        name=(stage.get("name") or "")[:120],
        details=_first_line(stage.get("details"))[:160],
        num_tasks=int(stage.get("numTasks", 0)),
        executor_run_time_ms=int(stage.get("executorRunTime") or 0),
        disk_bytes_spilled=int(stage.get("diskBytesSpilled") or 0),
        memory_bytes_spilled=int(stage.get("memoryBytesSpilled") or 0),
        shuffle_read_bytes=int(stage.get("shuffleReadBytes") or 0),
        shuffle_write_bytes=int(stage.get("shuffleWriteBytes") or 0),
        task_time_quantiles=quantiles,
        skew_ratio=skew,
    )


def _executor_load_from_raw(e: dict) -> ExecutorLoad:
    total_dur = int(e.get("totalDuration") or 0)
    total_gc = int(e.get("totalGCTime") or 0)
    gc_frac = (total_gc / total_dur) if total_dur > 0 else 0.0
    return ExecutorLoad(
        executor_id=str(e.get("id", "?")),
        total_tasks=int(e.get("totalTasks") or 0),
        total_duration_ms=total_dur,
        total_gc_time_ms=total_gc,
        failed_tasks=int(e.get("failedTasks") or 0),
        gc_fraction=gc_frac,
    )


# --------------------------------------------------------------------------
# Public entry point
# --------------------------------------------------------------------------


def run_profile(
    api: FabricApi,
    *,
    ws_id: str,
    item_id: str,
    livy_id: str,
    app_id: str,
    top_n: int = 5,
    skew_threshold: float = 2.0,
) -> ProfileResult:
    """Fetch stages + task summaries + executors and build a ProfileResult.

    Args:
        top_n: How many stages to pull task summaries for, sorted by
               executorRunTime desc. Task-summary fetches are the biggest
               contributor to latency, so we deliberately limit them.
        skew_threshold: `p95/p50` ratio above which a stage is flagged as
                        skewed. 2.0 is a practical signal; MS Spark History
                        Server's Diagnosis tab defaults to 3.0 — pick what
                        matches your noise tolerance.
    """
    raw_stages = api.list_stages(ws_id, item_id, livy_id, app_id)
    # Only COMPLETE stages carry meaningful metrics; skipped stages show up
    # here too but with zero runtime and would pollute "slow stages".
    completed = [
        s for s in raw_stages
        if (s.get("status") or "").upper() in {"COMPLETE", "FAILED"}
    ]
    # Sort by executorRunTime desc — the metric Spark History Server uses for
    # "slow stages" in its Summary view.
    completed.sort(key=lambda s: int(s.get("executorRunTime") or 0), reverse=True)

    top = completed[:top_n]

    # Fetch task summaries for top-N only. Stages with <=1 task cannot skew
    # (nothing to compare) — skip the extra round-trip.
    top_with_summary: list[StageSkew] = []
    for stage in top:
        summary: Optional[dict] = None
        num_tasks = int(stage.get("numTasks") or 0)
        if num_tasks > 1:
            try:
                summary = api.stage_task_summary(
                    ws_id,
                    item_id,
                    livy_id,
                    app_id,
                    int(stage.get("stageId", 0)),
                    int(stage.get("attemptId", 0)),
                )
            except Exception:
                # Task summary is best-effort; a single missing stage shouldn't
                # kill the whole profile run.
                summary = None
        top_with_summary.append(_stage_skew_from_raw(stage, summary))

    skewed = [s for s in top_with_summary if s.skew_ratio >= skew_threshold]
    spill = [
        s for s in top_with_summary
        if s.disk_bytes_spilled > 0 or s.memory_bytes_spilled > 0
    ]
    spill.sort(key=lambda s: s.disk_bytes_spilled, reverse=True)

    raw_execs = api.list_executors(ws_id, item_id, livy_id, app_id)
    # Exclude the driver "executor" — it has totalDuration but no tasks and
    # dominates any GC-fraction sort without giving useful signal.
    execs = [
        _executor_load_from_raw(e)
        for e in raw_execs
        if (e.get("id") or "").lower() != "driver"
    ]
    execs.sort(key=lambda e: e.total_gc_time_ms, reverse=True)

    return ProfileResult(
        app_id=app_id,
        top_stages_by_duration=top_with_summary,
        skewed_stages=skewed,
        spill_stages=spill,
        executor_hotspots=execs[:top_n],
        num_stages_total=len(raw_stages),
        num_executors_total=len(raw_execs),
    )
