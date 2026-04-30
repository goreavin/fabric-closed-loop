from __future__ import annotations

import shlex
import subprocess
from dataclasses import dataclass
from typing import Callable, Sequence


class RunNotFound(LookupError):
    pass


class FabCliError(RuntimeError):
    pass


@dataclass(frozen=True)
class RunSpec:
    kind: str  # "latest" | "livy" | "jobInstance"
    value: str | None

    @classmethod
    def parse(cls, raw: str) -> "RunSpec":
        if raw == "latest":
            return cls("latest", None)
        if raw.startswith("livy:"):
            return cls("livy", raw[len("livy:") :])
        if raw.startswith("jobInstance:"):
            return cls("jobInstance", raw[len("jobInstance:") :])
        raise ValueError(
            f"--run must be 'latest', 'livy:<UUID>', or 'jobInstance:<UUID>', got: {raw!r}"
        )


def _default_fab_runner(args: Sequence[str]) -> str:
    cmd = ["fab", *args]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise FabCliError(
            f"`{shlex.join(cmd)}` failed (exit {proc.returncode}): {proc.stderr.strip()}"
        )
    return proc.stdout.strip()


def resolve_workspace_item(
    path: str, *, fab_runner: Callable[[Sequence[str]], str] | None = None
) -> tuple[str, str]:
    runner = fab_runner or _default_fab_runner
    if "/" not in path:
        raise ValueError(
            "Notebook path must be of the form '<workspace>.Workspace/<item>.Notebook'"
        )
    workspace_segment = path.split("/", 1)[0]
    if not workspace_segment.endswith(".Workspace"):
        raise ValueError(
            "First segment must end with '.Workspace' (e.g. 'MyWorkspace.Workspace')"
        )
    ws_id = runner(["get", workspace_segment, "-q", "id"])
    item_id = runner(["get", path, "-q", "id"])
    return ws_id, item_id


def select_run(api, ws_id: str, item_id: str, spec: RunSpec) -> dict:
    sessions = api.list_livy_sessions(ws_id, item_id)
    if not sessions:
        raise RunNotFound(f"No livy sessions found for item {item_id}")

    if spec.kind == "latest":
        return max(sessions, key=lambda s: s.get("submittedDateTime", ""))

    key = "livyId" if spec.kind == "livy" else "jobInstanceId"
    matches = [s for s in sessions if s.get(key) == spec.value]
    if not matches:
        raise RunNotFound(
            f"No session matched {spec.kind}:{spec.value} for item {item_id}"
        )
    return matches[0]
