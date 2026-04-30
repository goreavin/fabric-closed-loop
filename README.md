# Fabric Notebook Closed-Loop Development

An end-to-end process for authoring, deploying, running, monitoring, and
correcting PySpark notebooks on Microsoft Fabric — driven entirely from a
headless agent environment with no portal interaction required.

## Documentation

- **[Full Process Guide](docs/process.md)** — comprehensive walkthrough with
  Mermaid diagrams, phase-by-phase detail, tool decision matrix, and gotchas.
- **[HTML Version](docs/index.html)** — single-page wiki-style rendering of
  the process guide.

## nbmon — Fabric Notebook Monitor

`nbmon` is a Python CLI that bridges the gap between `fab job start` (which
provides no error detail on failure) and the Fabric Spark Monitoring REST API
(which has the actual driver logs with Python tracebacks and Spark Advise
categories).

### Installation

```bash
pip install msal requests
```

### Quick Start

```bash
# Add nbmon to your PATH
export PATH="$PWD/nbmon/bin:$PATH"

# Check recent runs
nbmon list "MyWorkspace.Workspace/My Notebook.Notebook"

# Get status + error banner for the latest run
nbmon status "MyWorkspace.Workspace/My Notebook.Notebook"

# Submit and live-stream driver logs
nbmon submit "MyWorkspace.Workspace/My Notebook.Notebook"

# Attach to an in-flight or completed run
nbmon attach "MyWorkspace.Workspace/My Notebook.Notebook" --run latest
```

### Prerequisites

- Python 3.10+
- `msal` and `requests` packages
- Fabric CLI (`fab`) authenticated via `fab auth login`

### Architecture

See [nbmon Internals](docs/process.md#notebook-monitor-nbmon-internals) in the
process guide for a detailed walkthrough of what each command does under the
hood, including the Spark Monitoring REST API endpoints used.

## License

MIT
