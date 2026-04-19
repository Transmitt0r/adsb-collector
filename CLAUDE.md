## Repo Structure

Two components:

| Component | Location | Runs on | Purpose |
|-----------|----------|---------|---------|
| Squawk | `squawk/` | NAS (Coolify) | Polls Pi, writes to TimescaleDB, weekly Telegram digest |
| Feeder | `feeder/` | Pi (Coolify) | readsb + tar1090 + fr24feed in Docker |

## Dev Environment

Nix devshell provides Python 3.13, uv, ruff, mypy, psql, pre-commit:

```bash
nix develop          # from repo root
pre-commit install   # once, sets up git hooks
```

Single `pyproject.toml` and `uv.lock` at repo root. Run tools from repo root:

```bash
uv run pytest
uv run ruff check .
uv run mypy squawk
```

## Pre-commit hooks

`.pre-commit-config.yaml` runs on every `git commit`:
- **ruff format** — auto-formats staged `.py` files
- **ruff check --fix** — lints and auto-fixes staged `.py` files
- **pytest (squawk)** — runs test suite when `squawk/` or `libs/` files change
- **pytest (db)** — runs db tests when `db/` files change

The Claude Code PostToolUse hook (`.claude/hooks/ruff-check.sh`) also runs ruff check immediately after each file edit, for faster feedback during development.

**IMPORTANT:** `ruff` and other hook tools are only available inside the nix devshell. Always run `git commit` via `nix develop --command git commit ...` — running it outside the devshell will fail the hooks because the executables are not on PATH.

## Testing

Tests are colocated with the code they test — no top-level `tests/` directory.
Examples: `libs/tar1090/test_tar1090.py`, `squawk/test_pipeline.py`.

## AI / LLM

All AI and LLM calls go through **google-adk** (`google-adk` package). Do not use
`google-genai` or any other AI SDK directly. Production code in `squawk/` must go
through google-adk.

## Infrastructure

- **Pi:** `tracker@flighttracker.local` — runs the feeder stack
- **NAS / server:** `coolify.local` — runs squawk via Coolify (auto-deploys from master)
- **Data endpoint:** `http://<pi-ip>/data/aircraft.json`
- **TimescaleDB:** shared between squawk and feeder

## Database Migrations

Managed by dbmate. Migration files in `db/migrations/`. The entrypoint (`entrypoint.sh`)
runs `dbmate up` on startup.
