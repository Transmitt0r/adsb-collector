@README.md

## Development Environment

This project uses a **Nix flake** for reproducible dev environments.

```bash
nix develop
```

This provides Python 3.13 with all project dependencies, plus `ruff` and `mypy`. Do NOT use `pip install` globally — add new dependencies to both `pyproject.toml` and `flake.nix`.

## Project Goals

1. **Feeder config** — keep `feeder/` in sync with the Raspberry Pi (`tracker@flighttracker.local`)
2. **Collector** — poll `http://flighttracker.local/tar1090/data/aircraft.json` every 5s and write to TimescaleDB

## Key Facts

- Pi SSH: `tracker@flighttracker.local`
- Primary data endpoint: `http://flighttracker.local/tar1090/data/aircraft.json`
- `alt_baro` can be the string `"ground"` — always type-check before using as int
- `flight` callsign has trailing spaces — strip before storing
- `seen` is seconds since last message — compute observation timestamp as `now - seen`
- The collector uses asyncpg (not SQLAlchemy); pool.acquire() is a sync context manager

## Quality Gates

Before committing:
```bash
ruff check collector tests
ruff format --check collector tests
mypy collector
pytest
```
