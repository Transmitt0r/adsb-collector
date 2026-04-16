# Collector

Async Python service that polls the Pi's tar1090 ADS-B endpoint every 5 seconds
and writes aircraft sightings to TimescaleDB.

## Structure

```
collector/
  collector/        ← Python package
    __main__.py     ← entry point (python -m collector)
    config.py       ← env var config
    models.py       ← AircraftState dataclass
    poller.py       ← HTTP polling of aircraft.json
    tracker.py      ← session state machine → DB writes
    db.py           ← asyncpg pool + schema init
    schema.sql      ← TimescaleDB schema
  tests/            ← pytest test suite
  docker-compose.yml ← TimescaleDB + collector (NAS)
  Dockerfile        ← builds ghcr.io/transmitt0r/adsb-collector
  pyproject.toml
  .env.example
```

## Key facts

- Data source: `http://<pi-ip>/data/aircraft.json` (tar1090 Docker container, no /tar1090/ prefix)
- `alt_baro` can be the string `"ground"` — always type-check before using as int
- `flight` callsign has trailing spaces — strip before storing
- `seen` is seconds since last message — compute timestamp as `now - seen`
- asyncpg `pool.acquire()` is a sync context manager
- Schema uses TimescaleDB hypertable for `position_updates` (1-day chunks, compressed after 7 days)

## Dev workflow

```bash
# from repo root
nix develop

# then from collector/
cd collector
pytest
ruff check collector tests
ruff format --check collector tests
mypy collector
python -m collector
```

## Deploy

```bash
cd collector
docker compose up -d
```
