# Squawk

A self-hosted system for historizing ADS-B flight data from a FlightRadar24 feeder station, with a weekly Telegram digest.

## Components

| Component | Location | Purpose |
|-----------|----------|---------|
| Squawk | `squawk/` | Polls Pi every 5s, writes sightings to TimescaleDB, weekly Telegram digest |
| Feeder | `feeder/` | readsb + tar1090 + fr24feed on the Pi |

## Infrastructure

- **Pi:** `tracker@flighttracker.local` — runs the feeder stack
- **NAS / server:** `coolify.local` — runs squawk via Coolify (auto-deploys from master)
- **Data endpoint:** `http://<pi-ip>/data/aircraft.json`
- **Database:** TimescaleDB (shared between squawk and feeder)

## Data Source

Squawk polls the Pi's tar1090 endpoint:

```
http://<pi-ip>/data/aircraft.json
```

Key fields per aircraft:

| Field | Description |
|-------|-------------|
| `hex` | ICAO 24-bit address — stable aircraft identifier |
| `flight` | Callsign |
| `alt_baro` | Barometric altitude (feet), or `"ground"` |
| `gs` | Ground speed (knots) |
| `lat`, `lon` | Position |
| `r_dst` | Distance from receiver (nautical miles) |
| `rssi` | Signal strength (dBFS) |
| `seen` | Seconds since last message received |

## Database Schema

Six tables in TimescaleDB:

- **`aircraft`** — registry, one row per unique ICAO hex
- **`sightings`** — one row per continuous observation session (start/end time, altitude/distance aggregates, callsign)
- **`position_updates`** — high-frequency position samples (hypertable, 1-day chunks, compressed after 7 days, retained 90 days)
- **`enriched_aircraft`** — AI scores, annotations, registration data per aircraft (TTL-based expiry)
- **`callsign_routes`** — origin/destination per flight callsign
- **`digests`** — cached weekly digests

## Deployment

Deployed via Coolify on `coolify.local`. Pushes to `master` auto-deploy.

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ADSB_URL` | — | tar1090 aircraft.json URL |
| `DATABASE_URL` | — | PostgreSQL connection string |
| `BOT_TOKEN` | — | Telegram bot token from @BotFather |
| `GEMINI_API_KEY` | — | Google Gemini API key |
| `ADMIN_CHAT_ID` | — | Telegram chat ID allowed to use `/debug` |
| `POLL_INTERVAL` | `5` | Seconds between polls |
| `SESSION_TIMEOUT` | `300` | Seconds of silence before a sighting session ends |
| `DIGEST_SCHEDULE` | `0 8 * * 0` | Cron schedule for weekly digest |
| `ENRICHMENT_TTL_DAYS` | `30` | Days before re-enriching a known aircraft |
| `ENRICHMENT_BATCH_SIZE` | `20` | Max aircraft per Gemini scoring call |
| `ENRICHMENT_FLUSH_SECS` | `30` | Max seconds to wait before flushing batch |
| `CLIENT_MAX_RETRIES` | `3` | Max retries for 429/5xx from external APIs |

## Dev Environment

```bash
nix develop   # provides Python 3.13, uv, ruff, mypy, psql
```

Single `pyproject.toml` and `uv.lock` at repo root. Run tools from repo root:

```bash
uv run pytest
uv run ruff check .
uv run mypy squawk
```
