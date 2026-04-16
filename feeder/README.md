# Feeder

Configuration for the Raspberry Pi ADS-B feeder stack.

## Containerised setup (recommended)

The entire feeder stack runs via Docker using `docker-compose.feeder.yml` in the repo root.

### Setup

1. Fill in the placeholders in `docker-compose.feeder.yml`:
   - `YOUR_LAT` / `YOUR_LON` — receiver coordinates
   - `YOUR_FR24_KEY_HERE` — from https://www.flightradar24.com/share-your-data

2. Run on the Pi:
   ```bash
   docker compose -f docker-compose.feeder.yml up -d
   ```

3. tar1090 web UI will be available at `http://flighttracker.local`

### Services

| Service | Image | Role |
|---------|-------|------|
| `readsb` | `sdr-enthusiasts/docker-readsb-protobuf` | ADS-B decoder, talks to RTL-SDR dongle |
| `tar1090` | `sdr-enthusiasts/docker-tar1090` | Web UI + aircraft JSON API |
| `fr24feed` | `sdr-enthusiasts/docker-flightradar24` | FlightRadar24 feeder |

## Legacy config files

The files `readsb`, `tar1090`, and `fr24feed.ini` are the original native config files
from before containerisation, kept for reference.
