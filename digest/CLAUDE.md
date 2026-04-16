# Digest

Weekly flight digest agent that reads from TimescaleDB, uses Google ADK with
Claude Haiku to write an engaging German-language digest, and delivers it via
Telegram bot.

## Structure

```
digest/
  app/
    __main__.py     ← entry point (python -m app)
    config.py       ← env var config
    db.py           ← user registration + digest cache (own Postgres)
    tools.py        ← ADK tools: get_sightings, lookup_aircraft
    agent.py        ← ADK agent (LiteLlm → Claude Haiku) + runner
    bot.py          ← Telegram handlers: /start, /stop, /debug
    scheduler.py    ← weekly cron via APScheduler
  docker-compose.yml ← Postgres (bot state) + digest agent (NAS)
  Dockerfile
  pyproject.toml
  .env.example
```

## Key facts

- Uses Google ADK with `LiteLlm(model="anthropic/claude-haiku-4-5-20251001")`
- Two databases: own Postgres (users + digest cache) and TimescaleDB (flight data, read-only)
- Weekly digest is cached — tokens spent once, sent to all users from cache
- `/debug` is admin-only, gated by `ADMIN_CHAT_ID` env var
- ADK tools (`get_sightings`, `lookup_aircraft`) are sync functions using psycopg2
- Digest is written in German

## Commands

| Command  | Access | Description |
|----------|--------|-------------|
| `/start` | all    | Register for weekly digest |
| `/stop`  | all    | Unregister |
| `/debug` | admin  | Generate and send fresh digest immediately |

## Deploy

```bash
cd digest
docker compose up -d
# Send /start to bot → check logs for chat_id → set ADMIN_CHAT_ID → restart
```
