## Architecture

Single deployable service: two async tasks in an `asyncio.TaskGroup`.

1. **Pipeline** (`pipeline.py`): polls tar1090 every 5s, records sightings, batches
   new/expired aircraft for enrichment via Gemini.
2. **Bot** (`bot/app.py`): Telegram bot — `/start`, `/stop`, `/debug`. Digest generation
   is triggered by the scheduler (weekly cron) or `/debug`.

Wiring lives in `__main__.py` — no business logic there.

## Module Layout

```
squawk/
  __main__.py           # wiring only
  config.py             # all env vars, single frozen dataclass
  db.py                 # asyncpg pool creation
  scheduler.py          # APScheduler behind a Protocol
  pipeline.py           # polling loop + enrichment batching
  enrichment.py         # ScoringClient protocol + ADK implementation + enrich_batch()
  digest.py             # DigestClient protocol + ADK implementation + generate_digest()
  charts.py             # matplotlib chart rendering for digests
  clients/              # typed HTTP clients, each behind a Protocol
  repositories/         # write repositories, one per table-owner
  queries/              # read-only cross-table queries (digest, charts)
  bot/                  # Telegram bot, handlers, broadcaster
```

## Key Patterns

- **Protocol-based DI:** every external dependency has a Protocol. Concrete
  implementations are private (`_GeminiScoringClient`, `_GeminiDigestClient`).
  Tests inject fakes.
- **Table ownership:** each repository owns specific tables for writes. Cross-table
  reads go through query objects. See docstrings in `repositories/` for ownership.
- **`libs/tar1090`** is a pure library — no squawk imports allowed (enforced by ruff
  banned-import rules in `libs/ruff.toml`).

## AI Clients

Both `enrichment.py` and `digest.py` use ADK `LlmAgent` with `output_schema` for
structured output. The pattern: create agent, run with `InMemorySessionService`,
parse the final response via Pydantic.
