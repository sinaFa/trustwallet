# Crypto price ETL

A Go service that polls Binance's public 24-hour ticker every 30 seconds for the
assets in Trust Wallet's registry, stores each response and the price snapshots
derived from it in Postgres, and appends both layers to JSON Lines files under
`data/`, which stand in for a data lake. Every stage is logged to `logs/etl.log`, and
health and Prometheus metrics are on port 8080. At capture Binance traded 62 of the
registry's 292 symbols; 56 of those are priced, 6 are excluded because the exchange's
pair is a different project than the registry's token, and the gap is measured on
every poll.

## Requirement checklist

| Requirement | Where |
| --- | --- |
| Extract from an open REST API | `internal/source/binance`, one GET per poll, no credentials |
| Go package fetching every 30 seconds | `internal/pipeline`, `ETL_POLL_INTERVAL` defaults to `30s` |
| Store into Postgres | `internal/store/postgres`, tables `raw_fetches`, `price_snapshots`, `assets` |
| Go function exporting processed data to local storage | `internal/export`, also runnable alone with `etl -export-only` |
| Raw data under `data/raw/`, transformed under `data/processed/` | `data/raw/ticker_24hr/dt=YYYY-MM-DD/ticker_24hr.jsonl` and `data/raw/exchange_info/dt=YYYY-MM-DD/exchange_info.jsonl`, `data/processed/price_snapshots/dt=YYYY-MM-DD/price_snapshots.jsonl` |
| Append, never overwrite | JSON Lines, opened `O_APPEND`, fsync per batch, checkpointed |
| Remove unnecessary fields, normalise, consistent timestamps | `internal/transform`, RFC 3339 UTC everywhere |
| Logging: API success/failure, transform errors, data saved | `logs/etl.log`, JSON lines via `log/slog`, see "Logging" |
| Health and Prometheus metrics endpoints | `GET /health`, `/healthz`, `/readyz`, `/metrics` |
| Dockerfile and volume mounts | `Dockerfile`, `docker-compose.yml`, see "Running it" |
| README with productionisation | this file |

## Running it

You need Docker with Compose v2. Nothing else, and no API keys.

```sh
git clone https://github.com/sinaFa/crypto-price-etl.git && cd crypto-price-etl
docker compose up --build -d
curl localhost:8080/readyz          # 200 a few seconds after the first poll
tail -f logs/etl.log
wc -l data/raw/*/dt=*/*.jsonl data/processed/price_snapshots/dt=*/*.jsonl
curl -s localhost:8080/metrics | grep ^etl_
docker compose exec postgres psql -U etl -d etl -c "select symbol, price, fetched_at from price_snapshots where symbol='BTC' order by fetched_at desc limit 5"
docker compose down                 # containers stop, files and the Postgres volume stay
```

`./data` and `./logs` are bind-mounted into the container, so both survive restarts
and are readable on the host; Postgres lives in the named volume `pgdata`. The image
runs as a non-root user (uid 65532): on Linux hosts create `data/` and `logs/` first
and make them writable by that uid. If 5432 or 8080 are taken on your machine,
`POSTGRES_PORT=5433 ETL_PORT=8081 docker compose up -d`. To run the image without
compose, mount the same two directories and point it at a Postgres:

```sh
docker build -t crypto-etl .
docker run --rm -p 8080:8080 -v "$PWD/data:/app/data" -v "$PWD/logs:/app/logs" \
  -e DATABASE_URL='postgres://etl:etl@host.docker.internal:5432/etl?sslmode=disable' crypto-etl
```

If `/readyz` stays at 503, `logs/etl.log` says why. Status 451 from Binance means it
refuses your network's jurisdiction; run from another network. A database error means
a response could not be stored: that run fails and the next poll fetches again, and
nothing reaches the files without its Postgres row first.

For local development you need Go 1.27 and a Postgres, for example the compose one:

```sh
docker compose up -d postgres
make once              # one full cycle, then exit
make run               # poll forever, Ctrl-C to stop
make check             # gofmt, vet, unit tests with the race detector
make test-integration  # store tests against the compose Postgres
```

Configuration is environment only, and every value has a working default:

| Variable | Default | Meaning |
| --- | --- | --- |
| `ETL_POLL_INTERVAL` | `30s` | time between polls |
| `ETL_API_URL` | `https://api.binance.com` | source base URL |
| `ETL_QUOTE_ASSET` | `USDT` | quote asset the registry's pairs trade against |
| `ETL_API_TIMEOUT` | `4s` | per-attempt HTTP timeout |
| `ETL_MAX_ATTEMPTS` | `2` | attempts per request; two requests of timeouts plus worst-case backoff must stay under the interval |
| `ETL_REGISTRY_PATH` | `reference/trustwallet_assets.csv` | asset dimension, loaded at start |
| `ETL_SYMBOL_MAP_PATH` | `reference/symbol_map.csv` | reviewed symbol to asset id map; a symbol it does not bind is never priced |
| `ETL_DATA_DIR` | `data` | root of the file exports |
| `ETL_LOG_PATH` | `logs/etl.log` | log file, also mirrored to stdout |
| `ETL_HTTP_ADDR` | `:8080` | health and metrics listener |
| `ETL_READINESS_MAX_AGE` | 3 x interval | readiness fails when the last success is older |
| `ETL_DRAIN_TIMEOUT` | `20s` | how long a run in flight may finish after a shutdown signal |
| `DATABASE_URL` | compose dev database | pgx connection string |

Four flags. `-once` runs a single cycle and exits. `-export-only` appends anything not
yet exported and exits. `-replay-after A -replay-to B` rewrites the snapshots of every
stored ticker response with id in `(A, B]` from the raw layer, one transaction per
response, so a transform fix corrects wrong rows rather than only adding missing ones;
`-replay-to 0` means to the end, and a replay is safe to repeat. The files cannot
express that deletion: rewritten rows export again with a newer `loaded_at`, and a
retracted row stays in the files until they are rebuilt. Every writing mode takes a
session lock in Postgres at start and exits with "another writer holds the lock" while
the service runs. To rebuild lost files from Postgres, stop the service, reset the
checkpoints, and export alone:

```sh
docker compose stop etl
docker compose exec postgres psql -U etl -d etl -c "delete from export_checkpoints"
docker compose run --rm etl -export-only
docker compose start etl
```

## How it works

```
                 every 30 s
  Binance   --GET-->  source client  -->  raw_fetches (Postgres)  -->  data/raw/...jsonl
  24h ticker          retry, timeout           |
                                               v
                                          transform  -->  price_snapshots (Postgres)  -->  data/processed/...jsonl
                                     bind to asset id,          ^
                                     validate, flag stale       | asset_id foreign key
                                     (reference/symbol_map.csv) |
                                                          assets (Postgres, from reference/trustwallet_assets.csv)

  /healthz  /readyz  /metrics  <--  observe (slog to logs/etl.log + stdout, Prometheus registry)
```

One cycle, each step logged:

1. Fetch. On the first poll, and again after Binance reports an unknown pair, the
   service asks `exchangeInfo` which registry symbols trade against the quote asset
   and keeps the ones the symbol map binds to an asset; a discovery that fails or
   finds nothing backs off, doubling from one interval to ten minutes. Every poll then
   makes one GET for those pairs' tickers, with bounded retries. Each HTTP request
   lands in `raw_fetches` as its own row, success or failure. A failed request ends
   the run there.
2. Raw. The response goes into `raw_fetches` as JSONB before any interpretation, so a
   transform bug can never lose data. This table is the replay point.
3. Transform. Tickers are bound to registry asset ids through the symbol map and
   validated; pairs that stopped updating are flagged. Bad records, and repeats of a
   pair inside one response, are dropped, logged and counted; they don't fail the run.
4. Load. Snapshots are inserted on their stable key, so re-applying a raw fetch
   changes nothing.
5. Export. New rows in both tables are appended to the day's JSONL files, then the
   checkpoint advances.

Runs never overlap: the loop is sequential, and each cycle runs under a deadline of
one poll interval, so a hung request is cut and recorded as a failure rather than
delaying the next tick. The schedule is fixed-rate: a run that overran gets one
immediate catch-up tick and the 30-second grid does not shift. On SIGTERM the run in
flight is allowed to finish, for up to `ETL_DRAIN_TIMEOUT`, so a fetched response
always reaches the raw table and a database write is never cut in half; the compose
file's stop grace period matches.

```
cmd/etl/                   main: wiring, flags, signals, graceful shutdown
internal/config/           environment parsing and validation
internal/registry/         asset dimension and symbol map loaders
internal/source/binance/   HTTP client, pair resolution, retry policy, fixtures
internal/transform/        tickers to snapshots, validation, staleness
internal/store/postgres/   migrations, inserts, export reads, checkpoints, writer lock
internal/export/           partitioned JSONL appender with checkpoints
internal/observe/          logger, metrics, health server
internal/pipeline/         the cycle and the loop
migrations/                versioned SQL, embedded, each applied once under a lock
reference/                 the registry snapshot, the symbol map and their provenance
```

## Data model

The contract the pipeline commits to:

| Field | Value |
| --- | --- |
| Source authority | Binance `GET /api/v3/ticker/24hr?type=MINI` for the registry's pairs, public, documented, keyless; the pair list comes from `GET /api/v3/exchangeInfo` |
| Source scope | registry symbols with a trading spot pair against the quote asset, `USDT` by default, and a verified row in `reference/symbol_map.csv`; the rest of the registry is the measured coverage gap |
| Output | Postgres `raw_fetches`, `price_snapshots`, `assets`; JSONL under `data/raw` and `data/processed` |
| Grain | `price_snapshots`: one row per symbol per poll. `raw_fetches`: one row per HTTP request, `kind` says which of the two, success or failure. `assets`: one row per asset id |
| Stable keys | `(raw_fetch_id, symbol)`; `raw_fetches.id`; `asset_id` |
| Event time | `source_time`, when Binance last updated the pair (its `closeTime`), UTC |
| Snapshot time | `fetched_at`, when the response arrived, UTC |
| Processing time | `loaded_at` |
| Freshness | a successful run within 90 s, or `/readyz` returns 503 |
| Mutation semantics | raw: append. snapshots: append at the key; a replay rewrites the rows of the raw fetches in its range. assets: upsert at start |
| Load behaviour | `INSERT`, `INSERT ... ON CONFLICT DO NOTHING`, `INSERT ... ON CONFLICT DO UPDATE`; replay `DELETE` plus `INSERT` per raw fetch in one transaction |
| Backfill scope | `-replay-after A -replay-to B` rewrites snapshots from `raw_fetches` for that id range; files rebuild by resetting checkpoints with the service stopped |
| Consumers | whoever reads Postgres or the files, joining `assets` on `asset_id`; no downstream job in this repo |
| Security | no source credentials; database URL from the environment; non-root distroless image |
| Success evidence | rows in both tables, lines in both files, `etl_last_success_timestamp_seconds` advancing |
| Recovery | restart the container; the key, the checkpoint and the writer lock make that safe |

`assets` is the dimension: one row per Trust Wallet asset id, 293 ids and 292
distinct symbols because `YLD` appears twice, from Ethereum's token list plus BTC,
ETH, BNB and SOL. It is upserted from the CSV at every start, and an asset that
disappears from the snapshot is stamped `removed_at` rather than deleted, so
historical prices still join. That is soft removal, not a slowly changing dimension;
valid-time history is the production design.

`raw_fetches` is the audit log: one row per HTTP request, success or failure, with
its endpoint, the request as sent, status, duration, attempts, an `outcome`, and for
failures the error text plus the JSON error body when Binance sent one. `kind` tells
the `exchangeInfo` call (`pairs`, body trimmed to symbol, status, base and quote asset
per pair, about 100 KB of the 2.5 MB Binance sends) from the ticker call (`tickers`,
body kept whole). The body is JSONB, semantic JSON rather than wire bytes. Rows are
never updated or deleted, and every snapshot points back through `raw_fetch_id`. That
id is the lineage.

`price_snapshots` is the fact table, one row per symbol per poll, keyed on
`(raw_fetch_id, symbol)`:

| Column | Meaning |
| --- | --- |
| `raw_fetch_id` | which response this came from |
| `asset_id` | the registry asset this price belongs to, set by the symbol map; the join key to `assets` |
| `symbol` | upper-cased Binance base asset ticker |
| `quote_currency` | the quote asset, `USDT` |
| `price` | the pair's last trade price, as sent, as NUMERIC |
| `source_time` | when Binance last updated the pair, UTC; the event clock |
| `last_trade_id` | id of the pair's most recent trade |
| `fetched_at` | when the response arrived, UTC |
| `loaded_at` | when the row was written |

The MINI ticker also carries a rolling 24-hour window: open, high, low, volume, open
time, first trade id, trade count. The snapshot keeps none of it. OHLC for any window
comes from the snapshots themselves, volume would be a second fact table if ever
needed, and the full ticker stays in the raw row.

Why this grain: a wide row per poll falls apart when the basket changes, and
change-only rows drop the polls that saw nothing new, which is exactly the evidence a
staleness question needs. One row per asset per poll keeps every observation and
grows predictably, and every other shape is a query on top; a change-only view is one
row per `(symbol, last_trade_id)`. A second quote asset is a new value in
`quote_currency`; a second source is a `source` column.

Prices arrive as decimal strings with eight places. The transform validates each as
an exact rational and stores it as sent in a NUMERIC column; there is no float
anywhere in the path. Three clocks, one job each: `source_time` orders prices and
tells a live pair from a halted one, `fetched_at` drives freshness and the file
partitions, `loaded_at` is when the row was written. Quiet pairs repeat their price
between polls and `last_trade_id` tells a repeat from a new trade; a pair whose
`source_time` stops moving counts in `etl_assets_stale` after an hour.

Asset identity comes from a reviewed map, not from the ticker symbol. Binance
identifies assets by symbol only, while the registry holds chain, contract and
decimals, and of the 62 registry symbols Binance traded at capture, six name a
different project on the exchange: `GAS` is Neo's gas token, not Gas DAO; `ID` is
SPACE ID, not Everest; `JUP` is Jupiter on Solana, not the Ethereum token of that
name; `LAYER` is Solayer, not Unilayer; `LUNA` is Terra 2.0, not the wrapped classic
token; `MEME` is Memeland's coin, not the 2020 token. `reference/symbol_map.csv` has
a row per traded symbol that binds it to an asset id or excludes it with the reason,
and the service refuses to start if a row names an asset the registry lacks. An
excluded symbol is counted in `etl_symbols_excluded`; a symbol the map has no row for
warns at discovery and counts in `etl_symbols_unverified`, so a new listing waits for
a review and costs one CSV row. Neither is ever priced.

Coverage is measured on every run: 56 priced and 236 missing at capture, logged once
as a baseline. After that, only a symbol priced in the previous run and missing now
warns and increments `etl_coverage_drops_total`; the baseline is restored from
Postgres at startup, so a drop across a restart still warns. The stable gap is a
property of the source; a sudden drop from 56 to 40 is the incident, and the alert
sits on that.

The files are JSON Lines partitioned by the UTC date of `fetched_at`: raw rows by
kind under `data/raw/ticker_24hr/` and `data/raw/exchange_info/`, snapshots under
`data/processed/price_snapshots/`. Raw lines carry the stored body, the full ticker
response or the trimmed discovery body; processed lines carry the snapshot plus
`snapshot_id`; numerics are decimal strings in both. Export is at-least-once: write,
fsync, then advance the checkpoint, so a crash between the two replays that batch,
and readers dedupe raw lines on `raw_fetch_id` and processed lines on
`(raw_fetch_id, symbol)` keeping the latest `loaded_at`. A crash mid-write leaves a
partial last line, and the next export truncates back to the last complete record
before appending. One run reads at most 50,000 rows per dataset in 5,000-row chunks
and says when it hit the cap. Only one process writes at a time: the session lock is
checked before every cycle, and a Postgres restart, which takes the lock with the
session, makes the service exit so its restart takes the lock again.

## Logging

`logs/etl.log` is JSON, one event per line with a UTC timestamp, mirrored to stdout.
Every line carries `run_id` and, once known, `raw_fetch_id`, so a file row, a Postgres
row and a log line can be tied to each other.

| Event | Level | Fields |
| --- | --- | --- |
| `api request succeeded` | INFO | kind, status, attempts, duration_ms, returned, bytes |
| `api request retrying` | WARN | kind, attempt, wait, error |
| `api request failed` | ERROR | kind, status, attempts, retryable, code, error |
| `trading pairs resolved` | INFO | pairs, excluded, unverified, symbols |
| `registry symbols the source trades that the symbol map has no row for` | WARN | count, symbols |
| `pair discovery backed off after a failure or an empty result` | WARN | not_before |
| `transformation error` | WARN | symbol, field, reason, one line per dropped record |
| `source coverage baseline` / `dropped` | INFO / WARN | counts of priced and missing symbols |
| `price movement since previous run` | INFO | changed, unchanged |
| `data saved successfully` | INFO | inserted, skipped |
| `data exported successfully` | INFO | dataset, rows, files, checkpoint |
| `export stopped at its per-run cap` | WARN | dataset, rows |
| `run completed` / `run failed` | INFO / ERROR | elapsed, error |
| `replay completed` | INFO | raw_fetches, deleted, inserted, errors |

Log rotation is left to the platform.

## Metrics and health

`GET /healthz` (and `/health`) answers 200 while the process runs. `GET /readyz`
answers 200 only when Postgres responds and the last fully successful run is younger
than `ETL_READINESS_MAX_AGE`; before the first success it answers 503, which keeps a
broken deploy out of rotation. `GET /metrics` is Prometheus exposition. Each pipeline
metric answers one operational question:

| Metric | What it answers |
| --- | --- |
| `etl_last_success_timestamp_seconds{stage}` | the freshness alert; `time() - value > 90` means three polls failed, separately for fetch, load and export |
| `etl_runs_total{outcome}` | success rate of whole cycles |
| `etl_api_requests_total{kind,outcome}`, `etl_api_retries_total{kind}` | source instability before it becomes an outage |
| `etl_api_request_duration_seconds{kind}` | a slowing source, ahead of timeouts |
| `etl_run_overruns_total` | runs longer than the interval, the precursor to lag |
| `etl_assets_requested`, `etl_assets_priced`, `etl_assets_missing` | per-poll coverage over the registry; a jump in missing is a source change |
| `etl_coverage_drops_total` | symbols priced last run and gone now; the one to alert on |
| `etl_assets_stale` | priced pairs with no source update for over an hour; a halted pair still returns a price |
| `etl_prices_changed_last_run` | whether polls carry new information; zero across many runs means the feed stopped moving |
| `etl_price_jumps_total` | a price moved more than 10x between polls; flagged and loaded, never dropped |
| `etl_transform_errors_total` | the source started sending malformed records |
| `etl_rows_written_total{table,op}` | rows landing; `op="skipped"` rising means replays |
| `etl_export_rows_total{dataset}`, `etl_export_capped_total{dataset}` | the files are being written, and whether a run stopped at the cap with a backlog |
| `etl_registry_ambiguous_symbols` | the registry repeats a symbol; the map decides which asset it is |
| `etl_symbols_excluded`, `etl_symbols_unverified` | symbols the map excluded (steady by design) and symbols it has no row for (a new listing waiting for review) |

`alerts.yml` ships three rules: staleness on `etl_last_success_timestamp_seconds`,
which also fires when the gauge is absent because the service never succeeded;
`increase(etl_coverage_drops_total[10m]) > 0`; and `etl_symbols_unverified > 0` at
warn level, since that one is actionable by a person, not a restart.

## Failure handling

The client classifies failures before retrying. Network errors, timeouts, 408, 418,
429, 500, 502, 503 and 504 are transient and retried with exponential backoff and full
jitter; a `Retry-After` header sets the wait, and when it asks for longer than one
poll can wait the client sends nothing until that time has passed, because Binance
bans IPs that keep calling after a 429 and a 418 is that ban. Everything else fails
at once: remaining 4xx, malformed bodies, the permanent 5xx codes. A 400 with Binance's
code -1121 (unknown pair) also makes the next poll rebuild the pair list. Bodies are
capped at 8 MB, error bodies kept up to 4 KB for the raw layer, and the config refuses
a timeout and attempt combination whose worst case for the two requests of a poll
does not fit inside the interval.

The transform never kills a run. A price that is not a positive decimal, a pair with
no trade to price from, or a `closeTime` before Binance existed or more than a minute
after the fetch (a source that switched to seconds would land in 1970) drops that one
record with a logged reason; a pair halted for months sits inside that bound and
loads flagged stale. Writes are idempotent wherever the data allows it: assets
upsert, snapshots insert on a unique key with conflicts skipped, and the export
checkpoint only advances after the file is durable.

## Tests

```sh
make test               # 63 unit tests, no external services
make test-integration   # 9 tests against the compose Postgres
```

Unit tests use `httptest` servers and in-memory fakes, one named test per failure
mode: retries and give-ups by status class, `Retry-After` and the ban pause, malformed
and oversized bodies, unknown pairs and discovery backoff, bad and duplicate records,
stale pairs, the crash between file write and checkpoint
(`TestExportCrashBetweenWriteAndCheckpointReplaysBatch`), the partial last line
(`TestExportRepairsTruncatedTail`), shutdown mid-run
(`TestShutdownDrainsTheRunInFlight`), the range rewrite
(`TestReplayRewritesARangeOfRawFetches`) and the lost writer lock. The integration
tests prove against a real Postgres what a fake cannot: migrations applied once and
refused when a file changed, the constraints and both foreign keys, idempotent replay
and the rewrite, NUMERIC round trips, and the writer lock, including its loss when
the holder's session is killed (`TestWriterLockAdmitsOneWriter`).

## Design notes

The source. The APIs suggested in the brief are placeholder datasets or need a key.
For a wallet, price data is the natural choice, and Binance's public spot API provides
it without a key: documented, every requested pair in one call, and each ticker
carries the pair's last update time and last trade id, which gives the model a source
clock next to its own. A ticker request for up to 100 pairs weighs 40 of the 6,000
request weight allowed per minute per IP, so polling every 30 seconds spends 80, and
pair discovery adds 20 when it runs.

The dimension. Trust Wallet's assets registry describes what the product actually
lists, so it is the asset dimension next to the price facts. It is a checked-in
snapshot with a stated date (`reference/README.md`), not a second polled source: the
registry changes through pull requests a few times a week. Ethereum's list plus the
four native coins is one chain, enough to prove the shape; BNB Smart Chain is the same
CSV format with more rows, and `assets` already carries `chain`.

No transformation framework and no orchestrator: dbt would add a second toolchain for
transformations that fit in one Go package and a few SQL constraints, and a ticker in
the process is the smallest thing that satisfies "every 30 seconds". Both earn their
place with a DAG, backfills and more than one job; that is the production section.

Postgres before files. The database provides transactions, a monotonic id to
checkpoint on, and constraints; the files are derived from it and can be rebuilt.
JSON Lines over CSV because the raw layer is nested, the processed layer needs decimal
strings, and a record per line survives partial writes.

Versioned migrations. Each SQL file applies once under an advisory lock, so two
replicas starting together cannot race, and its checksum is recorded; a file that
changes after it ran stops the service at startup. A database whose tables exist but
were never recorded is adopted on the next start.

## Production

How I'd run the same design on AWS, and what changes when the volume is real.

The stack: ECS Fargate for the service, RDS Postgres Multi-AZ on day one, Kinesis for
the raw stream from the second step on, S3 with Delta tables read and written by
Databricks, Databricks Workflows for batch jobs, CloudWatch for metrics, logs and
alarms, Secrets Manager for credentials, Terraform for all of it. Kubernetes, Kafka,
Airflow or Loki slot into the same places; nothing below depends on the vendor.

Two steps. Day one is this service as it stands, one Fargate task per source since
polling is serial per source, writing to RDS; nothing in the model changes. The second
step moves the raw sink to Kinesis and a Bronze Delta table, triggered by sources that
push instead of being polled, or more polled sources than one container per source
can hold. The retry, timeout and coverage logic carries over; only the sink moves.

Storage and modelling. `data/raw` becomes a Bronze Delta table on S3, partitioned by
fetch date, with the response body kept as wire bytes, stricter than the local JSONB.
`data/processed` becomes a Silver table with the same grain and key, Z-ordered on
`(symbol, fetched_at)`. The registry becomes a type 2 dimension with `valid_from` and
`valid_to`, so a price observed in March joins to the asset as the registry described
it in March. The symbol map becomes a Silver table with an owner, `verified_on` and
`verified_by`, plus a test that fails when the exchange lists a symbol it does not
know. Staging views do renames and casts; Gold marts start with latest price per
asset, hourly and daily OHLC, and daily coverage per source, each with a declared
grain and tests for key uniqueness, positive prices and freshness, and contracts that
freeze column names and types for the BI layer. Small files are the storage hazard at
a 30-second cadence, handled by buffering in the stream or auto-compaction.

Orchestration and reliability. Workflows schedules Silver and Gold on partition
arrival; backfills are partition-scoped rewrites of Silver from Bronze, never appends,
which is why Bronze keeps the body unmodified and why the local replay is a rewrite.
Production adds a dead-letter path for responses that fail the transform entirely.
Recovery stays the same: restart the container; the key, the checkpoint and the lock
make that safe.

Observability. The CloudWatch agent scrapes `/metrics`; the three rules become the
first alarms. Logs land with `run_id` and `raw_fetch_id` as indexed fields. Data
quality checks at the Silver boundary (row count against `etl_assets_priced`, key
uniqueness, freshness) fail the job before Gold publishes, and a daily reconciliation
compares the raw stream count with the Silver row count.

Scale and cost. One poll is about 16 KB and 56 rows, about 160k rows a day; Postgres
carries that for years. What grows is sources and assets per source, and both scale
horizontally: one container per source, a partition per source in Bronze, a stateless
transform. The money goes to small-file churn and re-reading Bronze, handled by
compaction and partition pruning; the raw layer moves to cheaper tiers after a
retention window.

Security. Distroless non-root image, no secrets in the repo or the image, the only
credential is the database URL and the compose one is a development default. IAM
roles per service, least privilege on bucket prefixes and encryption at rest go in on
day one; a source with terms of use or personal data gets a per-source client policy
and a classification tag that follows the data downstream.

CI. `make check`, the integration tests against a Postgres service container and the
image build run on hosted runners. The smoke run of `-once` against the real endpoint
needs a self-hosted runner in a region Binance serves; hosted runners sit in US
regions, which get the 451.

## Limitations

- Prices are quoted in USDT, a dollar stablecoin, not in dollars; a USD price would
  need a USDT/USD rate from another source.
- Asset identity is a hand-reviewed map. Six symbols are excluded today, and a new
  listing is not priced until someone adds its row.
- At capture 230 of 292 registry symbols had no USDT pair on Binance, and the six
  exclusions bring the priced set to 56. The pipeline measures the gap; it can't close
  it.
- The pair list is resolved on the first poll and after an unknown pair, so a pair
  listed mid-run is picked up at the next restart.
- Exports are at-least-once, readers dedupe on the business key, and the files cannot
  express a deletion: a row a rewrite retracted stays until the files are rebuilt.
- One export run reads at most 50,000 rows per dataset; at 56 rows per poll a backlog
  never forms.
- The rate-limit pause lives in memory. A restart during a 418 ban calls once more at
  startup and the `Retry-After` on that response re-establishes the pause.
- Log rotation and metrics scraping belong to the platform. The registry snapshot is
  refreshed by hand; the date is in `reference/README.md`.

Claude Fable was used throughout this assignment; the design decisions are mine.
