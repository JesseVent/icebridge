# icebridge

Two Rust poll loops that turn a [`supalite-replica`](https://github.com/JesseVent/supalite-replica)-backed
SQLite database into an Apache Iceberg-on-S3 analytics lake, with a fast
read-back path into the same operational database — an "LTAP" slice: OLTP
speed (SQLite in-process), analytics scale (Iceberg + DuckDB), no managed
warehouse required.

```
tmpfs SQLite ──litestream──▶ replica ──restore+diff──▶ Bronze (Iceberg)
                                                             │
                                                    duckdb aggregation
                                                             │
                                                             ▼
operational SQLite ◀────sync-back──── Gold (Iceberg, recomputed each cycle)
```

Modeled on Databricks' Bronze/Silver/Gold + Synced Tables pattern (a
reference architecture the project owner supplied) — adapted to a stack with
no managed "synced table" feature, so the reverse-ETL hop is hand-rolled.

## Two binaries

### `icebridge` (Bronze) — `src/main.rs`

Every `ICEBRIDGE_POLL_INTERVAL_MS`:
1. Restores the Litestream replica to a fresh snapshot file (shells out to
   `restore/restore.js`, a thin wrapper around `supalite-replica`'s
   `restoreReplica()` — see [why not reimplement in Rust](#why-shell-out-to-node-for-restore)).
2. Diffs it against the previous poll's snapshot by primary key (`src/diff.rs`)
   — added/changed rows and deleted primary keys.
3. Writes the delta as one Parquet file + a `fast_append` transaction into a
   Bronze Iceberg table (`src/iceberg_sink.rs`), via a REST catalog.

The diff cursor is a stable file (`icebridge-previous.db` in the working
dir), not just in-memory state — restart-safe. A crash between commit and
cursor-advance replays that cycle's rows on next start; safe because the
Bronze log is append-only and idempotent under a latest-`_ingested_at`-per-key
read.

### `gold` (Gold) — `src/bin/gold.rs`

Every `ICEBRIDGE_GOLD_POLL_INTERVAL_MS` (coarser — a slow aggregation query
here must never stall Bronze ingestion, hence the separate binary):
1. Loads Bronze's current Iceberg snapshot via the catalog (not a raw path
   glob — `catalog.load_table()` resolves the *current* metadata pointer).
2. Runs a user-supplied `ICEBRIDGE_GOLD_QUERY` against it via the `duckdb`
   CLI (DuckDB reads Iceberg tables natively and already proved correct
   during Bronze verification — a subprocess call, not a query-engine
   integration).
3. Drops + recreates the Gold Iceberg table with the result.
4. Syncs the same rows into a plain table in the *operational* SQLite
   database (`restore/sync-back.js`, through `@supabase/lite`'s connection
   layer — correct pragmas, same path the app's own writes use) so the app
   reads Gold with normal SQL, never touching Iceberg/DuckDB itself.

The Gold query is responsible for deduping Bronze's append-only log before
grouping — `row_number() OVER (PARTITION BY id ORDER BY _ingested_at DESC)`,
filtered to `rn = 1 AND NOT _deleted`. icebridge doesn't hide this; it's
Bronze's log shape leaking through by design.

## Quickstart (local verification)

Spin up a local Iceberg REST catalog fixture (no cloud creds needed):

```bash
docker run -d --name icebridge-rest -p 8181:8181 \
  -v /tmp/icebridge-warehouse:/tmp/icebridge-warehouse \
  -e CATALOG_WAREHOUSE=/tmp/icebridge-warehouse \
  -e CATALOG_URI=jdbc:sqlite:/tmp/iceberg_catalog.db \
  apache/iceberg-rest-fixture
```

```bash
export ICEBRIDGE_CATALOG_URI=http://localhost:8181
export ICEBRIDGE_CATALOG_WAREHOUSE=/tmp/icebridge-warehouse
export ICEBRIDGE_STORAGE=fs

export ICEBRIDGE_DATABASE_FILE=/path/to/app.db
export ICEBRIDGE_REPLICA_JSON='{"type":"file","path":"/path/to/replica"}'
export ICEBRIDGE_TABLE=items
export ICEBRIDGE_MAX_CYCLES=1   # omit to poll forever

cargo run --release --bin icebridge
```

```bash
export ICEBRIDGE_GOLD_ICEBERG_TABLE=items_gold
export ICEBRIDGE_GOLD_QUERY='SELECT count(*) n FROM bronze WHERE NOT _deleted'
export ICEBRIDGE_SYNC_DATABASE_FILE=/path/to/app.db
export ICEBRIDGE_SYNC_TABLE=items_gold   # must already exist in app.db
export ICEBRIDGE_GOLD_MAX_CYCLES=1

cargo run --release --bin gold
```

Inspect either table with DuckDB directly:

```bash
duckdb -c "INSTALL iceberg; LOAD iceberg;
SELECT * FROM iceberg_scan('/tmp/icebridge-warehouse/default/items/metadata/...');"
```

## Environment variables

| Var | Used by | Notes |
|---|---|---|
| `ICEBRIDGE_CATALOG_URI` | both | required |
| `ICEBRIDGE_CATALOG_WAREHOUSE` | both | optional |
| `ICEBRIDGE_CATALOG_TOKEN` | both | optional |
| `ICEBRIDGE_STORAGE` | both | `fs` for local/dev; anything else (default) = S3 |
| `ICEBRIDGE_S3_ENDPOINT/REGION/ACCESS_KEY_ID/SECRET_ACCESS_KEY` | both | forwarded to storage layer |
| `ICEBRIDGE_DATABASE_FILE` | Bronze | source SQLite file |
| `ICEBRIDGE_REPLICA_JSON` | Bronze | `supalite-replica` replica config JSON |
| `ICEBRIDGE_TABLE` | Bronze | source table name |
| `ICEBRIDGE_NAMESPACE` | Bronze | default `"default"` |
| `ICEBRIDGE_ICEBERG_TABLE` | Bronze | default = `ICEBRIDGE_TABLE` |
| `ICEBRIDGE_POLL_INTERVAL_MS` | Bronze | default `5000` |
| `ICEBRIDGE_MAX_CYCLES` | Bronze | omit to poll forever |
| `ICEBRIDGE_RESTORE_SCRIPT` | Bronze | default `restore/restore.js` |
| `ICEBRIDGE_WORKDIR` | Bronze | default OS temp dir; holds the diff cursor |
| `ICEBRIDGE_GOLD_NAMESPACE` | Gold | default = Bronze namespace |
| `ICEBRIDGE_GOLD_ICEBERG_TABLE` | Gold | required |
| `ICEBRIDGE_GOLD_QUERY` | Gold | required — must dedupe Bronze itself (see above) |
| `ICEBRIDGE_SYNC_DATABASE_FILE` / `ICEBRIDGE_SYNC_TABLE` | Gold | required — destination table must already exist |
| `ICEBRIDGE_SYNC_SCRIPT` | Gold | default `restore/sync-back.js` |
| `ICEBRIDGE_GOLD_POLL_INTERVAL_MS` | Gold | default `30000` |
| `ICEBRIDGE_GOLD_MAX_CYCLES` | Gold | omit to poll forever |
| `ICEBRIDGE_TIMING` | both | set to print per-phase timing to stderr |

## Performance

Benchmarked at 1K → 1M source rows (`/tmp/bench-*` fixtures).

**Bronze's diff** was originally O(table size) regardless of delta size — a
fixed 20-row delta cost 2ms at ~1.1K rows vs. 1001ms at ~1M rows, because
both snapshots were pulled into Rust `HashMap<String, Vec<Value>>`s and
compared there. Fixed by rewriting `diff.rs` to run the comparison as SQL
(`ATTACH DATABASE` the previous snapshot, `LEFT JOIN` by primary key) instead
of fetching into Rust structures:

| rows | old diff_ms | new diff_ms | raw-scan floor |
|---|---|---|---|
| 1,120 | 1 | 1 | 2 |
| 100,000 | 103 | 10 | 8 |
| 1,000,120 | 1,412 | **66** | 60 |

~21x faster at 1M rows, within 10% of the floor. Verified against a
raw-scan-time floor and a poll-interval crossover calculation that true
row-level CDC (a version column + keyed query, or SQLite's session
extension) isn't worth building yet — the SQL-diff fix already recovers
~90% of the available headroom, pushing the "Bronze falls behind its own 5s
poll loop" point from ~3.5M rows to ~65M rows.

**Gold's DuckDB aggregation** stays flat (~60–155ms) from 1K to 1M Bronze
rows — vectorized columnar aggregation over Parquet, driven by output
cardinality, not input size. Write cost (~200–290ms) is dominated by catalog
round-trips (drop/create/commit + sync-back connection open/close).

## Known limitations

- **Deletes are an append-only log, not real Iceberg deletes.**
  `iceberg-rust` 0.10.1's `Transaction` only exposes `fast_append` — no
  delete/overwrite action. Bronze records a removed primary key as a new row
  with `_deleted = true`; downstream reads must pick the latest
  `_ingested_at` per key and filter `_deleted = false`.
- **Gold is drop+recreate per cycle**, not incremental — same missing
  overwrite-action limitation, sidestepped at the `Catalog` level
  (`drop_table` + `create_table`) since Gold is a fully-recomputed rollup
  anyway.
- **Single-column primary key only.**
- **Not tested against real S3 or Supabase's hosted Iceberg REST catalog**
  (project `gpfuhxmtidkynxhmfphg`, the intended production target) — local
  filesystem catalog verification only so far.
- **Sync-back never DDLs** the operational destination table — it must
  already exist, owned by the app's own migration.

### Why shell out to Node for restore?

`supalite-replica`'s `restoreReplica()` is TypeScript, tightly coupled to
`@supabase/lite`'s connection layer and `node:sqlite`. Reimplementing that in
Rust would mean re-deriving Litestream's restore semantics independently —
`restore/restore.js` is a thin JSON-in/JSON-out wrapper instead, the same
pattern `sync-back.js` and the `duckdb` CLI calls follow throughout this
project: shell out to the tool that already gets it right.

## Testing

```bash
cargo test --release
```

Unit tests cover the pure logic — SQL-diff correctness (added/changed/deleted
rows, NULL-vs-NULL handling, tables without a primary key) in `src/diff.rs`,
and schema/type inference + Arrow batch building in `src/iceberg_sink.rs` and
`src/bin/gold.rs`. None of it needs Docker, DuckDB, or a catalog — CI runs
this on every push.

Full pipeline verification (Bronze end-to-end against a real Iceberg REST
catalog, Gold's DuckDB aggregation, correctness at 1M-row scale) is manual —
see [Quickstart](#quickstart-local-verification) — since it needs Docker and
isn't yet exercised against real S3/Supabase infra (see
[Known limitations](#known-limitations)).

## Status

**Early / unreleased.** Proof-of-concept proven against a local Iceberg REST
catalog fixture, not yet against real S3 or Supabase's hosted catalog. Expect
the schema (env vars, table shapes) to change without notice until this has
a tagged release.

## License

[MIT](./LICENSE) © Jesse Vent
