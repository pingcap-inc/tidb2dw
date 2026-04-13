# Iceberg Storage Consumer Design

## Context

`tidb2dw` currently consumes TiCDC CSV cloud-storage output and applies it to downstream warehouses. The new requirement is to consume TiCDC Iceberg storage sink output and write the same changes into the existing downstream targets:

- Snowflake
- Redshift
- BigQuery
- Databricks

The TiCDC worktree at `~/go/src/github.com/pingcap/ticdc/.git/wtm/feat/storage-sink-iceberg` already provides the Iceberg source-side capabilities needed for this work:

- scan Iceberg tables and metadata versions
- load one table version from Iceberg metadata
- decode newly added Parquet data files into row changes

This feature is not about making `tidb2dw` produce Iceberg. It is about making `tidb2dw` consume Iceberg storage sink output.

## Goals

- Add an Iceberg source mode to `tidb2dw`.
- Support `full`, `snapshot-only`, and `incremental-only` when the source is Iceberg storage sink output.
- Support the four existing warehouse targets in Iceberg mode.
- Preserve the current CSV-based pipeline while introducing a new row-oriented pipeline for Iceberg.
- Reuse TiCDC Iceberg reader logic directly instead of reimplementing Iceberg metadata and Parquet decoding.
- Migrate existing legacy storage-consumer imports to `ticdc` where the `ticdc` package layout provides a compatible replacement.

## Non-Goals

- Do not make `tidb2dw` create Iceberg tables or write Iceberg metadata.
- Do not add support for arbitrary third-party Iceberg tables. Only TiCDC Iceberg storage sink layout is in scope.
- Do not remove the existing CSV storage sink path in this change.
- Do not redesign all existing warehouse SQL generation beyond what is required for row-based apply.

## User-Facing Model

`tidb2dw` gains a new source mode:

- `--source-format=csv` keeps the current behavior and remains the default.
- `--source-format=iceberg` switches the program to Iceberg consumption.

Iceberg mode introduces source-specific flags:

- `--iceberg.source-uri`: the exact TiCDC Iceberg sink URI, including the catalog and warehouse parameters needed by TiCDC Iceberg config parsing.
- `--iceberg.poll-interval`: polling interval for new metadata versions in long-running modes.

Existing `--storage` remains required, but its meaning changes in Iceberg mode:

- it is the writable staging and checkpoint workspace used by `tidb2dw` while applying row batches to downstream warehouses
- it is not the upstream TiCDC sink location

In Iceberg mode, the current `tidb.*`, `cdc.*`, `snapshot-concurrency`, `cdc.flush-interval`, and `cdc.file-size` flags are ignored because TiDB snapshot export and TiCDC changefeed creation are not part of the runtime path.

## High-Level Architecture

The codebase will support two parallel ingestion paths:

1. CSV storage sink path
2. Iceberg storage sink path

The existing CSV path keeps the current connector contract. Iceberg mode introduces a row-oriented apply path with new abstractions.

### New Abstractions

Add a new row-based downstream interface, separate from the existing file-based connector:

```go
type RowChange struct {
    Op         string
    CommitTs   uint64
    CommitTime string
    Columns    map[string]*string
}

type RowApplier interface {
    CreateTableFromDefinition(tableDef cloudstorage.TableDefinition) error
    InitSchema(columns []cloudstorage.TableCol) error
    ExecDDL(tableDef cloudstorage.TableDefinition) error
    ApplyRows(tableDef cloudstorage.TableDefinition, rows []RowChange) error
    Close()
}
```

The existing connector interface remains in place for the CSV path. Iceberg mode depends only on `RowApplier`.

### New Pipeline

Iceberg mode uses the following flow:

1. Parse Iceberg source config with `github.com/pingcap/ticdc/pkg/sink/iceberg`.
2. Discover source tables and latest metadata versions.
3. For each table, replay metadata versions in order.
4. For each metadata version:
   - build DDL events from previous and current table versions
   - apply DDL first
   - decode each newly added Parquet file into row changes
   - batch rows and apply them through the warehouse-specific `RowApplier`
5. Persist checkpoints after DDL and after each data file.

Per-table ordering is strict. Cross-table concurrency is allowed.

## Source-Side Design

Create an Iceberg source package responsible for:

- source configuration parsing
- table discovery
- metadata version iteration
- conversion from Iceberg schema to `cloudstorage.TableDefinition`
- conversion from decoded Parquet rows to `RowChange`
- checkpoint load/store

This package should use TiCDC directly for:

- `ListHadoopTables`
- `LoadTableVersion`
- `DecodeParquetFile`

Internal Iceberg tables under `__ticdc` are ignored, matching TiCDC storage-consumer behavior.

### Table Definition Mapping

The existing `cloudstorage.TableDefinition` remains the schema carrier used by downstream SQL generation. Iceberg columns are mapped into `cloudstorage.TableCol` using TiCDC’s existing reader metadata and original column metadata embedded in Iceberg column docs.

For the first metadata version of a table, `CreateTableFromDefinition` is used instead of `CopyTableSchema`, because there is no TiDB source table lookup in Iceberg mode.

### DDL Semantics

DDL is generated per metadata version by comparing the previous and current Iceberg table versions.

Comparison rules:

- same column ID, different name: rename column
- same column ID, same name, different type/nullability/PK semantics: modify column
- column ID removed: drop column
- new column ID added: add column
- first observed version: create schema and create table

DDL always executes before the Parquet data files belonging to the same metadata version.

## Runtime Modes

### snapshot-only

Replay metadata versions from `1` to the latest version visible at startup, then exit.

### full

Replay metadata versions from `1` to the latest version visible at startup, then continue polling for newer versions.

### incremental-only

If no checkpoint exists, initialize the table state at the latest version visible at startup and consume only future versions. If the downstream table does not exist, create it from the latest schema definition but do not backfill historical rows. If a checkpoint exists, resume from the next unprocessed unit.

This defines “snapshot” for Iceberg mode as historical metadata-version replay, not current-table snapshot loading.

## Checkpointing

The existing CSV `.checkpoint` model is file-oriented and not sufficient for Iceberg version replay. Iceberg mode adds a new checkpoint record per table stored under the writable `--storage` workspace.

Checkpoint fields:

- source identifier hash
- schema name
- table name
- last metadata version
- whether DDL for that version has been applied
- last applied data file path within that metadata version

Checkpoint rules:

- after DDL succeeds, persist `ddl_applied=true`
- after each data file succeeds, persist that file path
- after a metadata version is fully consumed, persist the completed version and clear the file cursor

This allows restart safety inside a metadata version without assuming whole-version idempotency.

## Downstream Apply Strategy

Iceberg mode is row-oriented at the pipeline boundary, but downstream implementations are allowed to use warehouse-specific bulk loading internally.

### Shared Principles

- `ApplyRows` accepts row changes for one table and one schema version.
- Rows are applied in commit order inside each batch.
- A warehouse implementation may materialize an internal staging artifact in `--storage` to preserve bulk efficiency.
- Downstream DML semantics must remain:
  - `I` inserts a row
  - `U` upserts the latest row by handle key
  - `D` deletes by handle key

### Snowflake

- Create a warehouse-specific temporary staging file from the row batch.
- Reuse the current merge pattern where possible.
- Add `CreateTableFromDefinition` for schema creation without TiDB access.

### Redshift

- Create a temporary staging file and load it into a temporary table.
- Reuse delete-and-insert merge logic.
- Add `CreateTableFromDefinition`.

### BigQuery

- Materialize a temporary staged file under GCS-backed `--storage`.
- Load it into a temporary BigQuery table, then reuse BigQuery `MERGE`.
- Add `CreateTableFromDefinition`.

### Databricks

- Materialize a temporary staged file and expose it through an external table or equivalent temporary relation.
- Reuse Databricks `MERGE`.
- Add `CreateTableFromDefinition`.

The key architectural decision is that Iceberg mode no longer depends on source file paths as the external contract. The external contract becomes row batches.

## Dependency Migration

The feature should add `github.com/pingcap/ticdc` as a direct dependency and use a local `replace` during development:

```go
replace github.com/pingcap/ticdc => ~/go/src/github.com/pingcap/ticdc/.git/wtm/feat/storage-sink-iceberg
```

Where compatible paths exist, migrate current legacy imports to `ticdc`, especially:

- `pkg/sink/cloudstorage`
- `pkg/config`
- `pkg/util`
- `pkg/logger`
- changefeed API model and client packages

Migration is required only where the `ticdc` package layout provides a compatible replacement without destabilizing the existing CSV path. Iceberg support itself must not be blocked by unrelated import churn.

## File and Package Direction

The implementation should introduce focused packages instead of placing all new logic into existing large files.

Expected additions:

- a new Iceberg source package for scan/replay/checkpoint logic
- a new row model and `RowApplier` interface in core interfaces
- new per-warehouse row-apply implementations
- command wiring changes to select CSV or Iceberg source mode

Expected modifications:

- `cmd/core.go`
- warehouse command files in `cmd/`
- connector packages under `pkg/*sql`
- metrics and API status reporting

## Error Handling

- Fail fast on unsupported Iceberg catalog types. Hadoop and Glue are in scope; REST catalog is out of scope.
- Fail fast when downstream primary-key requirements cannot be derived from Iceberg schema metadata.
- Surface per-table fatal errors in the existing API service.
- Preserve current process behavior where one table failure marks that table failed and prevents false success reporting.

## Metrics and Status

Iceberg mode should integrate with existing `/info` and `/metrics` reporting.

Additional Iceberg-specific metrics should include:

- last applied metadata version per table
- number of decoded Parquet files
- number of row batches applied
- per-table checkpoint lag in metadata versions

API table stages remain:

- `loading_snapshot`
- `loading_incremental`
- `finished`

For Iceberg mode:

- historical replay runs under `loading_snapshot`
- polling future versions runs under `loading_incremental`

## Testing Strategy

This change must follow TDD.

Primary tests:

- unit tests for Iceberg version diff to DDL mapping
- unit tests for Iceberg row decoding to `RowChange`
- unit tests for checkpoint resume logic
- unit tests for `CreateTableFromDefinition` in each warehouse package
- unit tests for `ApplyRows` batching semantics in each warehouse package

Integration tests:

- replay a TiCDC Iceberg storage fixture into each warehouse-specific row applier using mocks or warehouse-local abstractions
- mode coverage for `snapshot-only`, `full`, and `incremental-only`
- restart/resume coverage inside a partially consumed metadata version

Compatibility tests:

- existing CSV path tests must still pass
- Iceberg mode must not change behavior for existing commands when `--source-format` is omitted

## Risks

- The new `ticdc` dependency may introduce module conflicts or package-path incompatibilities with the old legacy dependency chain.
- BigQuery remains dependent on GCS-backed writable staging, so source and staging locations must stay separate.
- Warehouse-specific row apply can become slow if implemented as per-row SQL instead of bulk apply.
- Schema evolution semantics must stay aligned with TiCDC Iceberg reader behavior, or replay can drift from source truth.

## Recommendation

Proceed with a staged implementation:

1. add shared row abstractions and Iceberg source package
2. make one warehouse implement `RowApplier` end to end
3. generalize the pattern to the other three warehouses
4. wire command-line selection and mode handling
5. finish checkpointing, metrics, and compatibility cleanup

This keeps the existing CSV path intact while establishing a durable source/applier split for future formats.
