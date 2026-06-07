# DDL Coverage Matrix and Acceptance Contract v1

Issue: PIN-10
Version: 1.0
Applies to: TiDB snapshot via Dumpling followed by TiCDC cloud storage replay into Snowflake.

## Purpose

This contract gives backend and QA stable scenario IDs for DDL support work. A scenario marked `supported` is part of the v1 acceptance bar. A scenario marked `unsupported` must fail explicitly with a deterministic error and no silent data corruption. A scenario marked `deferred` is intentionally outside v1 and must be documented in user-facing limitations before release.

## Test Harness Contract

Each executable scenario should run against one tenant/task pair unless the scenario states otherwise:

- Source schema: `ddl_v1_src`
- Snowflake schema: isolated per run, for example `DDL_V1_<run_id>`
- Base table: `orders`
- Primary key: `id BIGINT PRIMARY KEY`
- Baseline columns: `id BIGINT NOT NULL`, `tenant_id BIGINT NOT NULL`, `status VARCHAR(32)`, `amount DECIMAL(18,2)`, `created_at DATETIME(3)`
- Baseline rows after snapshot: ids `1`, `2`, `3`
- CDC rows after snapshot: use ids `4`, `5` for inserts and id `2` for updates/deletes

For every supported scenario, QA should capture:

- TiDB DDL and DML statements used to generate the fixture.
- Object storage layout containing Dumpling files, TiCDC `schema_*.json` files, DML files, and checkpoints.
- Snowflake DDL executed by tidb2dw, if rewritten.
- Final Snowflake `information_schema.columns` result for the table.
- Final Snowflake row count and representative row values.
- Error text and exit status for unsupported scenarios.

## Global Acceptance Rules

### Ordering

`DDL-V1-ORDER-001` is the ordering contract for all supported scenarios:

1. Dumpling snapshot creates the initial Snowflake table and loads all snapshot rows before incremental replay starts.
2. The initial TiCDC schema file with an empty query initializes the in-memory schema before any non-empty DDL query is executed.
3. For each table version, the schema file is processed before DML files that reference that table version.
4. DML files are merged only against the exact schema version carried by their path.
5. DDL followed by DML in the same TiCDC flush interval must produce the same final Snowflake state as executing the original TiDB statements in commit order.

### Idempotency

`DDL-V1-ORDER-002` is the idempotency contract for all supported scenarios:

1. Replaying a completed DML file is skipped when its checkpoint exists.
2. Replaying a completed DDL schema file is a no-op after the schema file query has been cleared or otherwise checkpointed.
3. Duplicate DDL input that cannot be proven completed must stop with a deterministic duplicate/out-of-order error instead of applying a destructive operation twice.
4. Restarting tidb2dw after any successful DDL must not require manual edits to object storage.

### Comparison Rules

Schema comparison must verify column name, ordinal position, mapped Snowflake type, nullable flag, default where v1 supports it, and primary key metadata where v1 supports it. Snowflake constraint enforcement differences are acceptable only when called out by the scenario.

Data comparison must verify row count, primary-key set, and representative values for every column affected by the DDL. Dropped columns must be absent from schema comparison and must not be referenced by later merge SQL. Renamed columns must preserve historical values.

Error comparison must verify a non-zero task result, a stable error class or prefix, and no partial checkpoint that would make restart skip unresolved work.

## Coverage Matrix

| Scenario ID | Status | TiDB input | Expected Snowflake outcome | Required assertions |
| --- | --- | --- | --- | --- |
| DDL-V1-001 | supported | Initial snapshot of `orders` | Table is created before snapshot load with all baseline columns and primary key metadata. | Schema matches baseline; row count is 3; rows 1-3 match TiDB values. |
| DDL-V1-002 | supported | `CREATE TABLE refunds (id BIGINT PRIMARY KEY, order_id BIGINT, amount DECIMAL(18,2))` after CDC starts, followed by inserts. | A new Snowflake table is created and subsequent DML for `refunds` is loaded. | `refunds` exists; row count and values match TiDB; table task discovery is documented for backend if current single-table execution must change. |
| DDL-V1-003 | supported | `DROP TABLE orders` after pending DML for `orders` has flushed. | Snowflake table is dropped only after earlier DML has been applied. | Earlier DML is visible before drop in audit logs; table no longer exists; later DML for the dropped table fails explicitly. |
| DDL-V1-004 | deferred | `RENAME TABLE orders TO orders_archive` with later writes to the new name. | Deferred because current single-table changefeed/task binding may not capture writes under the new table name. | Runner expects a documented deferred result; no silent creation of a partially synced table. |
| DDL-V1-005 | supported | `ALTER TABLE orders ADD COLUMN coupon_code VARCHAR(64) NULL`, followed by inserts and updates using the column. | Column is added; historical rows contain `NULL`; new values replicate. | Schema includes nullable `COUPON_CODE`; old rows are null; new/updated rows match TiDB. |
| DDL-V1-006 | supported | `ALTER TABLE orders ADD COLUMN source VARCHAR(16) NOT NULL DEFAULT 'web'`. | Column is added with a default acceptable to Snowflake; historical and new rows have deterministic values. | Schema includes `SOURCE`; no load failure for historical rows; default/value comparison matches TiDB. |
| DDL-V1-007 | supported | `ALTER TABLE orders DROP COLUMN status`, followed by updates/inserts. | Column is removed and later merge SQL does not reference it. | `STATUS` absent from schema; row count unchanged except intended DML; later DML succeeds. |
| DDL-V1-008 | supported | `ALTER TABLE orders RENAME COLUMN amount TO gross_amount`, followed by updates. | Column is renamed and historical values are preserved. | `GROSS_AMOUNT` exists; `AMOUNT` absent; row 2 retains pre-rename value until updated; post-rename DML matches TiDB. |
| DDL-V1-009 | supported | Widen type: `ALTER TABLE orders MODIFY COLUMN status VARCHAR(128)`. | Snowflake column type is widened without data loss. | Type/length matches mapping; all status values preserved; longer post-DDL value loads. |
| DDL-V1-010 | supported | Numeric precision widen: `ALTER TABLE orders MODIFY COLUMN amount DECIMAL(20,4)`. | Snowflake numeric precision/scale changes and subsequent values load. | Type precision/scale match; representative decimal values compare exactly. |
| DDL-V1-011 | unsupported | Narrow or incompatible type change that would truncate or fail conversion, for example `VARCHAR(128)` to `VARCHAR(8)` with longer values. | Task stops before loading incompatible DML. | Stable unsupported/incompatible-type error; no checkpoint for unresolved DDL or dependent DML. |
| DDL-V1-012 | supported | `ALTER TABLE orders MODIFY COLUMN status VARCHAR(32) NOT NULL` after all rows are non-null. | Snowflake sets `NOT NULL`. | Precheck or Snowflake execution succeeds; nullable flag is false; DML with null fails or is rejected consistently. |
| DDL-V1-013 | unsupported | `ALTER TABLE orders MODIFY COLUMN status VARCHAR(32) NOT NULL` while existing rows contain null. | Task stops with a clear not-null violation. | Error names the table/column; no partial checkpoint; restart behavior is deterministic. |
| DDL-V1-014 | supported | `ALTER TABLE orders MODIFY COLUMN status VARCHAR(32) NULL`. | Snowflake drops `NOT NULL` and later null values load. | Nullable flag is true; DML setting null succeeds; values match TiDB. |
| DDL-V1-015 | supported | Drop an existing default, for example `ALTER TABLE orders ALTER COLUMN status DROP DEFAULT`. | Snowflake default metadata is removed where present. | Default is absent in schema comparison; row values are unchanged. |
| DDL-V1-016 | deferred | Change default on an existing column, for example `ALTER TABLE orders ALTER COLUMN status SET DEFAULT 'new'`. | Deferred unless backend implements reliable Snowflake default rewrite and comparison. Data replication is unaffected because TiCDC emits row values. | Runner expects documented deferred result; no warning-only success if schema metadata diverges. |
| DDL-V1-017 | supported | Add non-primary secondary index, drop secondary index, or rename secondary index. | No Snowflake physical index is created; data replication continues. | Scenario is treated as metadata no-op with an info log; schema/data comparison passes. |
| DDL-V1-018 | deferred | Add/drop unique key. | Deferred because Snowflake uniqueness constraints are not enforced like TiDB and current merge semantics do not use them. | Runner expects documented deferred result; implementation must not claim uniqueness enforcement. |
| DDL-V1-019 | deferred | Add/drop/change primary key after snapshot. | Deferred because merge keys and deduplication depend on stable primary-key columns. | Runner expects documented deferred result; task must not continue with stale merge keys. |
| DDL-V1-020 | supported | `TRUNCATE TABLE orders`, followed by inserts. | Snowflake table is truncated at the correct table version, then later inserts load. | Rows before truncate are absent; post-truncate rows are present; row count matches TiDB. |
| DDL-V1-021 | supported | Partition maintenance that changes only TiDB partition metadata and emits no row-level deletes, for example add/drop/reorganize empty partition. | Snowflake has no partition metadata change; data remains correct. | Schema unchanged; row counts and values match TiDB; info log records metadata no-op. |
| DDL-V1-022 | deferred | Partition DDL that implies data movement or deletion, for example truncate/drop partition containing rows. | Deferred until CDC output and Snowflake delete semantics are verified per partition action. | Runner expects documented deferred result or an explicit unsupported error; no silent row drift. |
| DDL-V1-023 | supported | DDL interleaved with DML: insert row 4, add column `coupon_code`, update row 4 with coupon, insert row 5. | Final Snowflake state matches TiDB commit order. | Row 4 exists with coupon after update; row 5 includes the new column; no DML uses the wrong schema version. |
| DDL-V1-024 | unsupported | DDL schema file arrives before earlier dependent DML files have flushed. | Task stops rather than applying DDL before dependent DML. | Stable ordering error; unresolved files have no checkpoint; restart can apply in correct order after missing files appear. |
| DDL-V1-025 | unsupported | Duplicate or out-of-order schema version replay with a non-empty DDL query. | Task rejects duplicate/out-of-order DDL unless it can prove the DDL was already checkpointed. | Stable duplicate/out-of-order error; destructive DDL is not executed twice. |
| DDL-V1-026 | unsupported | Column type unsupported by Snowflake mapping, for example `MEDIUMBLOB`, `LONGBLOB`, `JSON`, `ENUM`, `SET`, or generated column requiring materialization. | Task fails before creating divergent Snowflake schema. | Error includes unsupported type and column; no partial table/schema checkpoint. |
| DDL-V1-027 | supported | `DROP TABLE orders`; later `CREATE TABLE orders (...)` with the same name and new table identity. | Snowflake drops the old table and creates the new table without merging old and new rows. | Old rows absent unless reinserted; new schema matches recreated table; logs/checkpoints distinguish table versions. |
| DDL-V1-028 | supported | Multiple tenant/task pairs run DDL on tables with the same unqualified name. | DDL and DML are isolated by tenant/task Snowflake schema, storage prefix, stage, and checkpoint namespace. | Each tenant has only its own rows/schema; checkpoint files do not cross-contaminate. |

## Release Gate

The v1 DDL implementation is releasable only when all `supported` scenarios above have automated evidence and all `unsupported` or `deferred` scenarios produce documented, deterministic behavior. A test report may cite the scenario ID and does not need to restate this contract.
