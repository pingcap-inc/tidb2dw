# PIN-9 Target Architecture And Refactor Boundary

Issue: PIN-9
Status: working contract
Related contracts:

- `architecture/multi-tenant-task-runtime.md`
- `architecture/pin9-detailed-design-and-capability.md`
- `specs/multi-tenant-task-contract-v1.md`
- `specs/ddl-coverage-contract-v1.md`

## Objective

Deploy one `tidb2dw` service that can run multiple independent TiDB-to-Snowflake replication tasks for multiple tenants. The service must support Dumpling snapshot plus TiCDC cloud-storage CDC, handle the v1 DDL matrix, isolate tenant/task state, and provide executable unit and e2e evidence before release.

## Target Runtime Shape

Each task is the unit of ownership, concurrency, failure, resume, metrics, and credentials.

```text
tenant
  task
    source: TiDB + TiCDC + source tables
    staging: S3 root scoped by tenant/task
    destination: Snowflake database/schema scoped by tenant/task
    state: lifecycle + checkpoints scoped by tenant/task
```

Default task state layout:

```text
s3://<bucket>/<deployment-prefix>/tenants/<tenant-id>/tasks/<task-id>/
  snapshot/
  increment/
  checkpoints/
```

Legacy single-task CLI mode remains supported, but multi-tenant mode must use the scoped layout by default. Plaintext CLI credentials are only a legacy compatibility surface; multi-tenant task config carries secret references and resolves them inside the runner just before connector construction.

## Required State Machine

`pkg/taskruntime.Controller` is the authoritative lifecycle boundary:

```text
created -> running -> stopped -> running
created -> running -> failed -> running
```

Rules:

- Validate task config and concurrency before opening TiDB, TiCDC, S3, or Snowflake connections.
- One failed task must not stop other tenants or tasks.
- Resume uses the same tenant/task checkpoint namespace and must not scan another task's S3 prefix.
- All task logs and metrics must include `tenant_id`, `task_id`, and `correlation_id`.

## Snowflake/S3 Execution Model

Backend wiring must build execution from `pkg/taskruntime.BuildPlan` and pass the plan into snapshot, CDC, and connector creation.

For each source table:

1. Create task/table-scoped Snowflake stages using `TablePlan.SnapshotStage` and `TablePlan.IncrementStage`.
2. Run Dumpling into `RuntimePlan.SnapshotURI`.
3. Copy snapshot files into the task Snowflake schema.
4. Create TiCDC changefeed with sink URI `RuntimePlan.IncrementURI`.
5. Replay TiCDC schema and DML files in table-version order.
6. Write checkpoints under `RuntimePlan.CheckpointURI`.

Snowflake schema defaults to a tenant/task namespace when not explicitly configured. Even when the schema is explicitly shared, stage names, CDC files, and checkpoints remain task-scoped.

## DDL Event Contract

The DDL source of truth is `specs/ddl-coverage-contract-v1.md`. Implementation must use stable scenario IDs in tests and reports.

Supported in v1:

- Initial table creation from snapshot.
- CDC `CREATE TABLE`, `DROP TABLE`, drop+recreate same table name, and `TRUNCATE TABLE`.
- Add/drop/rename column.
- Widen string and numeric types.
- Set/drop `NOT NULL` when Snowflake execution is valid.
- Drop default.
- Non-unique secondary index and metadata-only partition DDL as no-ops.
- DDL interleaved with DML when table-version order is preserved.
- Multi-tenant/task DDL isolation.

Explicitly unsupported in v1:

- Incompatible or narrowing type changes.
- `NOT NULL` when existing data violates it.
- Duplicate or out-of-order non-checkpointed DDL.
- DML for a dropped table after the drop is applied.
- Unsupported Snowflake type mappings.

Deferred in v1:

- Table rename.
- Changing an existing default.
- Add/drop unique key.
- Add/drop/change primary key.
- Partition DDL that moves or deletes data.

Unsupported or deferred cases must fail with deterministic task-level errors. They must not silently continue, mutate unrelated task state, or write checkpoints that skip unresolved work on restart.

## Refactor Boundaries

### Backend Builder

Owns executable wiring only after this contract:

- Add multi-tenant task config/API/CLI entry that maps to `taskruntime.TaskConfig`.
- Replace global status in cloud mode with per-tenant/task status.
- Thread `RuntimePlan` through Dumpling, TiCDC, snapshot replay, incremental replay, and Snowflake connector setup.
- Change snapshot/increment loadinfo and checkpoints from legacy hard-coded paths to task-scoped paths.
- Preserve legacy CLI behavior by mapping it through `NewLegacySnowflakeTask` or equivalent compatibility adapter.

Do not create external Snowflake/S3 objects before `BuildPlan` and controller concurrency checks succeed.

### Data / Integration

Owns real data-flow evidence:

- Generate TiDB fixtures for every supported, unsupported, and deferred DDL scenario.
- Run Dumpling and TiCDC into real S3 prefixes scoped by tenant/task.
- Run Snowflake verification against isolated schemas.
- Capture object layout, TiCDC schema files, Snowflake DDL, row counts, representative row values, and deterministic errors.

### QA

Owns release evidence:

- Unit tests for `taskruntime` validation, plan naming, lifecycle transitions, concurrency, and failure isolation.
- Unit tests for Snowflake DDL generation by scenario ID.
- E2E report covering create table, drop table, alter columns, multi-tenant, multi-task, failure recovery, unsupported cases, and deferred cases.

### Security / Performance

Owns review gates:

- Confirm no plaintext credentials are logged, returned by APIs, written to reports, or embedded in task config.
- Confirm tenant/task IDs cannot escape S3 prefixes or collide in Snowflake stage/checkpoint names.
- Review concurrency limits, Snowflake warehouse/copy cost, S3 listing scope, and retry behavior.

## Completion Standard

PIN-9 is complete only when all of the following are true:

- Multi-tenant task config validates before external side effects.
- Two tenants can run overlapping source table names without sharing S3 prefixes, Snowflake stages, schemas, checkpoints, logs, or task status.
- One tenant can run multiple tasks concurrently within configured limits.
- Dumpling snapshot plus TiCDC CDC reaches Snowflake for supported DDL scenarios in the v1 matrix.
- Unsupported and deferred DDL scenarios fail or report deterministic documented results.
- Process restart resumes from the same tenant/task checkpoint namespace.
- Legacy single-task CLI behavior still works.
- UT and e2e evidence are attached in the final test report.
