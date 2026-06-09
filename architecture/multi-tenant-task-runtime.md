# Multi-Tenant Task Runtime Architecture

Issue: PIN-12
Depends on: `specs/ddl-coverage-contract-v1.md`

## Goal

A single `tidb2dw` deployment can run independent TiDB-to-Snowflake replication tasks for multiple tenants. Tenant credentials, source CDC offsets, object storage state, Snowflake namespaces, logs, metrics, and failure handling are separated by default. Sharing must be explicit in task configuration.

## Configuration Boundaries

Configuration is split into five blocks before any external connection is opened:

- `tenant`: stable tenant ID and display metadata.
- `task`: stable task ID, name, and optional correlation ID.
- `source`: TiDB endpoint, TiCDC endpoint, and source table list.
- `staging`: object storage provider, root URI, and optional deployment prefix.
- `destination`: Snowflake account, database, schema, warehouse, and credential references.

Secret values are not part of the task contract. Task config carries secret references only. CLI flags may continue to accept plaintext credentials for the legacy single-task mode, but multi-tenant config must resolve references through the deployment secret provider before connector creation and must never echo resolved values in logs, API responses, reports, or fixtures.

## Runtime Plan

`pkg/taskruntime.BuildPlan` validates the configuration and returns a pure runtime plan. The plan is safe to create before opening TiDB, S3/GCS, or Snowflake connections.

Default state layout:

```text
<staging-uri>/<prefix>/tenants/<tenant-id>/tasks/<task-id>/
  snapshot/
  increment/
  checkpoints/
```

For Snowflake, a task gets its own target schema unless `destination.snowflake.schema` is explicitly set. Stages are task-scoped and table-scoped:

```text
<TENANT>_<TASK>_SNAPSHOT_<SOURCE_DB>_<SOURCE_TABLE>
<TENANT>_<TASK>_INCREMENT_<SOURCE_DB>_<SOURCE_TABLE>
```

This prevents two tenants with the same source table name from sharing stages, load history, checkpoints, or CDC offsets.

## Lifecycle

`pkg/taskruntime.Controller` defines the local lifecycle state machine:

```text
created -> running -> stopped -> running
created -> running -> failed -> running
```

Transitions:

- `create`: validates and registers the task plan.
- `start`: starts a created task if tenant/global concurrency allows it.
- `stop`: requests a running task to stop after current safe unit of work.
- `resume`: restarts a stopped or failed task using the same task ID and checkpoint namespace.
- `fail`: marks a task failed with a stable class, sanitized message, and retryability flag.

The controller is intentionally independent of TiDB/Snowflake connectors so invalid config and concurrency rejection happen before external side effects.

## Failure Isolation

Failures are scoped to the task that owns the failing connector or checkpoint.

- Snowflake write failure: mark only that task as `failed`; other running tasks continue.
- S3/GCS read/write failure: fail only tasks using the affected staging root.
- Invalid config: reject before creating stages, changefeeds, dump files, or Snowflake objects.
- Duplicate or out-of-order DDL: fail the affected task according to `DDL-V1-ORDER-002`.
- Process restart: resume reads `<tenant-id>/<task-id>` checkpoint state only.

The v1 retry boundary is one task. Retries must not replay a different task's DDL, DML, snapshot loadinfo, or checkpoint marker.

## Logging And Metrics

Every task log and metric carries:

- `tenant_id`
- `task_id`
- `correlation_id`

Metric labels use `tenant` and `task`. Logs and metrics must not include plaintext passwords, AWS keys, Snowflake private connection strings, session tokens, or credential-bearing object storage query strings.

## Migration Path

Existing CLI usage remains a legacy single-task mode. `pkg/taskruntime.NewLegacySnowflakeTask` maps the current `snowflake` command shape to:

- tenant ID: `default`
- task ID: `default`
- legacy object storage layout: unchanged `<storage>/snapshot` and `<storage>/increment`
- existing Snowflake schema: unchanged

Operators can migrate by assigning real tenant/task IDs and removing `UseLegacyLayout`. The first migrated run writes to the task-scoped storage prefix and Snowflake schema, so it should be treated as a new replication task unless checkpoint files are copied deliberately.

## Open Wiring Work

The package and contract are ready for command/service wiring:

- Add a config file or API endpoint that accepts the split task config.
- Resolve secret references into connector configs inside the task runner, not during validation.
- Replace global API status with per-tenant/task status responses.
- Add metric labels to existing counters/gauges.
- Thread task-scoped snapshot/increment URIs and stage names through the Snowflake connector creation path.
