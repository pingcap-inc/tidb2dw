# Multi-Tenant Task Contract v1

Issue: PIN-12

## Acceptance Contract

Each executable task has exactly one tenant owner and one task ID. A task may include multiple source tables. A tenant may run multiple tasks. Two tenants may use the same source database/table names without sharing destination state unless `AllowSharedState` is explicitly enabled.

## Required Validation

Validation must complete before opening external connections or creating external objects:

- tenant and task IDs are stable ASCII identifiers.
- source tables are unique within a task.
- target table names are unique within a task.
- staging URI is an absolute object storage URI matching the configured provider.
- staging prefix cannot contain path traversal.
- Snowflake database and credential references are present.
- TiDB endpoint and credential references are present.
- concurrency values are non-negative.

## Isolation Matrix

| Surface | Default owner | Sharing rule |
| --- | --- | --- |
| Dumpling snapshot files | tenant/task storage root | Never shared unless `AllowSharedState` is enabled and reviewed. |
| TiCDC increment files | tenant/task storage root | Never shared unless source changefeed is explicitly shared. |
| Checkpoints/loadinfo | tenant/task storage root | Never shared. |
| Snowflake schema | tenant/task destination namespace | May be explicitly configured, but stage names remain task-scoped. |
| Snowflake external stages | tenant/task/table | Never shared. |
| API lifecycle state | tenant/task | Never shared. |
| Logs/metrics | tenant/task/correlation ID | Labels shared only as dimensions, not state. |

## Failure Scenarios

| Scenario | Expected behavior |
| --- | --- |
| One task fails Snowflake writes while another task runs | Failed task moves to `failed`; other task remains `running`. |
| Two tenants use overlapping table names | Runtime plans produce distinct storage roots, Snowflake schemas, stages, and checkpoint prefixes. |
| Invalid tenant/task config | Validation returns an error before any external side effect. |
| Resume after process restart | Runner uses the same tenant/task checkpoint namespace and correlation ID. |

## Legacy Compatibility

Legacy single-task CLI mode maps to tenant `default` and task `default` with `UseLegacyLayout=true`. This preserves existing `<storage>/snapshot` and `<storage>/increment` paths. Multi-tenant deployments must use task-scoped layout by default.
