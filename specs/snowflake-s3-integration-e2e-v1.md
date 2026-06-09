# Snowflake/S3 Dumpling+CDC Integration E2E Matrix v1

Issue: PIN-9
Owners: Data / Integration
Depends on:

- `architecture/multi-tenant-task-runtime.md`
- `specs/ddl-coverage-contract-v1.md`
- `specs/multi-tenant-task-contract-v1.md`
- `docs/pin14-e2e-runbook.md`

## Purpose

This spec defines the executable data-flow boundary for real Snowflake and S3 validation. It turns the DDL and multi-tenant contracts into concrete e2e scenarios that can be generated, uploaded, replayed, compared, retried, and cleaned up without touching shared production objects.

## Data Flow Under Test

The e2e path must exercise the same logical stages as production:

1. Generate deterministic TiDB fixture data for one or more tenant/task pairs.
2. Produce Dumpling-style snapshot CSV files, load info, TiCDC schema files, TiCDC DML files, and mapping metadata.
3. Upload fixture files to a scoped S3 root:

```text
<PIN14_S3_STORAGE_URI>/<PIN14_STAGING_PREFIX>/tenants/<tenant_id>/tasks/<task_id>/scenarios/<scenario_id>/
```

4. Prepare only the configured Snowflake database/schema and task-owned tables/stages.
5. Load the snapshot before replaying CDC files.
6. Apply TiCDC schema events and DML in manifest timeline order.
7. Compare Snowflake schema, row count, selected rows, and checksum against the manifest expected outcome.
8. Clean up only task-owned S3 prefixes and Snowflake objects when `PIN14_CLEANUP_MODE` allows it.

Secrets are not part of fixture files, manifests, logs, reports, or issue comments. Use only the environment-variable names from `docs/pin14-e2e-runbook.md`.

## Runnable Paths

### Local deterministic fixture planning

This path has no external Snowflake/S3 side effects.

```bash
make build
./bin/tidb2dw fixturegen --profile smoke --scenarios DDL-V1-001,DDL-V1-002,DDL-V1-005,DDL-V1-028
PIN14_E2E_MODE=validate scripts/pin14-e2e.sh
./bin/tidb2dw pin14-e2e --mode plan --scenarios DDL-V1-001,DDL-V1-002,DDL-V1-005,DDL-V1-028
```

`validate` checks required config names and path safety. `plan` verifies the generated manifest, selected scenarios, S3 object layout, Snowflake namespace names, and tenant/task scenario scopes.

### Real Snowflake/S3 smoke

This path requires approved Snowflake, S3, TiDB, and TiCDC credentials in the operator shell or CI secret store.

```bash
make test
make build
./bin/tidb2dw fixturegen --profile smoke --scenarios DDL-V1-001,DDL-V1-002,DDL-V1-005,DDL-V1-007,DDL-V1-020,DDL-V1-028
PIN14_E2E_MODE=validate scripts/pin14-e2e.sh
PIN14_E2E_MODE=run scripts/pin14-e2e.sh
```

The `pin14-e2e` cobra command has concrete S3/Snowflake clients for `run` mode. It uploads generated fixture files to the scoped S3 prefix, creates scoped Snowflake stages/tables, loads snapshot and CDC files, validates row counts/checksums/samples, and cleans up owned S3/Snowflake objects according to `PIN14_CLEANUP_MODE`.

## Scenario Matrix

| ID | Path | Scenarios | Tenancy | Required evidence |
| --- | --- | --- | --- | --- |
| INT-SF-S3-001 | fixture plan | DDL-V1-001 | single tenant/task | Manifest resolves snapshot, loadinfo, initial schema, DML files, checksum, and Snowflake target table. |
| INT-SF-S3-002 | real smoke | DDL-V1-001 | single tenant/task | Snapshot files land in scoped S3 root; Snowflake table exists; 3+ snapshot rows load before CDC. |
| INT-SF-S3-003 | fixture plan then real smoke when DDL wiring exists | DDL-V1-002 | single tenant/task | New table schema event creates `refunds`; later DML lands in that table only. |
| INT-SF-S3-004 | fixture plan then real smoke when DDL wiring exists | DDL-V1-003, DDL-V1-027 | single tenant/task | Drop table and drop/recreate do not merge old and new table identity; later DML after drop fails or loads only after recreate. |
| INT-SF-S3-005 | fixture plan then real smoke when DDL wiring exists | DDL-V1-005, DDL-V1-006, DDL-V1-007, DDL-V1-008 | single tenant/task | Add/drop/rename column DDL updates Snowflake schema and later merge SQL uses the correct column set. |
| INT-SF-S3-006 | fixture plan then real smoke when DDL wiring exists | DDL-V1-009, DDL-V1-010, DDL-V1-011, DDL-V1-026 | single tenant/task | Type widen succeeds; incompatible or unsupported type stops before divergent data load and leaves no skip checkpoint. |
| INT-SF-S3-007 | fixture plan then real smoke when DDL wiring exists | DDL-V1-012, DDL-V1-013, DDL-V1-014, DDL-V1-015, DDL-V1-016 | single tenant/task | Nullability/default-supported cases match Snowflake metadata; unsupported/deferred cases are deterministic non-success. |
| INT-SF-S3-008 | fixture plan then real smoke when DDL wiring exists | DDL-V1-017, DDL-V1-018, DDL-V1-019, DDL-V1-021, DDL-V1-022 | single tenant/task | Metadata-only DDL is explicit no-op; deferred key/partition cases cannot silently pass as supported. |
| INT-SF-S3-009 | fixture plan then real smoke when DDL wiring exists | DDL-V1-023, DDL-V1-024, DDL-V1-025 | single tenant/task | DDL/DML ordering is enforced; missing earlier DML and duplicate/out-of-order schema files are rejected. |
| INT-SF-S3-010 | fixture plan | DDL-V1-028 plus DDL-V1-001/005 for both default tenants | two tenants, same task/table names | Distinct S3 roots, Snowflake tables/stages, manifest schemas, and checkpoints; no cross-tenant rows or files. |
| INT-SF-S3-011 | real smoke | DDL-V1-001 for two task IDs under one tenant | one tenant, two tasks | Task-scoped S3 roots and Snowflake object names; one task cleanup cannot remove the other task's files/objects. |
| INT-SF-S3-012 | negative validation | invalid/missing env, dirty S3 URI with query, invalid prefix, invalid cleanup mode | any | `validate` fails before external side effects and error text contains config names only, not values. |
| INT-SF-S3-013 | failure recovery | inject Snowflake load/apply failure after S3 upload | single tenant/task | Failed scenario returns non-zero, keeps unresolved files uncheckpointed, and a rerun with same tenant/task resumes inside same scoped root. |
| INT-SF-S3-014 | cleanup boundary | successful and failed run with `always`, `on-success`, `never` | single tenant/task | Cleanup deletes only `<storage>/<prefix>/tenants/<tenant>/tasks/<task>/...` and task-owned Snowflake tables/stages. |

## Completion Standard

The integration/e2e work is complete when:

- `make test` passes with fixture, DDL SQL rewrite, task runtime, and e2e planner tests.
- `fixturegen --profile smoke --scenarios all` generates manifests for every `DDL-V1-*` row.
- `pin14-e2e --mode plan --scenarios all` succeeds against generated fixtures with real-looking but non-secret Snowflake/S3 config values.
- At least INT-SF-S3-001, 002, 010, 011, 012, and 014 have runnable evidence before the first merge.
- Every `supported` DDL row from `specs/ddl-coverage-contract-v1.md` has either real Snowflake/S3 evidence or an explicit implementation gap linked in the final report.
- All `unsupported` and `deferred` rows fail or skip deterministically; none are counted as successful replication.
- The final report contains scenario IDs, tenant/task IDs, row counts, checksums, object counts, cleanup mode, command exit status, and sanitized artifact paths only.

## Current Integration Gaps

- The fixture generator can create Dumpling/CDC files and manifests, and the e2e runner can validate/plan them.
- Real `pin14-e2e --mode run` requires `PIN14_AWS_REGION` and `PIN14_SNOWFLAKE_STORAGE_INTEGRATION` in addition to the Snowflake/S3/TiDB/CDC environment contract.
- The wrapper `scripts/pin14-e2e.sh` runs the legacy `snowflake` command against real Snowflake/S3, but it does not yet replay the full generated DDL scenario manifest.
- Backend wiring must connect task-scoped storage roots, stage names, manifest timeline replay, and DDL checkpointing before the full supported DDL matrix can be promoted from plan evidence to real Snowflake/S3 pass evidence.
