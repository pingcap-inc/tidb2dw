# PIN-14 QA Test Plan and Final Report Template

Issue: PIN-9
Scope: TiDB Dumpling + TiCDC replication to Snowflake through S3, with DDL handling, multiple tenants, multiple tasks, and failure recovery.

## Test Gates

### Gate 1: Unit And Component Tests

Run with the project Makefile so the TiDB link flags are applied:

```bash
make test
```

Required coverage:

| Area | Required cases | Evidence |
| --- | --- | --- |
| Snowflake DDL rewrite | create table, drop table, truncate table, add/drop/rename column, widen varchar, widen decimal, nullable to not-null, not-null to nullable, supported metadata no-op | `pkg/snowsql/ddl_test.go` passes |
| Unsupported DDL | narrow type, incompatible type, unsupported TiDB type, default change, table rename, PK change, unique key change, destructive partition DDL | deterministic error assertion in `pkg/snowsql/ddl_test.go` |
| Merge SQL generation | identifier quoting, renamed/dropped columns not referenced by later merge SQL, PK join quoting | `pkg/snowsql/ddl_test.go` and follow-up merge tests |
| Tenant/task config validation | invalid tenant/task IDs, duplicate source/target tables, invalid staging URI, path traversal, missing credential refs, negative concurrency | `pkg/taskruntime/config_test.go` |
| Tenant/task isolation | distinct S3 roots, snapshot/increment/checkpoint prefixes, Snowflake schemas, stage names, logs and metrics labels | `pkg/taskruntime/config_test.go` |
| Task lifecycle | create/start/stop/resume/fail, per-tenant concurrency limit, total concurrency limit, one failed task does not stop unrelated running task | `pkg/taskruntime/lifecycle_test.go` |
| E2E runner behavior | config validation without external side effects, secret values not emitted in errors, per-tenant plan isolation, cleanup behavior, one tenant failure does not mask another tenant result | `pkg/pin14e2e/runner_test.go` |
| Fixture generation | every DDL scenario ID has generated TiDB statements, expected schema, expected rows/checksum, tenant/task manifest | `pkg/testfixtures/*_test.go` |

Gate 1 passes only when `make test` exits 0.

### P0 Verification Addendum

This section must be updated whenever PM asks for a P0-only verification pass. It separates local evidence from real external e2e evidence.

| P0 item | Current result | Evidence | Remaining action |
| --- | --- | --- | --- |
| SQL has no embedded S3 secrets | Code review passed | `pkg/snowsql/sql_test.go` asserts generated external stage SQL uses `STORAGE_INTEGRATION` and does not contain AWS key/token literals. Security/Perf final P0 review passed. `make test` passed on 2026-06-08. | None for code review; still requires real e2e evidence before release. |
| S3 URI has no embedded credentials | Code review passed | `cmd/core_test.go` asserts `getS3URIWithCredentials` strips credential query parameters and moves credentials to AWS env for SDK use. Security/Perf final P0 review passed. `make test` passed on 2026-06-08. | None for code review; still requires real e2e evidence before release. |
| External API auth and redaction | Code review passed | `cmd/core_test.go` covers local vs non-local API address classification. `pkg/apiservice/api_info_test.go` covers credential-bearing error redaction. Security/Perf final P0 review accepted localhost as the default boundary and verified token middleware for external `/info` and `/metrics`. | None for code review; keep raw logs and API evidence sanitized in real e2e report. |
| Effective `RuntimePlan` isolation | Code review passed; real e2e blocked | Security/Perf final P0 review confirmed non-legacy uses task-scoped checkpoint root, DML checkpoints under `checkpoints/increment`, DDL checkpoints under `checkpoints/ddl`, and `--legacy-layout` passes nil checkpoint URI to preserve legacy sidecar checkpoint behavior. `cmd/snowflake_test.go` and `replicate/increment_test.go` cover these paths. | Real Snowflake/S3 tenant/task isolation e2e remains blocked until the requester provides required `AWS_*`, `PIN14_*`, TiDB, CDC, and Snowflake environment variables. |
| P1 blockers | Still blocked until real e2e passes | Code review P0 is passed, but real Snowflake/S3 tenant/task isolation evidence is not available. Issue metadata says waiting on human-provided real environment for Data e2e. | Do not unlock P1 concurrency, CDC hardening, or destructive DDL controls until the real e2e run passes. |

### Gate 2: Real Snowflake/S3 E2E Validation

Run only with approved real credentials from the operator shell, CI secrets, or an approved secret store. Do not commit or paste secret values.

```bash
make build
PIN14_E2E_MODE=validate scripts/pin14-e2e.sh
PIN14_E2E_MODE=run scripts/pin14-e2e.sh
```

Required scenario set:

| Scenario group | Scenario IDs | Required evidence |
| --- | --- | --- |
| Baseline snapshot | `DDL-V1-001` | Snowflake table schema, row count, representative rows, S3 snapshot object count |
| Table lifecycle | `DDL-V1-002`, `DDL-V1-003`, `DDL-V1-020`, `DDL-V1-027` | create/drop/truncate/recreate ordering, final table existence, old rows absent where required |
| Column evolution | `DDL-V1-005` through `DDL-V1-015` | final `information_schema.columns`, row count, affected-row values, Snowflake DDL log |
| Metadata no-op | `DDL-V1-017`, `DDL-V1-021` | schema/data unchanged, explicit no-op log |
| Ordering/idempotency | `DDL-V1-023`, `DDL-V1-024`, `DDL-V1-025` | correct commit-order outcome or stable unsupported error, no invalid checkpoint |
| Unsupported/deferred behavior | `DDL-V1-004`, `DDL-V1-011`, `DDL-V1-013`, `DDL-V1-016`, `DDL-V1-018`, `DDL-V1-019`, `DDL-V1-022`, `DDL-V1-026` | non-zero task result or documented deferred result; no silent success |
| Multi-tenant/multi-task | `DDL-V1-028` plus at least two tenant/task identities | isolated S3 roots, Snowflake schemas/stages, checkpoints, row values, and task status |
| Failure recovery | injected Snowflake load failure, S3 upload failure, DDL-then-DML failure, process restart after successful DDL | failed task status, unaffected peer task status, cleanup scope, deterministic resume behavior |

Gate 2 passes only when:

- `validate` completes before any external side effect.
- `run` exits 0 for all supported scenarios.
- Unsupported/deferred scenarios fail or report exactly as documented.
- Cleanup touches only the generated tenant/task S3 prefix and test-created Snowflake objects.
- Sanitization scan passes before any artifact is attached.

## Final Test Report Template

```text
PIN-14 Final QA Report

Build under test:
- Repo/branch:
- Commit:
- Binary version:
- Test date:
- QA owner:

Environment:
- Run label:
- Snowflake region/account label: redacted
- S3 region/bucket label: redacted
- TiDB/CDC environment label: redacted
- Tenant/task identities:
- Cleanup mode:

Gate 1: Unit And Component Tests
- Command: make test
- Result: PASS/FAIL
- Notes:
- Failures:

P0 Verification Summary:
- SQL secret embedding: PASS/FAIL/NOT RUN
- S3 URI secret embedding: PASS/FAIL/NOT RUN
- External API auth/redaction: PASS/PARTIAL/FAIL/NOT RUN
- RuntimePlan effective path isolation: PASS/PARTIAL/FAIL/NOT RUN
- Security/Perf P0 review: PASS/FAIL/PENDING
- Data/Integration real Snowflake/S3 e2e: PASS/FAIL/BLOCKED_BY_CREDENTIALS/PENDING
- P1 readiness: READY/BLOCKED
- P1 blockers:

Gate 2: Real Snowflake/S3 E2E
- Validate command result: PASS/FAIL
- Run command result: PASS/FAIL
- Artifact directory:
- Sanitization scan: PASS/FAIL
- Cleanup result: PASS/FAIL

Scenario Matrix:
| Scenario ID | Status expected | Result | Evidence | Notes |
| --- | --- | --- | --- | --- |
| DDL-V1-001 | supported | PASS/FAIL | schema, row count, checksum | |
| DDL-V1-002 | supported | PASS/FAIL | table created, rows | |
| DDL-V1-003 | supported | PASS/FAIL | drop ordering, table absent | |
| DDL-V1-004 | deferred | PASS/FAIL | deterministic deferred result | |
| DDL-V1-005..015 | supported/mixed | PASS/FAIL | columns, row values | |
| DDL-V1-017 | supported | PASS/FAIL | metadata no-op log | |
| DDL-V1-020..028 | supported/mixed | PASS/FAIL | lifecycle, ordering, isolation | |

Multi-Tenant/Multi-Task Evidence:
- Tenant/task pairs:
- S3 scoped roots:
- Snowflake schemas/stages:
- Checkpoint prefixes:
- Isolation result: PASS/FAIL

Failure Recovery Evidence:
- Injected failure:
- Expected status:
- Actual status:
- Resume/retry result:
- Peer task impact:
- Cleanup scope:
- Result: PASS/FAIL

Security/Sanitization:
- Secret scan result:
- Raw logs attached: NO
- Redacted values:
- Residual risk:

Final Decision:
- PASS/FAIL
- Release blockers:
- Follow-up issues:
```

## Current QA Baseline

On 2026-06-08, `make test` passed locally on the current dirty worktree. A direct bare package invocation without Makefile link flags failed in `pkg/snowsql` due to the TiDB `runtime.buildVersion` link reference, so QA should use `make test` as the supported unit test command unless the build flags are replicated explicitly.
