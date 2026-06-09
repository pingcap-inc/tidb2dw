# PIN-14 Snowflake/S3 E2E Runbook

This runbook defines the secure execution contract for the real Snowflake/S3
e2e validation used by PIN-14 and PIN-14.3. It documents config names only;
do not commit, paste, or attach secret values.

## Config Contract

Secrets must come from the operator shell, CI secrets, or an approved secret
store. Non-secret labels and prefixes may be passed as normal environment
variables.

Required Snowflake config:

- `PIN14_SNOWFLAKE_ACCOUNT_ID`
- `PIN14_SNOWFLAKE_USER`
- `PIN14_SNOWFLAKE_PASSWORD`
- `PIN14_SNOWFLAKE_DATABASE`
- `PIN14_SNOWFLAKE_SCHEMA`
- `PIN14_SNOWFLAKE_WAREHOUSE`

Optional Snowflake stage config:

- `PIN14_SNOWFLAKE_STORAGE_INTEGRATION`, preferred when a Snowflake storage
  integration is authorized for the test S3 location. When unset, the runner
  creates external stages with AWS credentials read from the required S3
  environment variables.

Required S3 config:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_SESSION_TOKEN` when using temporary credentials
- `AWS_REGION`
- `PIN14_S3_STORAGE_URI`, for example `s3://<bucket>/<e2e-root>`, with no query string
- `PIN14_AWS_REGION`
- `PIN14_STAGING_PREFIX`, scoped to the release run, for example `pin14/20260608`

Required TiDB and CDC config:

- `PIN14_TIDB_HOST`
- `PIN14_TIDB_PORT`
- `PIN14_TIDB_USER`
- `PIN14_TIDB_PASSWORD`
- `PIN14_TIDB_SSL_CA` when TLS is required
- `PIN14_CDC_HOST`
- `PIN14_CDC_PORT`

Required run identity and evidence config:

- `PIN14_ENV_LABEL`, such as `ci-sf-s3-usw2` or `local-sf-s3-dev`
- `PIN14_TENANT_ID`
- `PIN14_TASK_ID`
- `PIN14_TABLES`, comma-separated `database.table` values
- `PIN14_SCENARIOS`, comma-separated PIN scenario IDs selected for evidence
- `PIN14_CLEANUP_MODE`, one of `always`, `on-success`, or `never`
- `PIN14_E2E_MODE`, one of `validate`, `run`, or `skip`

Optional wrapper config:

- `PIN14_TIDB2DW_BIN`, default `./bin/tidb2dw`
- `PIN14_ARTIFACT_DIR`, default `artifacts/pin14-e2e`
- `PIN14_E2E_RUN_ID`, default `GITHUB_RUN_ID` or UTC timestamp
- `PIN14_FIXTURE_DIR`, default `<artifact-dir>/fixtures`
- `PIN14_MANIFEST_PATH`, default `<fixture-dir>/manifests/manifest.json`
- `PIN14_FIXTURE_PROFILE`, default `e2e`
- `PIN14_FIXTURE_SEED`, default `140`

## Local Invocation

Load secrets into the environment from an approved store or local shell. Do not
put secret values in tracked files.

```bash
make build

export PIN14_E2E_MODE=validate
export PIN14_ENV_LABEL=local-sf-s3-dev
export PIN14_TENANT_ID=tenant-a
export PIN14_TASK_ID=task-1
export PIN14_TABLES=sales.orders,sales.refunds
export PIN14_SCENARIOS=DDL-V1-001,DDL-V1-002
export PIN14_CLEANUP_MODE=always

# Export the required AWS, Snowflake, TiDB, CDC, S3 URI, and staging prefix
# variables from a secret store or shell session here.

scripts/pin14-e2e.sh

export PIN14_E2E_MODE=run
scripts/pin14-e2e.sh
```

Use `validate` first. It validates all required config names and returns before
network calls, S3 writes, Snowflake writes, or local artifact creation.

## CI Invocation

The manual `PIN-14 Snowflake/S3 E2E` workflow injects secret values from GitHub
Actions secrets and uses `PIN14_E2E_MODE` to make external-environment behavior
explicit:

- `skip` exits successfully with an explicit skip message.
- `validate` fails if required config is missing, but performs no external side
  effects.
- `run` performs validation, executes the e2e command, scans artifacts, and
  returns non-zero on any command or sanitization failure.

In `run` mode the wrapper first generates deterministic fixtures with
`tidb2dw fixturegen`, then invokes `tidb2dw pin14-e2e --mode run`. Secret-bearing
Snowflake and AWS values are read from the environment; they are not passed as
CLI arguments.

CI must not silently pass when external access is unavailable. Use `skip` only
when the release checklist records that no real Snowflake/S3 evidence was
attempted. Use `validate` to verify secret wiring before allowing a real run.

## Sanitization Rules

Raw logs and artifacts are not release evidence until `scripts/pin14-e2e.sh`
finishes artifact scanning successfully.

Reject and do not attach artifacts that contain:

- Any exact secret value from AWS, Snowflake, or TiDB environment variables.
- DSNs or object-storage URLs with query strings.
- Presigned or derived URLs, including `X-Amz-Signature`.
- `secret-access-key`, `session-token`, `password=`, `token=`,
  `snowflake.pass`, or `tidb.pass` material.
- Account identifiers, usernames, bucket names, or hostnames when the release
  report does not need them. Use `PIN14_ENV_LABEL`, `PIN14_TENANT_ID`,
  `PIN14_TASK_ID`, scenario IDs, checksums, counts, and object/table counts
  instead.

The wrapper stores artifacts under:

```text
artifacts/pin14-e2e/pin14-e2e-<env-label>-<tenant-id>-<task-id>-<run-id>/
```

Attach only sanitized summaries and evidence files. Do not attach raw logs if
sanitization fails.

## Cleanup And Rollback

The run may remove only resources inside the generated scoped root:

```text
<PIN14_S3_STORAGE_URI>/<PIN14_STAGING_PREFIX>/tenants/<PIN14_TENANT_ID>/tasks/<PIN14_TASK_ID>/
```

Snowflake cleanup is limited to the configured PIN-14 database/schema and the
tables, external stages, load markers, and temporary objects created for the
same tenant/task identity. Never drop shared databases, shared schemas, or S3
prefixes above the scoped root.

If cleanup partially fails:

- Return non-zero and mark the run as failed.
- Preserve sanitized cleanup output in the artifact directory.
- Record the remaining scoped S3 prefixes and Snowflake object names without
  credentials, presigned URLs, or account URLs.
- Do not retry by widening the cleanup prefix or dropping parent schemas.
- Re-run cleanup only with the same tenant/task scope after confirming ownership.

Rollback means stopping the e2e command, preserving sanitized evidence, and
cleaning only test-created objects. It does not modify customer data or
production Snowflake/S3 namespaces.

## QA Checklist For PIN-14.3

1. Record the branch or commit under test and selected `PIN14_SCENARIOS`.
2. Run `make test` and `make build`.
3. Run `PIN14_E2E_MODE=validate scripts/pin14-e2e.sh` and confirm it passes
   before external side effects.
4. Run `PIN14_E2E_MODE=run scripts/pin14-e2e.sh` against the approved real
   Snowflake/S3 environment.
5. Confirm snapshot-through-S3 validation, CDC DML, supported DDL scenarios,
   and at least two tenant/task identities when the runner supports the full
   PIN-14 matrix.
6. Confirm failures return non-zero for missing credentials, invalid
   credentials, secret-like material in artifacts, partial cleanup failure, and
   explicit CI `skip` or `validate` modes.
7. Confirm cleanup touched only the scoped S3 root and test-created Snowflake
   objects.
8. Publish only sanitized summaries, scenario IDs, counts, checksums,
   tenant/task labels, and artifact names. Do not publish secret values, raw
   DSNs, presigned URLs, Snowflake account URLs, or raw logs that failed
   scanning.
