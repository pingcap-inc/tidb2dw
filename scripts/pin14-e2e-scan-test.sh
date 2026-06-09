#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PIN14_E2E_SOURCE_ONLY=1 source "$ROOT_DIR/scripts/pin14-e2e.sh"
unset PIN14_E2E_SOURCE_ONLY

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

export AWS_ACCESS_KEY_ID="TEST_AWS_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="TEST_AWS_SECRET_ACCESS_KEY"
export AWS_SESSION_TOKEN="TEST_AWS_SESSION_TOKEN"
export PIN14_S3_STORAGE_URI="s3://""example-bucket/e2e-root"
export PIN14_SNOWFLAKE_ACCOUNT_ID="example-account"
export PIN14_SNOWFLAKE_USER="TEST_SNOWFLAKE_USER"
export PIN14_SNOWFLAKE_PASSWORD="TEST_SNOWFLAKE_PASSWORD"
export PIN14_TIDB_USER="TEST_TIDB_USER"
export PIN14_TIDB_PASSWORD="TEST_TIDB_PASSWORD"

write_artifact() {
	local name="$1"
	local content="$2"
	local dir="$tmp_dir/$name"
	mkdir -p "$dir"
	printf '%s\n' "$content" >"$dir/stdout.log"
	printf '%s' "$dir"
}

assert_accepts() {
	local name="$1"
	local content="$2"
	local dir
	dir="$(write_artifact "$name" "$content")"
	scan_artifacts "$dir"
}

assert_rejects() {
	local name="$1"
	local content="$2"
	local dir
	dir="$(write_artifact "$name" "$content")"
	if (scan_artifacts "$dir") >/dev/null 2>&1; then
		printf 'expected artifact scan to reject %s\n' "$name" >&2
		exit 1
	fi
}

assert_wrapper_accepts_redacted_cleanup() {
	local fake_bin="$tmp_dir/fake-tidb2dw"
	cat >"$fake_bin" <<'FAKE_TIDB2DW'
#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
	fixturegen)
		exit 0
		;;
	pin14-e2e)
		printf 'cleanup removed prefix s3://<redacted>\n'
		printf 'cleanup removed prefixes ["s3://<redacted>"]\n'
		exit 0
		;;
	*)
		exit 2
		;;
esac
FAKE_TIDB2DW
	chmod +x "$fake_bin"

	(
		export AWS_REGION="us-west-2"
		export PIN14_AWS_REGION="us-west-2"
		export PIN14_STAGING_PREFIX="pin14/test"
		export PIN14_TENANT_ID="tenant-a"
		export PIN14_TASK_ID="task-orders"
		export PIN14_TIDB_HOST="127.0.0.1"
		export PIN14_TIDB_PORT="4000"
		export PIN14_CDC_HOST="127.0.0.1"
		export PIN14_CDC_PORT="8300"
		export PIN14_TABLES="sales.orders"
		export PIN14_ENV_LABEL="scan-test"
		export PIN14_CLEANUP_MODE="always"
		export PIN14_E2E_MODE="run"
		export PIN14_TIDB2DW_BIN="$fake_bin"
		export PIN14_ARTIFACT_DIR="$tmp_dir/wrapper-artifacts"
		export PIN14_E2E_RUN_ID="redacted-cleanup-pass"
		export PIN14_SNOWFLAKE_DATABASE="TEST_DB"
		export PIN14_SNOWFLAKE_SCHEMA="TEST_SCHEMA"
		export PIN14_SNOWFLAKE_WAREHOUSE="TEST_WH"
		export PIN14_SNOWFLAKE_STORAGE_INTEGRATION=""

		"$ROOT_DIR/scripts/pin14-e2e.sh" >"$tmp_dir/wrapper.stdout" 2>"$tmp_dir/wrapper.stderr"
	)
}

assert_wrapper_accepts_redacted_cleanup

redacted_account_url="<redacted>"".snowflakecomputing.com"
assert_accepts "redacted-placeholders" "cleanup removed prefixes [\"s3://<redacted>\"]
cleanup root s3://<redacted>,
password=<redacted>
token='<redacted>'
dsn=<redacted>
account ${redacted_account_url}"

raw_s3_uri="s3://""raw-bucket/raw-prefix"
raw_access_key="AKIA""IOSFODNN7EXAMPLE"
raw_snowflake_account_url="demo-account"".snowflakecomputing.com"
raw_dsn="dsn=snowflake://""user:pass@example/db"

assert_rejects "raw-s3-uri" "cleanup removed ${raw_s3_uri}"
assert_rejects "known-secret-value" "leaked value TEST_AWS_SECRET_ACCESS_KEY"
assert_rejects "aws-access-key-id" "aws_access_key_id=${raw_access_key}"
assert_rejects "aws-secret-key-label" "aws_secret_access_key='not-redacted-secret'"
assert_rejects "session-token-label" "session-token=not-redacted-token"
assert_rejects "password-label" "password=not-redacted-password"
assert_rejects "snowflake-account-url" "connected to ${raw_snowflake_account_url}"
assert_rejects "dsn-label" "${raw_dsn}"

printf 'pin14-e2e artifact scan tests passed\n'
