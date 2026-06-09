#!/usr/bin/env bash
set -euo pipefail

usage() {
	cat <<'USAGE'
Usage: PIN14_E2E_MODE=validate|run|skip scripts/pin14-e2e.sh

Required environment is documented in docs/pin14-e2e-runbook.md. The wrapper
validates config before external side effects, runs tidb2dw in run mode, and
rejects logs/artifacts that contain known secret values or secret-like material.
USAGE
}

log() {
	printf '[pin14-e2e] %s\n' "$*"
}

fail() {
	printf '[pin14-e2e] ERROR: %s\n' "$*" >&2
	exit 1
}

require_env() {
	local name="$1"
	if [[ -z "${!name:-}" ]]; then
		missing+=("$name")
	fi
}

require_clean_id() {
	local name="$1"
	local value="${!name:-}"
	if [[ ! "$value" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$ ]]; then
		fail "$name must match ^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$"
	fi
}

normalize_prefix() {
	local prefix="$1"
	prefix="${prefix#/}"
	prefix="${prefix%/}"
	if [[ -z "$prefix" ]]; then
		fail "PIN14_STAGING_PREFIX cannot be empty after normalization"
	fi
	if [[ "$prefix" == *".."* || "$prefix" == *"?"* || "$prefix" == *"\\"* ]]; then
		fail "PIN14_STAGING_PREFIX cannot contain path traversal, query strings, or backslashes"
	fi
	if [[ ! "$prefix" =~ ^[A-Za-z0-9._/-]+$ ]]; then
		fail "PIN14_STAGING_PREFIX may only contain ASCII letters, digits, dot, underscore, dash, and slash"
	fi
	printf '%s' "$prefix"
}

validate_config() {
	missing=()
	local required=(
		AWS_ACCESS_KEY_ID
		AWS_SECRET_ACCESS_KEY
		AWS_REGION
		PIN14_SNOWFLAKE_ACCOUNT_ID
		PIN14_SNOWFLAKE_USER
		PIN14_SNOWFLAKE_PASSWORD
		PIN14_SNOWFLAKE_DATABASE
		PIN14_SNOWFLAKE_SCHEMA
		PIN14_SNOWFLAKE_WAREHOUSE
		PIN14_S3_STORAGE_URI
		PIN14_AWS_REGION
		PIN14_STAGING_PREFIX
		PIN14_TENANT_ID
		PIN14_TASK_ID
		PIN14_TIDB_HOST
		PIN14_TIDB_PORT
		PIN14_TIDB_USER
		PIN14_TIDB_PASSWORD
		PIN14_CDC_HOST
		PIN14_CDC_PORT
		PIN14_TABLES
		PIN14_ENV_LABEL
		PIN14_CLEANUP_MODE
	)
	for name in "${required[@]}"; do
		require_env "$name"
	done
	if ((${#missing[@]} > 0)); then
		printf '[pin14-e2e] ERROR: missing required config names:\n' >&2
		printf '  - %s\n' "${missing[@]}" >&2
		exit 1
	fi

	require_clean_id PIN14_TENANT_ID
	require_clean_id PIN14_TASK_ID
	require_clean_id PIN14_ENV_LABEL

	case "$PIN14_CLEANUP_MODE" in
		always|on-success|never) ;;
		*) fail "PIN14_CLEANUP_MODE must be one of: always, on-success, never" ;;
	esac

	case "$PIN14_S3_STORAGE_URI" in
		s3://*) ;;
		*) fail "PIN14_S3_STORAGE_URI must start with s3://" ;;
	esac
	if [[ "$PIN14_S3_STORAGE_URI" == *"?"* ]]; then
		fail "PIN14_S3_STORAGE_URI must not contain query strings or embedded credentials"
	fi

	PIN14_NORMALIZED_STAGING_PREFIX="$(normalize_prefix "$PIN14_STAGING_PREFIX")"
	export PIN14_NORMALIZED_STAGING_PREFIX
}

artifact_root() {
	local run_id="${PIN14_E2E_RUN_ID:-${GITHUB_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}}"
	printf '%s/pin14-e2e-%s-%s-%s-%s' \
		"${PIN14_ARTIFACT_DIR:-artifacts/pin14-e2e}" \
		"$PIN14_ENV_LABEL" \
		"$PIN14_TENANT_ID" \
		"$PIN14_TASK_ID" \
		"$run_id"
}

scan_artifacts() {
	local target="$1"
	local secrets=(
		"${AWS_ACCESS_KEY_ID:-}"
		"${AWS_SECRET_ACCESS_KEY:-}"
		"${AWS_SESSION_TOKEN:-}"
		"${PIN14_S3_STORAGE_URI:-}"
		"${PIN14_SNOWFLAKE_ACCOUNT_ID:-}"
		"${PIN14_SNOWFLAKE_USER:-}"
		"${PIN14_SNOWFLAKE_PASSWORD:-}"
		"${PIN14_TIDB_USER:-}"
		"${PIN14_TIDB_PASSWORD:-}"
	)
	for secret in "${secrets[@]}"; do
		if [[ -n "$secret" ]] && grep -R -F -q -- "$secret" "$target"; then
			fail "secret value detected in $target"
		fi
	done

	local unredacted_value="['\"]?[^<'\"[:space:]][^[:space:]]*"
	local secret_like_pattern="(access-key[=:]${unredacted_value}|secret-access-key[=:]${unredacted_value}|session-token[=:]${unredacted_value}|x-amz-signature[=:]${unredacted_value}|aws_access_key_id[=:]${unredacted_value}|aws_key_id[=:]${unredacted_value}|aws_secret_key[=:]${unredacted_value}|aws_secret_access_key[=:]${unredacted_value}|aws_session_token[=:]${unredacted_value}|aws_token[=:]${unredacted_value}|snowflake\.pass[=:]${unredacted_value}|tidb\.pass[=:]${unredacted_value}|password[=:]${unredacted_value}|token[=:]${unredacted_value}|dsn[=:]${unredacted_value}|s3://[[:alnum:]][^[:space:]]+|[[:alnum:]_.-]+\.snowflakecomputing\.com|A(KIA|SIA)[A-Z0-9]{16})"
	if grep -R -E -i -q "$secret_like_pattern" "$target"; then
		fail "secret-like material detected in $target"
	fi
}

main() {
	if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
		usage
		exit 0
	fi

	local mode="${PIN14_E2E_MODE:-validate}"
	case "$mode" in
		skip)
			log "external Snowflake/S3 e2e run skipped explicitly by PIN14_E2E_MODE=skip"
			exit 0
			;;
		validate|run) ;;
		*) fail "PIN14_E2E_MODE must be one of: validate, run, skip" ;;
	esac

	validate_config

	if [[ "$mode" == "validate" ]]; then
		log "configuration validation passed; no external side effects were performed"
		exit 0
	fi

	local out_dir
	out_dir="$(artifact_root)"
	mkdir -p "$out_dir"

	local raw_log="$out_dir/raw.log"
	local summary="$out_dir/summary.env"
	local tidb2dw_bin="${PIN14_TIDB2DW_BIN:-./bin/tidb2dw}"
	local fixture_dir="${PIN14_FIXTURE_DIR:-$out_dir/fixtures}"
	local manifest_path="${PIN14_MANIFEST_PATH:-$fixture_dir/manifests/manifest.json}"

	export TIDB2DW_TIDB_HOST="$PIN14_TIDB_HOST"
	export TIDB2DW_TIDB_PORT="$PIN14_TIDB_PORT"
	export TIDB2DW_TIDB_USER="$PIN14_TIDB_USER"
	export TIDB2DW_TIDB_PASSWORD="$PIN14_TIDB_PASSWORD"
	export TIDB2DW_TIDB_SSL_CA="${PIN14_TIDB_SSL_CA:-}"
	export TIDB2DW_CDC_HOST="$PIN14_CDC_HOST"
	export TIDB2DW_CDC_PORT="$PIN14_CDC_PORT"
	export TIDB2DW_SNOWFLAKE_ACCOUNT_ID="$PIN14_SNOWFLAKE_ACCOUNT_ID"
	export TIDB2DW_SNOWFLAKE_USER="$PIN14_SNOWFLAKE_USER"
	export TIDB2DW_SNOWFLAKE_PASSWORD="$PIN14_SNOWFLAKE_PASSWORD"
	export TIDB2DW_SNOWFLAKE_DATABASE="$PIN14_SNOWFLAKE_DATABASE"
	export TIDB2DW_SNOWFLAKE_SCHEMA="$PIN14_SNOWFLAKE_SCHEMA"
	export TIDB2DW_SNOWFLAKE_WAREHOUSE="$PIN14_SNOWFLAKE_WAREHOUSE"
	export TIDB2DW_SNOWFLAKE_STORAGE_INTEGRATION="$PIN14_SNOWFLAKE_STORAGE_INTEGRATION"
	export TIDB2DW_TENANT_ID="$PIN14_TENANT_ID"
	export TIDB2DW_TASK_ID="$PIN14_TASK_ID"
	export TIDB2DW_STAGING_PREFIX="$PIN14_NORMALIZED_STAGING_PREFIX"

	IFS=',' read -r -a tables <<< "$PIN14_TABLES"
	local table_count=0
	for table in "${tables[@]}"; do
		table="${table//[[:space:]]/}"
		if [[ -n "$table" ]]; then
			table_count=$((table_count + 1))
		fi
	done
	if ((table_count == 0)); then
		fail "PIN14_TABLES must contain at least one database.table entry"
	fi

	log "generating deterministic fixtures under $fixture_dir"
	set +e
	"$tidb2dw_bin" fixturegen \
		--output-dir "$fixture_dir" \
		--profile "${PIN14_FIXTURE_PROFILE:-e2e}" \
		--seed "${PIN14_FIXTURE_SEED:-140}" \
		--scenarios "${PIN14_SCENARIOS:-all}" \
		>"$out_dir/fixturegen.stdout.log" 2>"$out_dir/fixturegen.stderr.log"
	local fixture_status=$?
	set -e
	if ((fixture_status != 0)); then
		scan_artifacts "$out_dir"
		fail "tidb2dw fixturegen failed with exit status $fixture_status"
	fi

	local command=("$tidb2dw_bin" pin14-e2e \
		--mode run \
		--fixture-dir "$fixture_dir" \
		--manifest "$manifest_path" \
		--scenarios "${PIN14_SCENARIOS:-all}" \
		--staging-prefix "$PIN14_NORMALIZED_STAGING_PREFIX")

	log "starting run; raw log and summary will be written under $out_dir"
	set +e
	"${command[@]}" >"$out_dir/stdout.log" 2>"$out_dir/stderr.log"
	local status=$?
	set -e

	{
		printf 'PIN14_ENV_LABEL=%s\n' "$PIN14_ENV_LABEL"
		printf 'PIN14_TENANT_ID=%s\n' "$PIN14_TENANT_ID"
		printf 'PIN14_TASK_ID=%s\n' "$PIN14_TASK_ID"
		printf 'PIN14_SCENARIOS=%s\n' "${PIN14_SCENARIOS:-}"
		printf 'PIN14_STAGING_ROOT=<redacted>\n'
		printf 'PIN14_CLEANUP_MODE=%s\n' "$PIN14_CLEANUP_MODE"
		printf 'PIN14_EXIT_STATUS=%s\n' "$status"
	} >"$summary"

	scan_artifacts "$out_dir"

	if ((status != 0)); then
		fail "tidb2dw Snowflake/S3 e2e command failed with exit status $status"
	fi
	log "run completed and artifact sanitization passed: $out_dir"
}

if [[ "${PIN14_E2E_SOURCE_ONLY:-0}" != "1" ]]; then
	main "$@"
fi
