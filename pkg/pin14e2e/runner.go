package pin14e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/pingcap-inc/tidb2dw/pkg/testfixtures"
)

type Mode string

const (
	ModeValidate Mode = "validate"
	ModePlan     Mode = "plan"
	ModeRun      Mode = "run"
)

type CleanupMode string

const (
	CleanupAlways    CleanupMode = "always"
	CleanupOnSuccess CleanupMode = "on-success"
	CleanupNever     CleanupMode = "never"
)

type Config struct {
	Mode                        Mode
	FixtureDir                  string
	ManifestPath                string
	Scenarios                   []string
	StorageURI                  string
	AWSRegion                   string
	StagingPrefix               string
	SnowflakeAccountID          string
	SnowflakeWarehouse          string
	SnowflakeDatabase           string
	SnowflakeSchema             string
	SnowflakeStorageIntegration string
	SnowflakeUser               string
	SnowflakePassword           string
	AWSAccessKeyID              string
	AWSSecretAccessKey          string
	AWSSessionToken             string
	CleanupMode                 CleanupMode
}

type StorageClient interface {
	PutObject(ctx context.Context, localPath, objectURI string) error
	DeletePrefix(ctx context.Context, prefixURI string) (int, error)
}

type SnowflakeClient interface {
	PrepareNamespace(ctx context.Context, database, schema string) error
	LoadSnapshot(ctx context.Context, scenario ScenarioRun) error
	ApplyCDC(ctx context.Context, scenario ScenarioRun) error
	FetchValidation(ctx context.Context, scenario ScenarioRun) (ActualOutcome, error)
	Cleanup(ctx context.Context, owned OwnedSnowflakeObjects) (CleanupSnowflakeSummary, error)
}

type ActualOutcome struct {
	SchemaColumns []string
	RowCount      int
	Checksum      string
	SelectedRows  map[string]testfixtures.Row
}

type ScenarioRun struct {
	Manifest       testfixtures.ValidationManifest
	StorageRootURI string
	SnapshotURIs   []string
	IncrementURIs  []string
	Database       string
	Schema         string
	Table          string
}

type OwnedSnowflakeObjects struct {
	Database string
	Schema   string
	Tables   []string
	Stages   []string
}

type CleanupSnowflakeSummary struct {
	TablesDropped int `json:"tables_dropped"`
	StagesDropped int `json:"stages_dropped"`
}

type CleanupSummary struct {
	S3PrefixesDeleted      []string `json:"s3_prefixes_deleted"`
	S3ObjectsDeleted       int      `json:"s3_objects_deleted"`
	SnowflakeTablesDropped int      `json:"snowflake_tables_dropped"`
	SnowflakeStagesDropped int      `json:"snowflake_stages_dropped"`
}

type Result struct {
	Mode            Mode             `json:"mode"`
	ScenarioResults []ScenarioResult `json:"scenario_results"`
	Cleanup         CleanupSummary   `json:"cleanup"`
}

type ScenarioResult struct {
	ScenarioID string `json:"scenario_id"`
	TenantID   string `json:"tenant_id"`
	TaskID     string `json:"task_id"`
	Status     string `json:"status"`
	Error      string `json:"error,omitempty"`
}

type Runner struct {
	Config     Config
	Storage    StorageClient
	Snowflake  SnowflakeClient
	NewClients func(Config) (StorageClient, SnowflakeClient, error)
}

var cleanIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$`)

func (r Runner) Run(ctx context.Context) (*Result, error) {
	cfg := normalizeConfig(r.Config)
	if err := ValidateConfig(cfg); err != nil {
		return nil, err
	}
	manifests, err := LoadManifests(cfg)
	if err != nil {
		return nil, err
	}
	scenarios, err := Plan(cfg, manifests)
	if err != nil {
		return nil, err
	}
	result := &Result{Mode: cfg.Mode}
	if cfg.Mode == ModeValidate || cfg.Mode == ModePlan {
		for _, scenario := range scenarios {
			result.ScenarioResults = append(result.ScenarioResults, ScenarioResult{
				ScenarioID: scenario.Manifest.ScenarioID,
				TenantID:   scenario.Manifest.Tenant.TenantID,
				TaskID:     scenario.Manifest.Tenant.TaskID,
				Status:     "planned",
			})
		}
		return result, nil
	}
	if r.Storage == nil && r.Snowflake == nil {
		newClients := r.NewClients
		if newClients == nil {
			newClients = NewRealClients
		}
		storage, snowflake, err := newClients(cfg)
		if err != nil {
			return nil, fmt.Errorf("%s", sanitizeError(err))
		}
		r.Storage = storage
		r.Snowflake = snowflake
	}
	if r.Storage == nil {
		return nil, fmt.Errorf("storage client is required for run mode")
	}
	if r.Snowflake == nil {
		return nil, fmt.Errorf("snowflake client is required for run mode")
	}

	runErr := r.runScenarios(ctx, scenarios, result)
	if shouldCleanup(cfg.CleanupMode, runErr) {
		cleanup, cleanupErr := r.cleanup(ctx, scenarios)
		result.Cleanup = cleanup
		if cleanupErr != nil && runErr == nil {
			runErr = cleanupErr
		}
	}
	if runErr != nil {
		return result, fmt.Errorf("%s", sanitizeError(runErr))
	}
	return result, nil
}

func (r Runner) runScenarios(ctx context.Context, scenarios []ScenarioRun, result *Result) error {
	var firstErr error
	for _, scenario := range scenarios {
		scenarioResult := ScenarioResult{
			ScenarioID: scenario.Manifest.ScenarioID,
			TenantID:   scenario.Manifest.Tenant.TenantID,
			TaskID:     scenario.Manifest.Tenant.TaskID,
			Status:     "passed",
		}
		if err := r.uploadScenario(ctx, scenario); err != nil {
			err = scenarioError(scenario, err)
			scenarioResult.Status = "failed"
			scenarioResult.Error = sanitizeError(err)
			if firstErr == nil {
				firstErr = err
			}
			result.ScenarioResults = append(result.ScenarioResults, scenarioResult)
			continue
		}
		if err := r.Snowflake.PrepareNamespace(ctx, scenario.Database, scenario.Schema); err != nil {
			err = scenarioError(scenario, err)
			scenarioResult.Status = "failed"
			scenarioResult.Error = sanitizeError(err)
			if firstErr == nil {
				firstErr = err
			}
			result.ScenarioResults = append(result.ScenarioResults, scenarioResult)
			continue
		}
		if err := r.Snowflake.LoadSnapshot(ctx, scenario); err != nil {
			err = scenarioError(scenario, err)
			scenarioResult.Status = "failed"
			scenarioResult.Error = sanitizeError(err)
			if firstErr == nil {
				firstErr = err
			}
			result.ScenarioResults = append(result.ScenarioResults, scenarioResult)
			continue
		}
		if scenario.Manifest.Expected.ExpectedError == "" {
			if err := r.Snowflake.ApplyCDC(ctx, scenario); err != nil {
				err = scenarioError(scenario, err)
				scenarioResult.Status = "failed"
				scenarioResult.Error = sanitizeError(err)
				if firstErr == nil {
					firstErr = err
				}
				result.ScenarioResults = append(result.ScenarioResults, scenarioResult)
				continue
			}
			actual, err := r.Snowflake.FetchValidation(ctx, scenario)
			if err != nil {
				err = scenarioError(scenario, err)
				scenarioResult.Status = "failed"
				scenarioResult.Error = sanitizeError(err)
				if firstErr == nil {
					firstErr = err
				}
				result.ScenarioResults = append(result.ScenarioResults, scenarioResult)
				continue
			}
			if err := CompareOutcome(scenario.Manifest.Expected, actual); err != nil {
				err = scenarioError(scenario, err)
				scenarioResult.Status = "failed"
				scenarioResult.Error = sanitizeError(err)
				if firstErr == nil {
					firstErr = err
				}
			}
		}
		result.ScenarioResults = append(result.ScenarioResults, scenarioResult)
	}
	return firstErr
}

func scenarioError(scenario ScenarioRun, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("scenario=%s tenant=%s task=%s table=%s: %w",
		scenario.Manifest.ScenarioID,
		scenario.Manifest.Tenant.TenantID,
		scenario.Manifest.Tenant.TaskID,
		scenario.Table,
		err,
	)
}

func (r Runner) uploadScenario(ctx context.Context, scenario ScenarioRun) error {
	files := append([]string{}, scenario.Manifest.SnapshotFiles...)
	files = append(files, scenario.Manifest.IncrementFiles...)
	files = append(files, scenario.Manifest.MappingFile)
	for _, rel := range files {
		if rel == "" {
			continue
		}
		localPath := filepath.Join(r.Config.FixtureDir, filepath.FromSlash(rel))
		objectURI, err := joinObjectURI(scenario.StorageRootURI, rel)
		if err != nil {
			return err
		}
		if err := r.Storage.PutObject(ctx, localPath, objectURI); err != nil {
			return fmt.Errorf("upload %s: %w", rel, err)
		}
	}
	return nil
}

func (r Runner) cleanup(ctx context.Context, scenarios []ScenarioRun) (CleanupSummary, error) {
	var summary CleanupSummary
	var firstErr error
	prefixes := map[string]struct{}{}
	owned := OwnedSnowflakeObjects{
		Database: r.Config.SnowflakeDatabase,
		Schema:   r.Config.SnowflakeSchema,
	}
	for _, scenario := range scenarios {
		prefixes[scenario.StorageRootURI] = struct{}{}
		owned.Tables = append(owned.Tables, scenario.Table)
		owned.Stages = append(owned.Stages, stageName(scenario, "SNAPSHOT"), stageName(scenario, "INCREMENT"))
	}
	for prefix := range prefixes {
		count, err := r.Storage.DeletePrefix(ctx, prefix)
		summary.S3PrefixesDeleted = append(summary.S3PrefixesDeleted, sanitizeForReport(prefix))
		summary.S3ObjectsDeleted += count
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("cleanup storage prefix %s: %w", sanitizeForReport(prefix), err)
		}
	}
	sort.Strings(summary.S3PrefixesDeleted)
	snowflake, err := r.Snowflake.Cleanup(ctx, owned)
	summary.SnowflakeTablesDropped = snowflake.TablesDropped
	summary.SnowflakeStagesDropped = snowflake.StagesDropped
	if err != nil && firstErr == nil {
		firstErr = fmt.Errorf("cleanup snowflake objects: %w", err)
	}
	return summary, firstErr
}

func normalizeConfig(cfg Config) Config {
	if cfg.Mode == "" {
		cfg.Mode = ModeValidate
	}
	if cfg.CleanupMode == "" {
		cfg.CleanupMode = CleanupAlways
	}
	if cfg.ManifestPath == "" && cfg.FixtureDir != "" {
		cfg.ManifestPath = filepath.Join(cfg.FixtureDir, "manifests", "manifest.json")
	}
	cfg.StagingPrefix = normalizePathPrefix(cfg.StagingPrefix)
	return cfg
}

func ValidateConfig(cfg Config) error {
	required := map[string]string{
		"fixture-dir":                cfg.FixtureDir,
		"manifest-path":              cfg.ManifestPath,
		"PIN14_S3_STORAGE_URI":       cfg.StorageURI,
		"PIN14_AWS_REGION":           cfg.AWSRegion,
		"PIN14_STAGING_PREFIX":       cfg.StagingPrefix,
		"PIN14_SNOWFLAKE_ACCOUNT_ID": cfg.SnowflakeAccountID,
		"PIN14_SNOWFLAKE_WAREHOUSE":  cfg.SnowflakeWarehouse,
		"PIN14_SNOWFLAKE_DATABASE":   cfg.SnowflakeDatabase,
		"PIN14_SNOWFLAKE_SCHEMA":     cfg.SnowflakeSchema,
		"PIN14_SNOWFLAKE_USER":       cfg.SnowflakeUser,
		"PIN14_SNOWFLAKE_PASSWORD":   cfg.SnowflakePassword,
	}
	if cfg.SnowflakeStorageIntegration == "" {
		required["AWS_ACCESS_KEY_ID"] = cfg.AWSAccessKeyID
		required["AWS_SECRET_ACCESS_KEY"] = cfg.AWSSecretAccessKey
	}
	missing := make([]string, 0)
	for name, value := range required {
		if value == "" {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		return fmt.Errorf("missing required config names: %s", strings.Join(missing, ", "))
	}
	if cfg.Mode != ModeValidate && cfg.Mode != ModePlan && cfg.Mode != ModeRun {
		return fmt.Errorf("mode must be one of validate, plan, run")
	}
	if cfg.CleanupMode != CleanupAlways && cfg.CleanupMode != CleanupOnSuccess && cfg.CleanupMode != CleanupNever {
		return fmt.Errorf("cleanup mode must be one of always, on-success, never")
	}
	parsed, err := url.Parse(cfg.StorageURI)
	if err != nil || parsed.Scheme != "s3" || parsed.Host == "" {
		return fmt.Errorf("PIN14_S3_STORAGE_URI must be an absolute s3:// URI")
	}
	if parsed.RawQuery != "" || parsed.User != nil {
		return fmt.Errorf("PIN14_S3_STORAGE_URI must not include credentials or query strings")
	}
	if cfg.StagingPrefix == "" || strings.Contains(cfg.StagingPrefix, "..") || strings.Contains(cfg.StagingPrefix, "\\") {
		return fmt.Errorf("PIN14_STAGING_PREFIX must be a non-empty scoped path without traversal")
	}
	for _, name := range []string{cfg.SnowflakeDatabase, cfg.SnowflakeSchema} {
		if !cleanIDPattern.MatchString(name) {
			return fmt.Errorf("snowflake namespace %q must use a clean identifier", name)
		}
	}
	if cfg.SnowflakeStorageIntegration != "" && !cleanIDPattern.MatchString(cfg.SnowflakeStorageIntegration) {
		return fmt.Errorf("PIN14_SNOWFLAKE_STORAGE_INTEGRATION must use a clean identifier")
	}
	return nil
}

func LoadManifests(cfg Config) ([]testfixtures.ValidationManifest, error) {
	data, err := os.ReadFile(cfg.ManifestPath)
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	var result testfixtures.Result
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	selected := map[string]struct{}{}
	for _, scenario := range cfg.Scenarios {
		scenario = strings.TrimSpace(scenario)
		if scenario != "" {
			selected[scenario] = struct{}{}
		}
	}
	manifests := make([]testfixtures.ValidationManifest, 0, len(result.Manifests))
	for _, manifest := range result.Manifests {
		if len(selected) > 0 {
			if _, ok := selected[manifest.ScenarioID]; !ok {
				continue
			}
		}
		if err := ensureManifestFiles(cfg.FixtureDir, manifest); err != nil {
			return nil, err
		}
		manifests = append(manifests, manifest)
	}
	if len(manifests) == 0 {
		return nil, fmt.Errorf("no manifests selected")
	}
	return manifests, nil
}

func ensureManifestFiles(root string, manifest testfixtures.ValidationManifest) error {
	files := append([]string{}, manifest.SnapshotFiles...)
	files = append(files, manifest.IncrementFiles...)
	files = append(files, manifest.MappingFile)
	for _, rel := range files {
		if rel == "" {
			continue
		}
		if strings.HasPrefix(rel, "/") || strings.Contains(rel, "..") {
			return fmt.Errorf("manifest %s has unsafe relative path %q", manifest.ScenarioID, rel)
		}
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel))); err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("manifest %s references missing file %s", manifest.ScenarioID, rel)
			}
			if errorsIsPermission(err) {
				return fmt.Errorf("manifest %s references unreadable file %s", manifest.ScenarioID, rel)
			}
			return err
		}
	}
	return nil
}

func Plan(cfg Config, manifests []testfixtures.ValidationManifest) ([]ScenarioRun, error) {
	cfg = normalizeConfig(cfg)
	scenarios := make([]ScenarioRun, 0, len(manifests))
	seenScopes := map[string]struct{}{}
	for _, manifest := range manifests {
		root, err := joinObjectURI(cfg.StorageURI, path.Join(cfg.StagingPrefix, "tenants", manifest.Tenant.TenantID, "tasks", manifest.Tenant.TaskID, "scenarios", manifest.ScenarioID))
		if err != nil {
			return nil, err
		}
		scope := strings.ToLower(root)
		if _, ok := seenScopes[scope]; ok {
			return nil, fmt.Errorf("duplicate scenario scope %s", root)
		}
		seenScopes[scope] = struct{}{}
		scenario := ScenarioRun{
			Manifest:       manifest,
			StorageRootURI: root,
			Database:       cfg.SnowflakeDatabase,
			Schema:         cfg.SnowflakeSchema,
			Table:          tableName(manifest),
		}
		for _, rel := range manifest.SnapshotFiles {
			uri, err := joinObjectURI(root, rel)
			if err != nil {
				return nil, err
			}
			scenario.SnapshotURIs = append(scenario.SnapshotURIs, uri)
		}
		for _, rel := range manifest.IncrementFiles {
			uri, err := joinObjectURI(root, rel)
			if err != nil {
				return nil, err
			}
			scenario.IncrementURIs = append(scenario.IncrementURIs, uri)
		}
		scenarios = append(scenarios, scenario)
	}
	return scenarios, nil
}

func CompareOutcome(expected testfixtures.ExpectedOutcome, actual ActualOutcome) error {
	if !stringSlicesEqual(expected.FinalSchemaColumns, actual.SchemaColumns) {
		return fmt.Errorf("schema mismatch: expected %v, got %v", expected.FinalSchemaColumns, actual.SchemaColumns)
	}
	if expected.FinalRowCount != actual.RowCount {
		return fmt.Errorf("row count mismatch: expected %d, got %d", expected.FinalRowCount, actual.RowCount)
	}
	if expected.Checksum != actual.Checksum {
		return fmt.Errorf("checksum mismatch: expected %s, got %s", expected.Checksum, actual.Checksum)
	}
	for key, expectedRow := range expected.SelectedRows {
		actualRow, ok := actual.SelectedRows[key]
		if !ok {
			return fmt.Errorf("selected row %s missing", key)
		}
		if !rowsEqual(expectedRow, actualRow) {
			return fmt.Errorf("selected row %s mismatch", key)
		}
	}
	return nil
}

func shouldCleanup(mode CleanupMode, runErr error) bool {
	switch mode {
	case CleanupAlways:
		return true
	case CleanupOnSuccess:
		return runErr == nil
	default:
		return false
	}
}

func normalizePathPrefix(prefix string) string {
	return strings.Trim(path.Clean("/"+prefix), "/")
}

func joinObjectURI(base string, parts ...string) (string, error) {
	parsed, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	cleanParts := []string{parsed.Path}
	for _, part := range parts {
		if part != "" {
			cleanParts = append(cleanParts, part)
		}
	}
	parsed.Path = path.Join(cleanParts...)
	if !strings.HasPrefix(parsed.Path, "/") {
		parsed.Path = "/" + parsed.Path
	}
	return parsed.String(), nil
}

func tableName(manifest testfixtures.ValidationManifest) string {
	return strings.ToUpper(fmt.Sprintf("%s_%s_%s", manifest.Tenant.TenantID, manifest.Tenant.TaskID, manifest.Table))
}

func stageName(scenario ScenarioRun, kind string) string {
	return strings.ToUpper(fmt.Sprintf("%s_%s_%s_%s", scenario.Manifest.Tenant.TenantID, scenario.Manifest.Tenant.TaskID, scenario.Manifest.ScenarioID, kind))
}

func sanitizeError(err error) string {
	if err == nil {
		return ""
	}
	return sanitizeForReport(err.Error())
}

func sanitizeForReport(msg string) string {
	secretPatterns := []string{"password", "secret", "token", "access-key", "session-token", "aws_key_id", "aws_secret_key", "aws_token"}
	for _, pattern := range secretPatterns {
		msg = regexp.MustCompile(`(?i)`+regexp.QuoteMeta(pattern)+`[=:][^[:space:]]+`).ReplaceAllString(msg, pattern+"=<redacted>")
		msg = regexp.MustCompile(`(?i)`+regexp.QuoteMeta(pattern)+`='[^']*'`).ReplaceAllString(msg, pattern+"='<redacted>'")
	}
	msg = regexp.MustCompile(`(?i)s3://[^[:space:]'",)]+`).ReplaceAllString(msg, "s3://<redacted>")
	msg = regexp.MustCompile(`(?i)[A-Z0-9_-]+\.snowflakecomputing\.com`).ReplaceAllString(msg, "<redacted>.snowflakecomputing.com")
	msg = regexp.MustCompile(`(?i)(account=|host=|dsn=)[^[:space:]]+`).ReplaceAllString(msg, "$1<redacted>")
	return msg
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func rowsEqual(a, b testfixtures.Row) bool {
	if len(a) != len(b) {
		return false
	}
	for key, av := range a {
		if b[key] != av {
			return false
		}
	}
	return true
}

func errorsIsPermission(err error) bool {
	return err != nil && strings.Contains(err.Error(), fs.ErrPermission.Error())
}
