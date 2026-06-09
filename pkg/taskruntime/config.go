package taskruntime

import (
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strings"
)

type WarehouseKind string

const (
	WarehouseSnowflake WarehouseKind = "snowflake"
)

type StorageProvider string

const (
	StorageS3  StorageProvider = "s3"
	StorageGCS StorageProvider = "gcs"
)

type TaskMode string

const (
	TaskModeFull            TaskMode = "full"
	TaskModeSnapshotOnly    TaskMode = "snapshot-only"
	TaskModeIncrementalOnly TaskMode = "incremental-only"
	TaskModeCloud           TaskMode = "cloud"
)

type TenantConfig struct {
	ID          string
	DisplayName string
}

type TaskIdentity struct {
	ID            string
	Name          string
	CorrelationID string
}

type CredentialRef struct {
	Name string
}

type TiDBSource struct {
	Endpoint      string
	UserRef       CredentialRef
	PasswordRef   CredentialRef
	SSLCAFilePath string
}

type CDCSource struct {
	Endpoint      string
	FlushInterval string
	FileSizeBytes int
}

type SourceTable struct {
	Database         string
	Table            string
	DestinationTable string
}

func (t SourceTable) FQN() string {
	return t.Database + "." + t.Table
}

func (t SourceTable) TargetName() string {
	if t.DestinationTable != "" {
		return t.DestinationTable
	}
	return t.Table
}

type SourceConfig struct {
	TiDB   TiDBSource
	CDC    CDCSource
	Tables []SourceTable
}

type StagingConfig struct {
	Provider StorageProvider
	URI      string
	Prefix   string
}

type SnowflakeDestination struct {
	AccountID   string
	Warehouse   string
	UserRef     CredentialRef
	PasswordRef CredentialRef
	Database    string
	Schema      string
	StagePrefix string
}

type DestinationConfig struct {
	Kind      WarehouseKind
	Snowflake SnowflakeDestination
}

type RuntimeConfig struct {
	Mode                TaskMode
	SnapshotConcurrency int
	MaxParallelTables   int
	AllowSharedState    bool
	UseLegacyLayout     bool
}

type TaskConfig struct {
	Tenant      TenantConfig
	Task        TaskIdentity
	Source      SourceConfig
	Staging     StagingConfig
	Destination DestinationConfig
	Runtime     RuntimeConfig
}

type TablePlan struct {
	SourceDatabase string
	SourceTable    string
	TargetTable    string
	SnapshotStage  string
	IncrementStage string
}

type RuntimePlan struct {
	TenantID          string
	TaskID            string
	CorrelationID     string
	StorageRootURI    string
	SnapshotURI       string
	IncrementURI      string
	CheckpointPrefix  string
	CheckpointURI     string
	SnowflakeDatabase string
	SnowflakeSchema   string
	Tables            []TablePlan
	LogFields         map[string]string
	MetricLabels      map[string]string
}

var idPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$`)

func BuildPlan(cfg TaskConfig) (*RuntimePlan, error) {
	if err := Validate(cfg); err != nil {
		return nil, err
	}

	rootURI, err := scopedStorageRoot(cfg)
	if err != nil {
		return nil, err
	}
	snapshotURI, err := joinURIPath(rootURI, "snapshot")
	if err != nil {
		return nil, err
	}
	incrementURI, err := joinURIPath(rootURI, "increment")
	if err != nil {
		return nil, err
	}
	checkpointURI, err := scopedCheckpointRoot(cfg, rootURI)
	if err != nil {
		return nil, err
	}
	checkpointURI, err = joinURIPath(checkpointURI, "checkpoints")
	if err != nil {
		return nil, err
	}

	correlationID := cfg.Task.CorrelationID
	if correlationID == "" {
		correlationID = cfg.Tenant.ID + "/" + cfg.Task.ID
	}

	schema := cfg.Destination.Snowflake.Schema
	if schema == "" {
		schema = snowflakeIdentifier(cfg.Tenant.ID + "_" + cfg.Task.ID)
	}

	stagePrefix := cfg.Destination.Snowflake.StagePrefix
	if stagePrefix == "" {
		stagePrefix = snowflakeIdentifier(cfg.Tenant.ID + "_" + cfg.Task.ID)
	}

	tables := make([]TablePlan, 0, len(cfg.Source.Tables))
	for _, table := range cfg.Source.Tables {
		stageSuffix := snowflakeIdentifier(table.Database + "_" + table.Table)
		tables = append(tables, TablePlan{
			SourceDatabase: table.Database,
			SourceTable:    table.Table,
			TargetTable:    table.TargetName(),
			SnapshotStage:  stagePrefix + "_SNAPSHOT_" + stageSuffix,
			IncrementStage: stagePrefix + "_INCREMENT_" + stageSuffix,
		})
	}

	plan := &RuntimePlan{
		TenantID:          cfg.Tenant.ID,
		TaskID:            cfg.Task.ID,
		CorrelationID:     correlationID,
		StorageRootURI:    rootURI.String(),
		SnapshotURI:       snapshotURI.String(),
		IncrementURI:      incrementURI.String(),
		CheckpointPrefix:  uriPath(checkpointURI),
		CheckpointURI:     checkpointURI.String(),
		SnowflakeDatabase: cfg.Destination.Snowflake.Database,
		SnowflakeSchema:   schema,
		Tables:            tables,
		LogFields: map[string]string{
			"tenant_id":      cfg.Tenant.ID,
			"task_id":        cfg.Task.ID,
			"correlation_id": correlationID,
		},
		MetricLabels: map[string]string{
			"tenant": cfg.Tenant.ID,
			"task":   cfg.Task.ID,
		},
	}
	return plan, nil
}

func Validate(cfg TaskConfig) error {
	if !idPattern.MatchString(cfg.Tenant.ID) {
		return fmt.Errorf("tenant id must match %s", idPattern.String())
	}
	if !idPattern.MatchString(cfg.Task.ID) {
		return fmt.Errorf("task id must match %s", idPattern.String())
	}
	if cfg.Destination.Kind != WarehouseSnowflake {
		return fmt.Errorf("destination kind %q is not supported", cfg.Destination.Kind)
	}
	if cfg.Destination.Snowflake.Database == "" {
		return fmt.Errorf("snowflake database is required")
	}
	if cfg.Destination.Snowflake.UserRef.Name == "" {
		return fmt.Errorf("snowflake user secret reference is required")
	}
	if cfg.Destination.Snowflake.PasswordRef.Name == "" {
		return fmt.Errorf("snowflake password secret reference is required")
	}
	if cfg.Source.TiDB.Endpoint == "" {
		return fmt.Errorf("tidb endpoint is required")
	}
	if cfg.Source.TiDB.UserRef.Name == "" {
		return fmt.Errorf("tidb user secret reference is required")
	}
	if cfg.Source.TiDB.PasswordRef.Name == "" {
		return fmt.Errorf("tidb password secret reference is required")
	}
	if cfg.Source.CDC.Endpoint == "" {
		return fmt.Errorf("ticdc endpoint is required")
	}
	if len(cfg.Source.Tables) == 0 {
		return fmt.Errorf("at least one source table is required")
	}
	seenSource := map[string]struct{}{}
	seenTarget := map[string]struct{}{}
	for _, table := range cfg.Source.Tables {
		if !idPattern.MatchString(table.Database) || !idPattern.MatchString(table.Table) {
			return fmt.Errorf("source table %q must use simple database.table identifiers", table.FQN())
		}
		sourceKey := strings.ToLower(table.FQN())
		if _, ok := seenSource[sourceKey]; ok {
			return fmt.Errorf("duplicate source table %s", table.FQN())
		}
		seenSource[sourceKey] = struct{}{}

		target := table.TargetName()
		if !idPattern.MatchString(target) {
			return fmt.Errorf("target table %q must use a simple identifier", target)
		}
		targetKey := strings.ToLower(target)
		if _, ok := seenTarget[targetKey]; ok {
			return fmt.Errorf("duplicate target table %s within task %s", target, cfg.Task.ID)
		}
		seenTarget[targetKey] = struct{}{}
	}
	if cfg.Staging.Provider != StorageS3 && cfg.Staging.Provider != StorageGCS {
		return fmt.Errorf("staging provider %q is not supported", cfg.Staging.Provider)
	}
	parsed, err := url.Parse(cfg.Staging.URI)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("staging uri must be an absolute object storage uri")
	}
	if parsed.Scheme != string(cfg.Staging.Provider) && !(cfg.Staging.Provider == StorageGCS && parsed.Scheme == "gs") {
		return fmt.Errorf("staging provider %q does not match uri scheme %q", cfg.Staging.Provider, parsed.Scheme)
	}
	if strings.Contains(cfg.Staging.Prefix, "..") {
		return fmt.Errorf("staging prefix cannot contain path traversal")
	}
	if cfg.Runtime.SnapshotConcurrency < 0 || cfg.Runtime.MaxParallelTables < 0 {
		return fmt.Errorf("runtime concurrency values cannot be negative")
	}
	return nil
}

func NewLegacySnowflakeTask(storageURI string, sourceTables []string, snowflakeDatabase, snowflakeSchema string) TaskConfig {
	cfg := NewSnowflakeTask("default", "default", storageURI, "", sourceTables, snowflakeDatabase, snowflakeSchema, "legacy-cli", "legacy-cli", 0, 0)
	cfg.Runtime.UseLegacyLayout = true
	cfg.Source.TiDB.Endpoint = "legacy-cli"
	cfg.Source.TiDB.UserRef = CredentialRef{Name: "legacy-cli-tidb-user"}
	cfg.Source.TiDB.PasswordRef = CredentialRef{Name: "legacy-cli-tidb-password"}
	cfg.Source.CDC.Endpoint = "legacy-cli"
	cfg.Destination.Snowflake.UserRef = CredentialRef{Name: "legacy-cli-snowflake-user"}
	cfg.Destination.Snowflake.PasswordRef = CredentialRef{Name: "legacy-cli-snowflake-password"}
	return cfg
}

func NewSnowflakeTask(
	tenantID, taskID, storageURI, storagePrefix string,
	sourceTables []string,
	snowflakeDatabase, snowflakeSchema string,
	tidbEndpoint, cdcEndpoint string,
	snapshotConcurrency, maxParallelTables int,
) TaskConfig {
	tables := make([]SourceTable, 0, len(sourceTables))
	for _, table := range sourceTables {
		parts := strings.SplitN(table, ".", 2)
		if len(parts) != 2 {
			continue
		}
		tables = append(tables, SourceTable{Database: parts[0], Table: parts[1]})
	}
	return TaskConfig{
		Tenant: TenantConfig{ID: tenantID},
		Task:   TaskIdentity{ID: taskID},
		Source: SourceConfig{
			TiDB: TiDBSource{
				Endpoint:    tidbEndpoint,
				UserRef:     CredentialRef{Name: "tidb-user"},
				PasswordRef: CredentialRef{Name: "tidb-password"},
			},
			CDC: CDCSource{
				Endpoint: cdcEndpoint,
			},
			Tables: tables,
		},
		Staging: StagingConfig{
			Provider: StorageS3,
			URI:      storageURI,
			Prefix:   storagePrefix,
		},
		Destination: DestinationConfig{
			Kind: WarehouseSnowflake,
			Snowflake: SnowflakeDestination{
				UserRef:     CredentialRef{Name: "snowflake-user"},
				PasswordRef: CredentialRef{Name: "snowflake-password"},
				Database:    snowflakeDatabase,
				Schema:      snowflakeSchema,
			},
		},
		Runtime: RuntimeConfig{
			Mode:                TaskModeFull,
			SnapshotConcurrency: snapshotConcurrency,
			MaxParallelTables:   maxParallelTables,
		},
	}
}

func scopedStorageRoot(cfg TaskConfig) (*url.URL, error) {
	root, err := url.Parse(cfg.Staging.URI)
	if err != nil {
		return nil, err
	}
	prefix := cfg.Staging.Prefix
	if cfg.Runtime.UseLegacyLayout {
		if prefix == "" {
			return root, nil
		}
		return joinURIPath(root, prefix)
	}
	parts := []string{}
	if prefix != "" {
		parts = append(parts, prefix)
	}
	if !cfg.Runtime.AllowSharedState {
		parts = append(parts, "tenants", cfg.Tenant.ID, "tasks", cfg.Task.ID)
	}
	return joinURIPath(root, parts...)
}

func scopedCheckpointRoot(cfg TaskConfig, storageRoot *url.URL) (*url.URL, error) {
	if cfg.Runtime.UseLegacyLayout || !cfg.Runtime.AllowSharedState {
		return storageRoot, nil
	}
	root, err := url.Parse(cfg.Staging.URI)
	if err != nil {
		return nil, err
	}
	parts := []string{}
	if cfg.Staging.Prefix != "" {
		parts = append(parts, cfg.Staging.Prefix)
	}
	parts = append(parts, "tenants", cfg.Tenant.ID, "tasks", cfg.Task.ID)
	return joinURIPath(root, parts...)
}

func joinURIPath(base *url.URL, parts ...string) (*url.URL, error) {
	next := *base
	cleanParts := []string{uriPath(base)}
	for _, part := range parts {
		if part != "" {
			cleanParts = append(cleanParts, part)
		}
	}
	next.Path = path.Join(cleanParts...)
	if strings.HasPrefix(uriPath(base), "/") && !strings.HasPrefix(next.Path, "/") {
		next.Path = "/" + next.Path
	}
	return &next, nil
}

func uriPath(u *url.URL) string {
	if u.Path == "" {
		return "/"
	}
	return u.Path
}

func snowflakeIdentifier(raw string) string {
	var b strings.Builder
	lastUnderscore := false
	for _, r := range strings.ToUpper(raw) {
		valid := (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
		if valid {
			b.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "TASK"
	}
	if len(out) > 128 {
		return out[:128]
	}
	return out
}
