package pin14e2e

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/pingcap-inc/tidb2dw/pkg/testfixtures"
	"github.com/stretchr/testify/require"
)

func TestValidateConfigRejectsMissingNamesWithoutSecretValues(t *testing.T) {
	cfg := validConfig(t)
	cfg.SnowflakePassword = ""

	err := ValidateConfig(cfg)

	require.ErrorContains(t, err, "PIN14_SNOWFLAKE_PASSWORD")
	require.NotContains(t, err.Error(), "super-secret")
}

func TestValidateConfigAllowsStorageIntegrationWithoutAccessKeyStageCredentials(t *testing.T) {
	cfg := validConfig(t)
	cfg.AWSAccessKeyID = ""
	cfg.AWSSecretAccessKey = ""

	require.NoError(t, ValidateConfig(cfg))
}

func TestValidateConfigRequiresAccessKeyCredentialsWhenStorageIntegrationMissing(t *testing.T) {
	cfg := validConfig(t)
	cfg.SnowflakeStorageIntegration = ""
	cfg.AWSSecretAccessKey = ""

	err := ValidateConfig(cfg)

	require.ErrorContains(t, err, "AWS_SECRET_ACCESS_KEY")
	require.NotContains(t, err.Error(), "aws-secret")
}

func TestPlanUsesManifestScopesForTenantTaskIsolation(t *testing.T) {
	cfg := validConfig(t)
	manifests, err := LoadManifests(cfg)
	require.NoError(t, err)

	plan, err := Plan(cfg, manifests)

	require.NoError(t, err)
	require.Len(t, plan, 2)
	require.NotEqual(t, plan[0].StorageRootURI, plan[1].StorageRootURI)
	require.Contains(t, plan[0].StorageRootURI, "/pin14/test/tenants/")
	require.Contains(t, plan[0].StorageRootURI, "/scenarios/DDL-V1-001")
	require.NotEmpty(t, plan[0].SnapshotURIs)
	require.NotEmpty(t, plan[0].IncrementURIs)
}

func TestRunnerContinuesWhenOneTenantFails(t *testing.T) {
	cfg := validConfig(t)
	cfg.Mode = ModeRun
	storage := &fakeStorage{}
	sf := &fakeSnowflake{failApplyForTenant: "tenant_a"}
	runner := Runner{Config: cfg, Storage: storage, Snowflake: sf}

	result, err := runner.Run(context.Background())

	require.Error(t, err)
	require.Len(t, result.ScenarioResults, 2)
	require.Equal(t, "failed", result.ScenarioResults[0].Status)
	require.Equal(t, "passed", result.ScenarioResults[1].Status)
	require.Greater(t, len(storage.puts), 0)
	require.Greater(t, result.Cleanup.S3ObjectsDeleted, 0)
	require.NotContains(t, result.Cleanup.S3PrefixesDeleted[0], "bucket")
	require.Equal(t, "s3://<redacted>", result.Cleanup.S3PrefixesDeleted[0])
}

func TestRunnerRunModeWiresConcreteClientsWhenNotInjected(t *testing.T) {
	cfg := validConfig(t)
	cfg.Mode = ModeRun
	var factoryConfig Config
	runner := Runner{
		Config: cfg,
		NewClients: func(actual Config) (StorageClient, SnowflakeClient, error) {
			factoryConfig = actual
			return &fakeStorage{}, &fakeSnowflake{}, nil
		},
	}

	result, err := runner.Run(context.Background())

	require.NoError(t, err)
	require.Equal(t, ModeRun, result.Mode)
	require.Equal(t, cfg.SnowflakeDatabase, factoryConfig.SnowflakeDatabase)
	require.Equal(t, cfg.StorageURI, factoryConfig.StorageURI)
	require.Len(t, result.ScenarioResults, 2)
}

func TestRunnerFailsWhenS3UploadSucceedsButSnowflakeLoadFails(t *testing.T) {
	cfg := validConfig(t)
	cfg.Mode = ModeRun
	runner := Runner{
		Config:    cfg,
		Storage:   &fakeStorage{},
		Snowflake: &fakeSnowflake{loadErr: errors.New("load failed")},
	}

	result, err := runner.Run(context.Background())

	require.ErrorContains(t, err, "load failed")
	require.Equal(t, "failed", result.ScenarioResults[0].Status)
	require.Greater(t, result.Cleanup.S3ObjectsDeleted, 0)
}

func TestRunnerFailsWhenDDLThenDMLFails(t *testing.T) {
	cfg := validConfig(t)
	cfg.Mode = ModeRun
	cfg.Scenarios = []string{"DDL-V1-005"}
	runner := Runner{
		Config:    cfg,
		Storage:   &fakeStorage{},
		Snowflake: &fakeSnowflake{applyErr: errors.New("dml failed after ddl")},
	}

	result, err := runner.Run(context.Background())

	require.ErrorContains(t, err, "dml failed after ddl")
	require.Equal(t, "failed", result.ScenarioResults[0].Status)
	require.Contains(t, result.ScenarioResults[0].Error, "dml failed after ddl")
}

func TestRunnerComparisonFailureIncludesScenarioContext(t *testing.T) {
	cfg := validConfig(t)
	cfg.Mode = ModeRun
	cfg.Scenarios = []string{"DDL-V1-005"}
	runner := Runner{
		Config:    cfg,
		Storage:   &fakeStorage{},
		Snowflake: &fakeSnowflake{checksum: "fnv64a:bad"},
	}

	result, err := runner.Run(context.Background())

	require.ErrorContains(t, err, "scenario=DDL-V1-005")
	require.ErrorContains(t, err, "tenant=tenant_a")
	require.ErrorContains(t, err, "task=task_orders")
	require.ErrorContains(t, err, "table=TENANT_A_TASK_ORDERS_ORDERS")
	require.ErrorContains(t, err, "checksum mismatch")
	require.Equal(t, "failed", result.ScenarioResults[0].Status)
	require.Contains(t, result.ScenarioResults[0].Error, "scenario=DDL-V1-005")
	require.Contains(t, result.ScenarioResults[0].Error, "table=TENANT_A_TASK_ORDERS_ORDERS")
}

func TestCleanupNeverLeavesOwnedScopesUntouched(t *testing.T) {
	cfg := validConfig(t)
	cfg.Mode = ModeRun
	cfg.CleanupMode = CleanupNever
	storage := &fakeStorage{}
	runner := Runner{Config: cfg, Storage: storage, Snowflake: &fakeSnowflake{}}

	result, err := runner.Run(context.Background())

	require.NoError(t, err)
	require.Empty(t, storage.deletedPrefixes)
	require.Empty(t, result.Cleanup.S3PrefixesDeleted)
}

func validConfig(t *testing.T) Config {
	t.Helper()
	dir := t.TempDir()
	_, err := testfixtures.Generate(testfixtures.Options{
		OutputDir:   dir,
		Profile:     testfixtures.ProfileSmoke,
		ScenarioIDs: []string{"DDL-V1-001", "DDL-V1-005"},
		Tenants: []testfixtures.TenantTask{
			{
				TenantID:          "tenant_a",
				TaskID:            "task_orders",
				SourceSchema:      "tidb2dw_tenant_a_task_orders",
				DestinationSchema: "DW_TENANT_A_TASK_ORDERS",
				StoragePrefix:     "tenant_a/task_orders",
				CheckpointPrefix:  "tenant_a/task_orders/checkpoints",
			},
			{
				TenantID:          "tenant_b",
				TaskID:            "task_orders",
				SourceSchema:      "tidb2dw_tenant_b_task_orders",
				DestinationSchema: "DW_TENANT_B_TASK_ORDERS",
				StoragePrefix:     "tenant_b/task_orders",
				CheckpointPrefix:  "tenant_b/task_orders/checkpoints",
			},
		},
		CleanOutput: true,
	})
	require.NoError(t, err)
	return Config{
		Mode:                        ModePlan,
		FixtureDir:                  dir,
		ManifestPath:                filepath.Join(dir, "manifests", "manifest.json"),
		StorageURI:                  "s3://bucket/e2e-root",
		AWSRegion:                   "us-west-2",
		StagingPrefix:               "pin14/test",
		Scenarios:                   []string{"DDL-V1-001"},
		SnowflakeAccountID:          "acct",
		SnowflakeWarehouse:          "wh",
		SnowflakeDatabase:           "PIN14_DB",
		SnowflakeSchema:             "PIN14_SCHEMA",
		SnowflakeStorageIntegration: "PIN14_S3_INT",
		SnowflakeUser:               "runner",
		SnowflakePassword:           "super-secret",
		AWSAccessKeyID:              "aws-key",
		AWSSecretAccessKey:          "aws-secret",
		AWSSessionToken:             "aws-token",
		CleanupMode:                 CleanupAlways,
	}
}

type fakeStorage struct {
	puts            []string
	deletedPrefixes []string
}

func (f *fakeStorage) PutObject(_ context.Context, _, objectURI string) error {
	f.puts = append(f.puts, objectURI)
	return nil
}

func (f *fakeStorage) DeletePrefix(_ context.Context, prefixURI string) (int, error) {
	f.deletedPrefixes = append(f.deletedPrefixes, prefixURI)
	return 2, nil
}

type fakeSnowflake struct {
	loadErr            error
	applyErr           error
	failApplyForTenant string
	checksum           string
}

func (f *fakeSnowflake) PrepareNamespace(context.Context, string, string) error {
	return nil
}

func (f *fakeSnowflake) LoadSnapshot(context.Context, ScenarioRun) error {
	return f.loadErr
}

func (f *fakeSnowflake) ApplyCDC(_ context.Context, scenario ScenarioRun) error {
	if f.failApplyForTenant == scenario.Manifest.Tenant.TenantID {
		return errors.New("tenant dml failed")
	}
	return f.applyErr
}

func (f *fakeSnowflake) FetchValidation(_ context.Context, scenario ScenarioRun) (ActualOutcome, error) {
	checksum := scenario.Manifest.Expected.Checksum
	if f.checksum != "" {
		checksum = f.checksum
	}
	return ActualOutcome{
		SchemaColumns: scenario.Manifest.Expected.FinalSchemaColumns,
		RowCount:      scenario.Manifest.Expected.FinalRowCount,
		Checksum:      checksum,
		SelectedRows:  scenario.Manifest.Expected.SelectedRows,
	}, nil
}

func (f *fakeSnowflake) Cleanup(context.Context, OwnedSnowflakeObjects) (CleanupSnowflakeSummary, error) {
	return CleanupSnowflakeSummary{TablesDropped: 2, StagesDropped: 2}, nil
}
