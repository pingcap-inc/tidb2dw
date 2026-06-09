package taskruntime

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildPlanIsolatesTenantTaskState(t *testing.T) {
	cfgA := validTaskConfig("tenant-a", "task-1", "orders")
	cfgB := validTaskConfig("tenant-b", "task-1", "orders")

	planA, err := BuildPlan(cfgA)
	require.NoError(t, err)
	planB, err := BuildPlan(cfgB)
	require.NoError(t, err)

	require.NotEqual(t, planA.StorageRootURI, planB.StorageRootURI)
	require.NotEqual(t, planA.SnapshotURI, planB.SnapshotURI)
	require.NotEqual(t, planA.IncrementURI, planB.IncrementURI)
	require.NotEqual(t, planA.CheckpointPrefix, planB.CheckpointPrefix)
	require.NotEqual(t, planA.CheckpointURI, planB.CheckpointURI)
	require.NotEqual(t, planA.SnowflakeSchema, planB.SnowflakeSchema)
	require.NotEqual(t, planA.Tables[0].SnapshotStage, planB.Tables[0].SnapshotStage)
	require.Equal(t, "tenant-a", planA.LogFields["tenant_id"])
	require.Equal(t, "task-1", planA.MetricLabels["task"])
	require.NotContains(t, strings.Join(mapValues(planA.LogFields), " "), "password")
}

func TestBuildPlanKeepsCheckpointIsolatedWhenStateSharingIsExplicit(t *testing.T) {
	cfg := validTaskConfig("tenant-a", "task-1", "orders")
	cfg.Runtime.AllowSharedState = true

	plan, err := BuildPlan(cfg)
	require.NoError(t, err)

	require.Equal(t, "s3://bucket/root", plan.StorageRootURI)
	require.Equal(t, "s3://bucket/root/tenants/tenant-a/tasks/task-1/checkpoints", plan.CheckpointURI)
}

func TestValidateRejectsInvalidConfigBeforePlan(t *testing.T) {
	cfg := validTaskConfig("tenant-a", "task-1", "orders")
	cfg.Staging.URI = "s3://bucket/root"
	cfg.Staging.Prefix = "../other"

	_, err := BuildPlan(cfg)
	require.ErrorContains(t, err, "path traversal")
}

func TestValidateRejectsMissingTiCDCEndpointBeforePlan(t *testing.T) {
	cfg := validTaskConfig("tenant-a", "task-1", "orders")
	cfg.Source.CDC.Endpoint = ""

	_, err := BuildPlan(cfg)
	require.ErrorContains(t, err, "ticdc endpoint is required")
}

func TestValidateRejectsDuplicateTargetsWithinTask(t *testing.T) {
	cfg := validTaskConfig("tenant-a", "task-1", "orders")
	cfg.Source.Tables = append(cfg.Source.Tables, SourceTable{Database: "sales", Table: "refunds", DestinationTable: "orders"})

	_, err := BuildPlan(cfg)
	require.ErrorContains(t, err, "duplicate target table")
}

func TestLegacySnowflakeTaskPreservesStorageLayout(t *testing.T) {
	cfg := NewLegacySnowflakeTask("s3://bucket/existing-prefix", []string{"sales.orders"}, "DW", "PUBLIC")

	plan, err := BuildPlan(cfg)
	require.NoError(t, err)

	require.Equal(t, "s3://bucket/existing-prefix", plan.StorageRootURI)
	require.Equal(t, "s3://bucket/existing-prefix/snapshot", plan.SnapshotURI)
	require.Equal(t, "s3://bucket/existing-prefix/increment", plan.IncrementURI)
	require.Equal(t, "PUBLIC", plan.SnowflakeSchema)
}

func TestSnowflakeTaskUsesTenantTaskScopedStorageByDefault(t *testing.T) {
	cfg := NewSnowflakeTask(
		"tenant-a",
		"task-orders",
		"s3://bucket/root",
		"deploy-a",
		[]string{"sales.orders"},
		"DW",
		"",
		"tidb:4000",
		"ticdc:8300",
		4,
		1,
	)

	plan, err := BuildPlan(cfg)
	require.NoError(t, err)

	require.Equal(t, "s3://bucket/root/deploy-a/tenants/tenant-a/tasks/task-orders", plan.StorageRootURI)
	require.Equal(t, "s3://bucket/root/deploy-a/tenants/tenant-a/tasks/task-orders/snapshot", plan.SnapshotURI)
	require.Equal(t, "s3://bucket/root/deploy-a/tenants/tenant-a/tasks/task-orders/increment", plan.IncrementURI)
	require.Equal(t, "s3://bucket/root/deploy-a/tenants/tenant-a/tasks/task-orders/checkpoints", plan.CheckpointURI)
	require.Equal(t, "TENANT_A_TASK_ORDERS", plan.SnowflakeSchema)
	require.Contains(t, plan.Tables[0].SnapshotStage, "TENANT_A_TASK_ORDERS_SNAPSHOT")
	require.Contains(t, plan.Tables[0].IncrementStage, "TENANT_A_TASK_ORDERS_INCREMENT")
}

func validTaskConfig(tenantID, taskID, tableName string) TaskConfig {
	return TaskConfig{
		Tenant: TenantConfig{ID: tenantID},
		Task:   TaskIdentity{ID: taskID},
		Source: SourceConfig{
			TiDB: TiDBSource{
				Endpoint:    "tidb:4000",
				UserRef:     CredentialRef{Name: "tidb-user"},
				PasswordRef: CredentialRef{Name: "tidb-password"},
			},
			CDC: CDCSource{
				Endpoint: "ticdc:8300",
			},
			Tables: []SourceTable{
				{Database: "sales", Table: tableName},
			},
		},
		Staging: StagingConfig{
			Provider: StorageS3,
			URI:      "s3://bucket/root",
		},
		Destination: DestinationConfig{
			Kind: WarehouseSnowflake,
			Snowflake: SnowflakeDestination{
				UserRef:     CredentialRef{Name: "sf-user"},
				PasswordRef: CredentialRef{Name: "sf-password"},
				Database:    "DW",
			},
		},
		Runtime: RuntimeConfig{
			Mode:                TaskModeFull,
			SnapshotConcurrency: 4,
			MaxParallelTables:   2,
		},
	}
}

func mapValues(m map[string]string) []string {
	values := make([]string, 0, len(m))
	for _, value := range m {
		values = append(values, value)
	}
	return values
}
