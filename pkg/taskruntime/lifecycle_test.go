package taskruntime

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestControllerLifecycleAndFailureIsolation(t *testing.T) {
	planA, err := BuildPlan(validTaskConfig("tenant-a", "task-a", "orders"))
	require.NoError(t, err)
	planB, err := BuildPlan(validTaskConfig("tenant-b", "task-b", "orders"))
	require.NoError(t, err)

	controller := NewController(ControllerOptions{MaxRunningTotal: 2, MaxRunningPerTenant: 1})
	_, err = controller.Create(*planA)
	require.NoError(t, err)
	_, err = controller.Create(*planB)
	require.NoError(t, err)

	recordA, err := controller.Start("tenant-a", "task-a")
	require.NoError(t, err)
	require.Equal(t, TaskStatusRunning, recordA.Status)
	recordB, err := controller.Start("tenant-b", "task-b")
	require.NoError(t, err)
	require.Equal(t, TaskStatusRunning, recordB.Status)

	_, err = controller.Fail("tenant-a", "task-a", TaskError{
		Class:     "snowflake_write_failed",
		Message:   "load failed for task-scoped target",
		Retryable: true,
	})
	require.NoError(t, err)

	statusA, err := controller.Status("tenant-a", "task-a")
	require.NoError(t, err)
	require.Equal(t, TaskStatusFailed, statusA.Status)
	require.Equal(t, "snowflake_write_failed", statusA.LastError.Class)
	statusB, err := controller.Status("tenant-b", "task-b")
	require.NoError(t, err)
	require.Equal(t, TaskStatusRunning, statusB.Status)
}

func TestControllerEnforcesTenantConcurrency(t *testing.T) {
	planA, err := BuildPlan(validTaskConfig("tenant-a", "task-a", "orders"))
	require.NoError(t, err)
	planB, err := BuildPlan(validTaskConfig("tenant-a", "task-b", "refunds"))
	require.NoError(t, err)

	controller := NewController(ControllerOptions{MaxRunningPerTenant: 1})
	_, err = controller.Create(*planA)
	require.NoError(t, err)
	_, err = controller.Create(*planB)
	require.NoError(t, err)
	_, err = controller.Start("tenant-a", "task-a")
	require.NoError(t, err)

	_, err = controller.Start("tenant-a", "task-b")
	require.ErrorContains(t, err, "running task limit")
}

func TestControllerResumeUsesSameTaskRecord(t *testing.T) {
	plan, err := BuildPlan(validTaskConfig("tenant-a", "task-a", "orders"))
	require.NoError(t, err)

	controller := NewController(ControllerOptions{})
	created, err := controller.Create(*plan)
	require.NoError(t, err)
	_, err = controller.Start("tenant-a", "task-a")
	require.NoError(t, err)
	_, err = controller.Stop("tenant-a", "task-a")
	require.NoError(t, err)
	resumed, err := controller.Resume("tenant-a", "task-a")
	require.NoError(t, err)

	require.Equal(t, created.CorrelationID, resumed.CorrelationID)
	require.Equal(t, TaskStatusRunning, resumed.Status)
}
