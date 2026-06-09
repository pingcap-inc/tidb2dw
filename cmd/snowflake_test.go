package cmd

import (
	"testing"

	"github.com/pingcap-inc/tidb2dw/pkg/taskruntime"
	"github.com/stretchr/testify/require"
)

func TestCheckpointURIForIncrementReplayPreservesLegacySidecar(t *testing.T) {
	plan := &taskruntime.RuntimePlan{CheckpointURI: "s3://bucket/root/checkpoints"}

	checkpointURI, err := checkpointURIForIncrementReplay(plan, true)

	require.NoError(t, err)
	require.Nil(t, checkpointURI)
}

func TestCheckpointURIForIncrementReplayUsesTaskScopedRoot(t *testing.T) {
	plan := &taskruntime.RuntimePlan{CheckpointURI: "s3://bucket/root/tenants/tenant-a/tasks/task-a/checkpoints"}

	checkpointURI, err := checkpointURIForIncrementReplay(plan, false)

	require.NoError(t, err)
	require.Equal(t, "s3://bucket/root/tenants/tenant-a/tasks/task-a/checkpoints", checkpointURI.String())
}
