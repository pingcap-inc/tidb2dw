package replicate

import (
	"testing"

	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/stretchr/testify/require"
)

func TestIncrementCheckpointPathUsesTaskScopedRoot(t *testing.T) {
	sess := &IncrementReplicateSession{
		fileExtension: CSVFileExtension,
		taskScoped:    true,
	}

	require.Equal(t,
		"increment/sales/orders/100/2026-06-08/CDC000001.checkpoint",
		sess.dmlCheckpointPath("sales/orders/100/2026-06-08/CDC000001.csv"),
	)
}

func TestIncrementCheckpointPathPreservesLegacySidecar(t *testing.T) {
	sess := &IncrementReplicateSession{
		fileExtension: CSVFileExtension,
		taskScoped:    false,
	}

	require.Equal(t,
		"sales/orders/100/2026-06-08/CDC000001.checkpoint",
		sess.dmlCheckpointPath("sales/orders/100/2026-06-08/CDC000001.csv"),
	)
}

func TestDDLCheckpointPathUsesSchemaTableAndVersion(t *testing.T) {
	sess := &IncrementReplicateSession{}

	require.Equal(t,
		"ddl/sales/orders/schema_100.checkpoint",
		sess.ddlCheckpointPath(cloudstorage.TableDefinition{
			Schema:       "sales",
			Table:        "orders",
			TableVersion: 100,
		}),
	)
}
