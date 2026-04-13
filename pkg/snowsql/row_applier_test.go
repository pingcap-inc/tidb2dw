package snowsql

import (
	"context"
	"net/url"
	"testing"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestSnowflakeRowApplierApplyRowsStagesFile(t *testing.T) {
	storageURI, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	extStorage, err := putil.GetExternalStorageWithDefaultTimeout(context.Background(), storageURI.String())
	require.NoError(t, err)

	var gotPath string
	applier := &SnowflakeRowApplier{
		storage: extStorage,
		loadIncrementFn: func(tableDef cloudstorage.TableDefinition, path string) error {
			gotPath = path
			return nil
		},
	}

	id := "1"
	err = applier.ApplyRows(cloudstorage.TableDefinition{
		Schema: "test",
		Table:  "users",
		Columns: []cloudstorage.TableCol{
			{Name: "id", Tp: "BIGINT", IsPK: "true"},
		},
	}, []coreinterfaces.RowChange{{
		Op:       "I",
		CommitTs: 101,
		Columns: map[string]*string{
			"id": &id,
		},
	}})
	require.NoError(t, err)
	require.NotEmpty(t, gotPath)
}

func TestGenCreateSchemaFromDefinition(t *testing.T) {
	sql, err := GenCreateSchemaFromDefinition(cloudstorage.TableDefinition{
		Schema: "test",
		Table:  "users",
		Columns: []cloudstorage.TableCol{
			{Name: "id", Tp: "BIGINT", IsPK: "true"},
			{Name: "name", Tp: "VARCHAR"},
		},
	})
	require.NoError(t, err)
	require.Contains(t, sql, "CREATE OR REPLACE TABLE users")
	require.Contains(t, sql, "PRIMARY KEY (id)")
}
