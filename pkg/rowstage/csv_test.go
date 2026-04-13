package rowstage

import (
	"context"
	"net/url"
	"testing"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	putil "github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestWriteIncrementFileEmptyRows(t *testing.T) {
	ctx := context.Background()
	storageURI, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	extStorage, err := putil.GetExternalStorageFromURI(ctx, storageURI.String())
	require.NoError(t, err)

	_, err = WriteIncrementFile(extStorage, "test.users", cloudstorage.TableDefinition{
		Schema: "test",
		Table:  "users",
		Columns: []cloudstorage.TableCol{
			{Name: "id", Tp: "BIGINT", IsPK: "true"},
			{Name: "name", Tp: "VARCHAR"},
		},
	}, nil)
	require.EqualError(t, err, "cannot stage empty row batch")
}

func TestWriteIncrementFile(t *testing.T) {
	ctx := context.Background()
	storageURI, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	extStorage, err := putil.GetExternalStorageFromURI(ctx, storageURI.String())
	require.NoError(t, err)

	id := "1"
	name := "alice"
	filePath, err := WriteIncrementFile(extStorage, "test.users", cloudstorage.TableDefinition{
		Schema: "test",
		Table:  "users",
		Columns: []cloudstorage.TableCol{
			{Name: "id", Tp: "BIGINT", IsPK: "true"},
			{Name: "name", Tp: "VARCHAR"},
		},
	}, []coreinterfaces.RowChange{
		{
			Op:       "I",
			CommitTs: 101,
			Columns: map[string]*string{
				"id":   &id,
				"name": &name,
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, "iceberg-stage/test_users/101.csv", filePath)

	content, err := extStorage.ReadFile(ctx, filePath)
	require.NoError(t, err)
	require.Equal(t, "I,users,test,101,1,alice\n", string(content))
}
