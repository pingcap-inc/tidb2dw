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

	content, err := extStorage.ReadFile(ctx, filePath)
	require.NoError(t, err)
	require.Equal(t, "I,users,test,101,1,alice\n", string(content))
}
