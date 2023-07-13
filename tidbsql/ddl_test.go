package tidbsql_test

import (
	"testing"

	"github.com/pingcap-inc/tidb2dw/tidbsql"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/stretchr/testify/require"
)

func TestGetColumnDiff(t *testing.T) {
	prev := []cloudstorage.TableCol{
		{
			ID:        1,
			Name:      "id",
			Tp:        "int",
			Precision: "11",
		},
		{
			ID:   2,
			Name: "name",
			Tp:   "varchar",
		},
		{
			ID:   3,
			Name: "age",
			Tp:   "int",
		},
		{
			ID:   4,
			Name: "birth",
			Tp:   "date",
		},
	}
	curr := []cloudstorage.TableCol{
		{
			ID:        5,
			Name:      "id",
			Tp:        "char",
			Precision: "10",
		},
		{
			ID:   2,
			Name: "color",
			Tp:   "varchar",
		},
		{
			ID:   4,
			Name: "birth",
			Tp:   "date",
		},
		{
			ID:   6,
			Name: "gender",
			Tp:   "varchar",
		},
	}
	expected := []tidbsql.ColumnDiff{
		{
			Action: tidbsql.MODIFY_COLUMN,
			Before: &prev[0],
			After:  &curr[0],
		},
		{
			Action: tidbsql.RENAME_COLUMN,
			Before: &prev[1],
			After:  &curr[1],
		},
		{
			Action: tidbsql.DROP_COLUMN,
			Before: &prev[2],
			After:  nil,
		},
		{
			Action: tidbsql.UNCHANGE,
			Before: &prev[3],
			After:  &curr[2],
		},
		{
			Action: tidbsql.ADD_COLUMN,
			Before: nil,
			After:  &curr[3],
		},
	}
	columnDiff, err := tidbsql.GetColumnDiff(prev, curr)
	require.NoError(t, err)
	require.ElementsMatch(t, expected, columnDiff)
}
