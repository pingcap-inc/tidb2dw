package snowsql_test

import (
	"testing"

	"github.com/pingcap-inc/tidb2dw/snowsql"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/stretchr/testify/require"
)

func TestGenDDLViaColumnsDiff(t *testing.T) {
	prevColumns := []cloudstorage.TableCol{
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
	curTableDef := cloudstorage.TableDefinition{
		Table:  "test_table",
		Schema: "test_schema",
		Columns: []cloudstorage.TableCol{
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
				ID:        6,
				Name:      "gender",
				Tp:        "varchar",
				Precision: "10",
			},
		},
	}

	expectedDDLs := []string{
		"ALTER TABLE test_table MODIFY COLUMN id CHAR(10);",
		"ALTER TABLE test_table RENAME COLUMN name TO color;",
		"ALTER TABLE test_table DROP COLUMN age;",
		"ALTER TABLE test_table ADD COLUMN gender VARCHAR(10);",
	}

	ddl, err := snowsql.GenDDLViaColumnsDiff(prevColumns, curTableDef)
	require.NoError(t, err)
	require.ElementsMatch(t, expectedDDLs, ddl)
}
