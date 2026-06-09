package snowsql_test

import (
	"strings"
	"testing"

	"github.com/pingcap-inc/tidb2dw/pkg/snowsql"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/stretchr/testify/require"
)

func TestGenDDLViaColumnsDiffSupportedColumnEvolution(t *testing.T) {
	prevColumns := []cloudstorage.TableCol{
		{ID: "1", Name: "id", Tp: "bigint", IsPK: "true", Nullable: "false"},
		{ID: "2", Name: "status", Tp: "varchar", Precision: "32", Nullable: "true"},
		{ID: "3", Name: "amount", Tp: "decimal", Precision: "18", Scale: "2", Nullable: "true"},
		{ID: "4", Name: "legacy", Tp: "int", Nullable: "true"},
	}
	curTableDef := cloudstorage.TableDefinition{
		Table:  "orders",
		Schema: "ddl_v1_src",
		Columns: []cloudstorage.TableCol{
			{ID: "1", Name: "id", Tp: "bigint", IsPK: "true", Nullable: "false"},
			{ID: "2", Name: "state", Tp: "varchar", Precision: "32", Nullable: "true"},
			{ID: "3", Name: "amount", Tp: "decimal", Precision: "20", Scale: "4", Nullable: "false"},
			{ID: "5", Name: "coupon_code", Tp: "varchar", Precision: "64", Nullable: "true"},
			{ID: "6", Name: "source", Tp: "varchar", Precision: "16", Nullable: "false", Default: "web"},
		},
	}

	expectedDDLs := []string{
		`ALTER TABLE "orders" RENAME COLUMN "status" TO "state";`,
		`ALTER TABLE "orders" MODIFY COLUMN "amount" NUMBER(20, 4), COLUMN "amount" SET NOT NULL;`,
		`ALTER TABLE "orders" DROP COLUMN IF EXISTS "legacy";`,
		`ALTER TABLE "orders" ADD COLUMN IF NOT EXISTS "coupon_code" VARCHAR(64);`,
		`ALTER TABLE "orders" ADD COLUMN "source" VARCHAR(16) NOT NULL DEFAULT 'web';`,
	}

	ddl, err := snowsql.GenDDLViaColumnsDiff(prevColumns, curTableDef)
	require.NoError(t, err)
	require.ElementsMatch(t, expectedDDLs, ddl)
}

func TestGenDDLViaColumnsDiffTableLifecycle(t *testing.T) {
	createTableDef := cloudstorage.TableDefinition{
		Table:  "refunds",
		Schema: "ddl_v1_src",
		Type:   timodel.ActionCreateTable,
		Columns: []cloudstorage.TableCol{
			{ID: "1", Name: "id", Tp: "bigint", IsPK: "true", Nullable: "false"},
			{ID: "2", Name: "order_id", Tp: "bigint", Nullable: "true"},
			{ID: "3", Name: "comment", Tp: "varchar", Precision: "64", Nullable: "true", Default: "O'Reilly"},
		},
	}

	ddl, err := snowsql.GenDDLViaColumnsDiff(nil, createTableDef)
	require.NoError(t, err)
	require.Equal(t, []string{strings.Join([]string{
		`CREATE TABLE IF NOT EXISTS "refunds" (`,
		`    "id" NUMBER NOT NULL,`,
		`    "order_id" NUMBER,`,
		`    "comment" VARCHAR(64) DEFAULT 'O''Reilly',`,
		`    PRIMARY KEY ("id")`,
		`);`,
	}, "\n")}, ddl)

	dropDDL, err := snowsql.GenDDLViaColumnsDiff(createTableDef.Columns, cloudstorage.TableDefinition{
		Table:  "refunds",
		Schema: "ddl_v1_src",
		Type:   timodel.ActionDropTable,
	})
	require.NoError(t, err)
	require.Equal(t, []string{`DROP TABLE IF EXISTS "refunds";`}, dropDDL)

	truncateDDL, err := snowsql.GenDDLViaColumnsDiff(createTableDef.Columns, cloudstorage.TableDefinition{
		Table:  "refunds",
		Schema: "ddl_v1_src",
		Type:   timodel.ActionTruncateTable,
	})
	require.NoError(t, err)
	require.Equal(t, []string{`TRUNCATE TABLE IF EXISTS "refunds";`}, truncateDDL)
}

func TestGenDDLViaColumnsDiffUnsupportedScenarios(t *testing.T) {
	prevColumns := []cloudstorage.TableCol{
		{ID: "1", Name: "id", Tp: "bigint", IsPK: "true", Nullable: "false"},
		{ID: "2", Name: "status", Tp: "varchar", Precision: "128", Nullable: "true", Default: "old"},
	}

	tests := []struct {
		name     string
		tableDef cloudstorage.TableDefinition
		wantErr  string
	}{
		{
			name: "narrow varchar",
			tableDef: cloudstorage.TableDefinition{
				Table:  "orders",
				Schema: "ddl_v1_src",
				Columns: []cloudstorage.TableCol{
					{ID: "1", Name: "id", Tp: "bigint", IsPK: "true", Nullable: "false"},
					{ID: "2", Name: "status", Tp: "varchar", Precision: "8", Nullable: "true", Default: "old"},
				},
			},
			wantErr: "narrowing column status",
		},
		{
			name: "change default",
			tableDef: cloudstorage.TableDefinition{
				Table:  "orders",
				Schema: "ddl_v1_src",
				Columns: []cloudstorage.TableCol{
					{ID: "1", Name: "id", Tp: "bigint", IsPK: "true", Nullable: "false"},
					{ID: "2", Name: "status", Tp: "varchar", Precision: "128", Nullable: "true", Default: "new"},
				},
			},
			wantErr: "changing default",
		},
		{
			name: "incompatible unsupported type change",
			tableDef: cloudstorage.TableDefinition{
				Table:  "orders",
				Schema: "ddl_v1_src",
				Columns: []cloudstorage.TableCol{
					{ID: "1", Name: "id", Tp: "bigint", IsPK: "true", Nullable: "false"},
					{ID: "2", Name: "status", Tp: "json", Nullable: "true"},
				},
			},
			wantErr: "incompatible type change",
		},
		{
			name: "unsupported added type",
			tableDef: cloudstorage.TableDefinition{
				Table:  "orders",
				Schema: "ddl_v1_src",
				Columns: []cloudstorage.TableCol{
					{ID: "1", Name: "id", Tp: "bigint", IsPK: "true", Nullable: "false"},
					{ID: "2", Name: "status", Tp: "varchar", Precision: "128", Nullable: "true", Default: "old"},
					{ID: "3", Name: "payload", Tp: "json", Nullable: "true"},
				},
			},
			wantErr: "Unsupported data type: json",
		},
		{
			name: "rename table",
			tableDef: cloudstorage.TableDefinition{
				Table:  "orders_archive",
				Schema: "ddl_v1_src",
				Type:   timodel.ActionRenameTable,
			},
			wantErr: "renamed tables need a new task binding",
		},
		{
			name: "primary key change",
			tableDef: cloudstorage.TableDefinition{
				Table:  "orders",
				Schema: "ddl_v1_src",
				Type:   timodel.ActionAddPrimaryKey,
			},
			wantErr: "primary key changes",
		},
		{
			name: "data-moving partition",
			tableDef: cloudstorage.TableDefinition{
				Table:  "orders",
				Schema: "ddl_v1_src",
				Type:   timodel.ActionTruncateTablePartition,
			},
			wantErr: "partition DDL can move or remove data",
		},
		{
			name: "unique index",
			tableDef: cloudstorage.TableDefinition{
				Table:  "orders",
				Schema: "ddl_v1_src",
				Type:   timodel.ActionAddIndex,
				Query:  "ALTER TABLE orders ADD UNIQUE INDEX idx_status(status)",
			},
			wantErr: "constraint semantics",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := snowsql.GenDDLViaColumnsDiff(prevColumns, tt.tableDef)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestGenDDLViaColumnsDiffMetadataOnlyNoOps(t *testing.T) {
	prevColumns := []cloudstorage.TableCol{
		{ID: "1", Name: "id", Tp: "bigint", IsPK: "true", Nullable: "false"},
		{ID: "2", Name: "status", Tp: "varchar", Precision: "32", Nullable: "true"},
	}
	tableDef := cloudstorage.TableDefinition{
		Table:   "orders",
		Schema:  "ddl_v1_src",
		Type:    timodel.ActionAddIndex,
		Query:   "ALTER TABLE orders ADD INDEX idx_status(status)",
		Columns: prevColumns,
	}

	ddl, err := snowsql.GenDDLViaColumnsDiff(prevColumns, tableDef)
	require.NoError(t, err)
	require.Empty(t, ddl)
}

func TestSnowflakeIdentifierAndMergeQuoting(t *testing.T) {
	require.Equal(t, `"weird""name"`, snowsql.QuoteIdentifier(`weird"name`))

	tableDef := cloudstorage.TableDefinition{
		Table: "Orders",
		Columns: []cloudstorage.TableCol{
			{Name: "id", Tp: "bigint", IsPK: "true"},
			{Name: "gross_amount", Tp: "decimal", Precision: "20", Scale: "4"},
		},
	}

	mergeSQL := snowsql.GenMergeInto(tableDef, "ddl_v1_src/Orders/1/2026-06-08/CDC000001.csv", "stage1")
	require.Contains(t, mergeSQL, `MERGE INTO "Orders" AS T`)
	require.Contains(t, mergeSQL, `$5 AS "id"`)
	require.Contains(t, mergeSQL, `T."id" = S."id"`)
	require.Contains(t, mergeSQL, `INSERT ("id", "gross_amount") VALUES (S."id", S."gross_amount")`)
}
