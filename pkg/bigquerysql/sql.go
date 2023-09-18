package bigquerysql

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
)

var (
	CDC_FLAG_COLUMN_NAME       = "tidb2dw_flag"
	CDC_TABLENAME_COLUMN_NAME  = "tidb2dw_tablename"
	CDC_SCHEMANAME_COLUMN_NAME = "tidb2dw_schemaname"
	CDC_TIMESTAMP_COLUMN_NAME  = "tidb2dw_timestamp"
)

func GenIncrementTableColumns(columns []cloudstorage.TableCol) []cloudstorage.TableCol {
	return append([]cloudstorage.TableCol{
		{
			Name: CDC_FLAG_COLUMN_NAME,
			Tp:   "varchar",
		},
		{
			Name: CDC_TABLENAME_COLUMN_NAME,
			Tp:   "varchar",
		},
		{
			Name: CDC_SCHEMANAME_COLUMN_NAME,
			Tp:   "varchar",
		},
		{
			// is timestamp?
			Name: CDC_TIMESTAMP_COLUMN_NAME,
			Tp:   "varchar",
		},
	}, columns...)
}

func GenMergeInto(tableDef cloudstorage.TableDefinition, datasetID, tableID, externalTableID string) string {
	pkColumn := make([]string, 0)
	onStat := make([]string, 0)
	for _, col := range tableDef.Columns {
		if col.IsPK == "true" {
			pkColumn = append(pkColumn, col.Name)
			onStat = append(onStat, fmt.Sprintf(`T.%s = S.%s`, col.Name, col.Name))
		}
	}

	updateStat := make([]string, 0, len(tableDef.Columns))
	for _, col := range tableDef.Columns {
		updateStat = append(updateStat, fmt.Sprintf(`%s = S.%s`, col.Name, col.Name))
	}

	insertStat := make([]string, 0, len(tableDef.Columns))
	for _, col := range tableDef.Columns {
		insertStat = append(insertStat, col.Name)
	}

	valuesStat := make([]string, 0, len(tableDef.Columns))
	for _, col := range tableDef.Columns {
		valuesStat = append(valuesStat, fmt.Sprintf(`S.%s`, col.Name))
	}

	mergeSQL := fmt.Sprintf(
		`MERGE INTO %s AS T USING
	(
		SELECT * EXCEPT(row_num)
		FROM (
			SELECT
				*, row_number() over (partition by %s order by %s desc) as row_num
			FROM %s
		)
		WHERE row_num = 1
	) AS S
	ON
	(
		%s
	)
	WHEN MATCHED AND S.%s != 'D' THEN UPDATE SET %s
	WHEN MATCHED AND S.%s = 'D' THEN DELETE
	WHEN NOT MATCHED AND S.%s != 'D' THEN INSERT (%s) VALUES (%s);`,
		fmt.Sprintf("`%s.%s`", datasetID, tableID),
		strings.Join(pkColumn, ", "),
		CDC_TIMESTAMP_COLUMN_NAME,
		fmt.Sprintf("`%s.%s`", datasetID, externalTableID),
		strings.Join(onStat, " AND "),
		CDC_FLAG_COLUMN_NAME,
		strings.Join(updateStat, ", "),
		CDC_FLAG_COLUMN_NAME,
		CDC_FLAG_COLUMN_NAME,
		strings.Join(insertStat, ", "),
		strings.Join(valuesStat, ", "),
	)

	return mergeSQL
}

func GenCreateSchema(columns []cloudstorage.TableCol, pkColumns []string, datasetID, tableID string) (string, error) {
	columnRows := make([]string, 0, len(columns))
	for _, column := range columns {
		row, err := GetBigQueryColumnString(column)
		if err != nil {
			return "", errors.Trace(err)
		}
		columnRows = append(columnRows, row)
	}

	sqlRows := make([]string, 0, len(columnRows)+1)
	sqlRows = append(sqlRows, columnRows...)
	if len(pkColumns) > 0 {
		sqlRows = append(sqlRows, fmt.Sprintf("PRIMARY KEY (%s) NOT ENFORCED", strings.Join(pkColumns, ", ")))
	}
	// Add idents
	for i := 0; i < len(sqlRows); i++ {
		sqlRows[i] = fmt.Sprintf("    %s", sqlRows[i])
	}

	sql := []string{}
	sql = append(sql, fmt.Sprintf(`CREATE OR REPLACE TABLE %s.%s (`, datasetID, tableID)) // TODO: Escape
	sql = append(sql, strings.Join(sqlRows, ",\n"))
	sql = append(sql, ")")

	return strings.Join(sql, "\n"), nil
}
