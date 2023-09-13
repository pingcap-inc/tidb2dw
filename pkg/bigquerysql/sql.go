package bigquerysql

import (
	"fmt"
	"strings"

	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
)

var (
	CDC_FLAG_COLUMN_NAME       = "tidb2dw_flag"
	CDC_TABLENAME_COLUMN_NAME  = "tidb2dw_tablename"
	CDC_SCHEMANAME_COLUMN_NAME = "tidb2dw_schemaname"
	CDC_TIMESTAMP_COLUMN_NAME  = "tidb2dw_timestamp"
)

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
