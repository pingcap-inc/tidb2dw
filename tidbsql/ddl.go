package tidbsql

import (
	"database/sql"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
)

type columnAction int8

const (
	UNCHANGE   columnAction = iota // 0
	ADD_COLUMN                     // 1
	DROP_COLUMN
	MODIFY_COLUMN
	RENAME_COLUMN
)

type ColumnDiff struct {
	Action columnAction
	Before *cloudstorage.TableCol
	After  *cloudstorage.TableCol
}

func CompareColumn(lhs, rhs *cloudstorage.TableCol) (columnAction, error) {
	if lhs.Name != rhs.Name {
		// In TiDB, when using a single ALTER TABLE statement to alter multiple schema objects (such as columns or indexes) of a table,
		// specifying the same object in multiple changes is not supported.
		// So if the column name is different, the other attributes must be the same
		if lhs.Tp != rhs.Tp || lhs.Default != rhs.Default || lhs.Precision != rhs.Precision || lhs.Scale != rhs.Scale || lhs.Nullable != rhs.Nullable || lhs.IsPK != rhs.IsPK {
			return UNCHANGE, errors.New(fmt.Sprintf("column name %s/%s is different, but other attributes are different too", lhs.Name, rhs.Name))
		}
		return RENAME_COLUMN, nil
	}
	if lhs.Tp != rhs.Tp || lhs.Default != rhs.Default || lhs.Precision != rhs.Precision || lhs.Scale != rhs.Scale || lhs.Nullable != rhs.Nullable || lhs.IsPK != rhs.IsPK {
		return MODIFY_COLUMN, nil
	}
	return UNCHANGE, nil
}

func GetColumnDiff(prev []cloudstorage.TableCol, curr []cloudstorage.TableCol) ([]ColumnDiff, error) {
	// name -> column
	prevNameMap := make(map[string]*cloudstorage.TableCol, len(prev))
	// id -> column
	prevIDMap := make(map[int64]*cloudstorage.TableCol, len(prev))
	for i, item := range prev {
		prevNameMap[item.Name] = &prev[i]
		prevIDMap[item.ID] = &prev[i]
	}
	currNameMap := make(map[string]*cloudstorage.TableCol, len(curr))
	currIDMap := make(map[int64]*cloudstorage.TableCol, len(curr))
	for i, item := range curr {
		currNameMap[item.Name] = &curr[i]
		currIDMap[item.ID] = &curr[i]
	}
	columnDiff := make([]ColumnDiff, 0, len(curr))
	// When prevItem and currItem have the same name, they must be the same column.
	for i, prevItem := range prev {
		if currItem, ok := currNameMap[prevItem.Name]; ok {
			if prevItem.ID != currItem.ID {
				// In TiDB, for `ALTER TABLE t MODIFY COLUMN a char(10);` where a is int(11) before,
				// TiDB will add a tmp column a_$, fill the data, delete the column a, and then rename the column a_$ to a.
				// In this case, the column id will be different.
				columnDiff = append(columnDiff, ColumnDiff{
					Action: MODIFY_COLUMN,
					Before: &prev[i],
					After:  currItem,
				})
				// delete prevItem and currItem from IDMap to avoid duplicate processing
				delete(prevIDMap, prevItem.ID)
				delete(currIDMap, currItem.ID)
			}
		}
	}
	for id, prevItem := range prevIDMap {
		if currItem, ok := currIDMap[id]; !ok {
			// If the column is in the prevMap, and the column is not in the currMap, it means that the column is deleted.
			columnDiff = append(columnDiff, ColumnDiff{
				Action: DROP_COLUMN,
				Before: prevItem,
				After:  nil,
			})
		} else {
			// If the column is in the prevMap and the currMap, it means that the column is modified/rename/unchange.
			action, err := CompareColumn(prevItem, currItem)
			if err != nil {
				return nil, errors.Trace(err)
			}
			columnDiff = append(columnDiff, ColumnDiff{
				Action: action,
				Before: prevItem,
				After:  currItem,
			})
			delete(currIDMap, id)
		}
		delete(prevIDMap, id)
	}
	// Add the remaining columns in currMap are not in prevMap, which means that the columns are added.
	for _, currItem := range currIDMap {
		columnDiff = append(columnDiff, ColumnDiff{
			Action: ADD_COLUMN,
			Before: nil,
			After:  currItem,
		})
	}
	return columnDiff, nil
}

func GetTiDBTableColumn(db *sql.DB, sourceDatabase, sourceTable string) ([]cloudstorage.TableCol, error) {
	columnQuery := fmt.Sprintf(`SELECT COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, 
CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION
FROM information_schema.columns
WHERE table_schema = "%s" AND table_name = "%s"`, sourceDatabase, sourceTable) // FIXME: Escape
	rows, err := db.Query(columnQuery)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// TODO: Confirm with generated column, sequence.
	defer rows.Close()
	tableColumns := make([]cloudstorage.TableCol, 0)
	for rows.Next() {
		var column struct {
			ColumnName    string
			ColumnDefault *string
			IsNullable    string
			DataType      string
			CharMaxLength *int
			NumPrecision  *int
			NumScale      *int
			DateTimePrec  *int
		}
		err = rows.Scan(
			&column.ColumnName,
			&column.ColumnDefault,
			&column.IsNullable,
			&column.DataType,
			&column.CharMaxLength,
			&column.NumPrecision,
			&column.NumScale,
			&column.DateTimePrec,
		)
		if err != nil {
			return nil, errors.Trace(err)
		}
		var precision, scale, nullable string
		if column.NumPrecision != nil {
			precision = fmt.Sprintf("%d", *column.NumPrecision)
		} else if column.DateTimePrec != nil {
			precision = fmt.Sprintf("%d", *column.DateTimePrec)
		} else if column.CharMaxLength != nil {
			precision = fmt.Sprintf("%d", *column.CharMaxLength)
		}
		if column.NumScale != nil {
			scale = fmt.Sprintf("%d", *column.NumScale)
		}
		if column.IsNullable == "YES" {
			nullable = "true"
		} else {
			nullable = "false"
		}
		var defaultVal interface{}
		if column.ColumnDefault != nil {
			defaultVal = *column.ColumnDefault
		}
		tableCol := cloudstorage.TableCol{
			Name:      column.ColumnName,
			Tp:        column.DataType,
			Default:   defaultVal,
			Precision: precision,
			Scale:     scale,
			Nullable:  nullable,
		}
		tableColumns = append(tableColumns, tableCol)
	}
	return tableColumns, nil
}
