package snowsql

import (
	"fmt"

	"github.com/pingcap/errors"
	timodel "github.com/pingcap/tidb/parser/model"
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
	// Use map to find the column with the same id
	prevMap := make(map[int64]*cloudstorage.TableCol, len(prev))
	for i, item := range prev {
		if _, ok := prevMap[item.ID]; !ok {
			prevMap[item.ID] = &prev[i]
		}
	}
	currMap := make(map[int64]*cloudstorage.TableCol, len(curr))
	for i, item := range curr {
		if _, ok := currMap[item.ID]; !ok {
			currMap[item.ID] = &curr[i]
		}
	}
	columnDiff := make([]ColumnDiff, 0, len(curr))
	for i, item := range prev {
		if afterItem, ok := currMap[item.ID]; !ok {
			// If the column is in the prevMap, and the column is not in the currMap, it means that the column is deleted.
			columnDiff = append(columnDiff, ColumnDiff{
				Action: DROP_COLUMN,
				Before: &prev[i],
				After:  nil,
			})
		} else {
			// If the column is in the prevMap and the currMap, it means that the column is modified/rename/unchange.
			action, err := CompareColumn(&item, afterItem)
			if err != nil {
				return nil, errors.Trace(err)
			}
			columnDiff = append(columnDiff, ColumnDiff{
				Action: action,
				Before: &prev[i],
				After:  afterItem,
			})
		}
	}
	for i, item := range curr {
		// If the column is not in the prevMap, and the column is in the currMap, it means that the column is added.
		if _, ok := prevMap[item.ID]; !ok {
			columnDiff = append(columnDiff, ColumnDiff{
				Action: ADD_COLUMN,
				Before: nil,
				After:  &curr[i],
			})
		}
	}
	return columnDiff, nil
}

func GetColumnDetail(col cloudstorage.TableCol) (string, error) {
	tp, err := GetSnowflakeType(col.Tp)
	if err != nil {
		return "", errors.Trace(err)
	}
	res := tp
	if col.Precision != "" {
		res += fmt.Sprintf("(%s", col.Precision)
		if col.Scale != "" {
			res += fmt.Sprintf(",%s", col.Scale)
		}
		res += ")"
	}
	if col.Nullable == "false" {
		res += " NOT NULL"
	}
	if col.Default != nil {
		res += fmt.Sprintf(` DEFAULT '%s'`, col.Default)
	}
	return res, nil
}

func GenDDLViaColumnsDiff(before []cloudstorage.TableCol, curTableDef cloudstorage.TableDefinition) ([]string, error) {
	if curTableDef.Type == timodel.ActionTruncateTable {
		return []string{fmt.Sprintf("TRUNCATE TABLE %s", curTableDef.Table)}, nil
	}
	if curTableDef.Type == timodel.ActionDropTable {
		return []string{fmt.Sprintf("DROP TABLE %s", curTableDef.Table)}, nil
	}
	if curTableDef.Type == timodel.ActionCreateTable {
		return nil, errors.New("Received create table ddl, which should not happen") // FIXME: drop table and create table
	}
	if curTableDef.Type == timodel.ActionRenameTables {
		return nil, errors.New("Received rename table ddl, new change data can not be capture by TiCDC any more." +
			"If you want to rename table, please start a new task to capture the new table") // FIXME: rename table to new table and rename back
	}
	if curTableDef.Type == timodel.ActionDropSchema {
		return []string{fmt.Sprintf("DROP SCHEMA %s", curTableDef.Schema)}, nil
	}
	if curTableDef.Type == timodel.ActionCreateSchema {
		return nil, errors.New("Received create schema ddl, which should not happen") // FIXME: drop schema and create schema
	}

	columnDiff, err := GetColumnDiff(before, curTableDef.Columns)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ddls := make([]string, 0, len(columnDiff))
	for _, item := range columnDiff {
		ddl := ""
		switch item.Action {
		case ADD_COLUMN:
			ddl += fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s ", curTableDef.Table, item.After.Name)
			detail, err := GetColumnDetail(*item.After)
			if err != nil {
				return nil, errors.Trace(err)
			}
			ddl += detail
		case DROP_COLUMN:
			ddl += fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", curTableDef.Table, item.Before.Name)
		case MODIFY_COLUMN:
			ddl += fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s ", curTableDef.Table, item.After.Name)
			detail, err := GetColumnDetail(*item.After)
			if err != nil {
				return nil, errors.Trace(err)
			}
			ddl += detail
		case RENAME_COLUMN:
			ddl += fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", curTableDef.Table, item.Before.Name, item.After.Name)
		default:
			// UNCHANGE
		}
		if ddl != "" {
			ddl += ";"
			ddls = append(ddls, ddl)
		}
	}

	// TODO: handle primary key
	return ddls, nil
}
