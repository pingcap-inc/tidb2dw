package snowsql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"go.uber.org/zap"
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

func GetColumnModifyString(diff *ColumnDiff) (string, error) {
	strs := make([]string, 0, 3)
	if diff.Before.Tp != diff.After.Tp || diff.Before.Precision != diff.After.Precision || diff.Before.Scale != diff.After.Scale {
		colStr, err := GetSnowflakeTypeString(*diff.After)
		if err != nil {
			return "", errors.Trace(err)
		}
		strs = append(strs, fmt.Sprintf("COLUMN %s", colStr))
	}
	if diff.Before.Default != diff.After.Default {
		if diff.After.Default == nil {
			strs = append(strs, fmt.Sprintf("COLUMN %s DROP DEFAULT", diff.After.Name))
		} else {
			log.Warn("Snowflake does not support update column default value", zap.String("column", diff.After.Name), zap.Any("before", diff.Before.Default), zap.Any("after", diff.After.Default))
		}
	}
	if diff.Before.Nullable != diff.After.Nullable {
		if diff.After.Nullable == "true" {
			strs = append(strs, fmt.Sprintf("COLUMN %s DROP NOT NULL", diff.After.Name))
		} else {
			strs = append(strs, fmt.Sprintf("COLUMN %s SET NOT NULL", diff.After.Name))
		}
	}
	return strings.Join(strs, ", "), nil
}

func GenDDLViaColumnsDiff(prevColumns []cloudstorage.TableCol, curTableDef cloudstorage.TableDefinition) ([]string, error) {
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

	columnDiff, err := GetColumnDiff(prevColumns, curTableDef.Columns)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ddls := make([]string, 0, len(columnDiff))
	for _, item := range columnDiff {
		ddl := ""
		switch item.Action {
		case ADD_COLUMN:
			ddl += fmt.Sprintf("ALTER TABLE %s ADD COLUMN ", curTableDef.Table)
			colStr, err := GetSnowflakeColumnString(*item.After)
			if err != nil {
				return nil, errors.Trace(err)
			}
			ddl += colStr
		case DROP_COLUMN:
			ddl += fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", curTableDef.Table, item.Before.Name)
		case MODIFY_COLUMN:
			ddl += fmt.Sprintf("ALTER TABLE %s MODIFY ", curTableDef.Table)
			modifyStr, err := GetColumnModifyString(&item)
			if err != nil {
				return nil, errors.Trace(err)
			}
			ddl += modifyStr
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

func getDefaultString(val interface{}) string {
	_, err := strconv.ParseFloat(fmt.Sprintf("%v", val), 64)
	if err != nil {
		return fmt.Sprintf("'%v'", val) // FIXME: escape
	}
	return fmt.Sprintf("%v", val)
}

// GetSnowflakeColumnString returns a string describing the column in Snowflake, e.g.
// "id INT NOT NULL DEFAULT '0'"
// Refer to:
// https://dev.mysql.com/doc/refman/8.0/en/data-types.html
// https://docs.snowflake.com/en/sql-reference/intro-summary-data-types
func GetSnowflakeColumnString(column cloudstorage.TableCol) (string, error) {
	var sb strings.Builder
	typeStr, err := GetSnowflakeTypeString(column)
	if err != nil {
		return "", errors.Trace(err)
	}
	sb.WriteString(typeStr)
	if column.Nullable == "false" {
		sb.WriteString(" NOT NULL")
	}
	if column.Default != nil {
		sb.WriteString(fmt.Sprintf(` DEFAULT %s`, getDefaultString(column.Default)))
	} else if column.Nullable == "true" {
		sb.WriteString(" DEFAULT NULL")
	}
	return sb.String(), nil
}
