package bigquerysql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"go.uber.org/zap"
)

func GetColumnModifyString(diff *tidbsql.ColumnDiff) (string, error) {
	strs := make([]string, 0, 3)
	if diff.Before.Tp != diff.After.Tp || diff.Before.Precision != diff.After.Precision || diff.Before.Scale != diff.After.Scale {
		colType, err := GetBigQueryColumnTypeString(*diff.After)
		if err != nil {
			return "", errors.Trace(err)
		}
		// https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_rules
		strs = append(strs, fmt.Sprintf("%s SET DATA TYPE %s", diff.After.Name, colType))
	}
	if diff.Before.Default != diff.After.Default {
		if diff.After.Default == nil {
			strs = append(strs, fmt.Sprintf("%s DROP DEFAULT", diff.After.Name))
		} else {
			strs = append(strs, fmt.Sprintf("%s SET DEFAULT %s", diff.After.Name, getDefaultString(diff.After.Default)))
		}
	}
	if diff.Before.Nullable != diff.After.Nullable {
		if diff.After.Nullable == "true" {
			strs = append(strs, fmt.Sprintf("%s DROP NOT NULL", diff.After.Name))
		} else {
			log.Warn("BigQuery does not support update column required", zap.String("column", diff.After.Name), zap.Any("before", diff.Before.Nullable), zap.Any("after", diff.After.Nullable))
		}
	}
	return strings.Join(strs, ", "), nil
}

func GenDDLViaColumnsDiff(datasetID, tableID string, prevColumns []cloudstorage.TableCol, curTableDef cloudstorage.TableDefinition) ([]string, error) {
	tableFullName := fmt.Sprintf("%s.%s", datasetID, tableID)

	if curTableDef.Type == timodel.ActionTruncateTable {
		return []string{fmt.Sprintf("TRUNCATE TABLE %s", tableFullName)}, nil
	}
	if curTableDef.Type == timodel.ActionDropTable {
		return []string{fmt.Sprintf("DROP TABLE %s", tableFullName)}, nil
	}
	if curTableDef.Type == timodel.ActionCreateTable {
		return nil, errors.New("Received create table ddl, which should not happen") // FIXME: drop table and create table
	}
	if curTableDef.Type == timodel.ActionRenameTables {
		return nil, errors.New("Received rename table ddl, new change data can not be capture by TiCDC any more." +
			"If you want to rename table, please start a new task to capture the new table") // FIXME: rename table to new table and rename back
	}
	if curTableDef.Type == timodel.ActionDropSchema {
		return nil, errors.New("Received drop schema ddl, which does not support") // FIXME: drop schema and create schema
	}
	if curTableDef.Type == timodel.ActionCreateSchema {
		return nil, errors.New("Received create schema ddl, which should not happen") // FIXME: drop schema and create schema
	}

	columnDiff, err := tidbsql.GetColumnDiff(prevColumns, curTableDef.Columns)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ddls := make([]string, 0, len(columnDiff))
	for _, item := range columnDiff {
		ddl := ""
		switch item.Action {
		case tidbsql.ADD_COLUMN:
			ddl += fmt.Sprintf("ALTER TABLE %s ADD COLUMN ", tableFullName)
			colStr, err := GetBigQueryColumnString(*item.After, false)
			if err != nil {
				return nil, errors.Trace(err)
			}
			ddl += colStr + ";"
			ddls = append(ddls, ddl)
			if item.After.Default != nil {
				ddls = append(
					ddls,
					fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;", tableFullName, item.After.Name, getDefaultString(item.After.Default)),
					fmt.Sprintf("UPDATE %s SET %s = %s WHERE TRUE;", tableFullName, item.After.Name, getDefaultString(item.After.Default)),
				)

			} else if item.After.Nullable == "true" {
				ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT NULL;", tableFullName, item.After.Name))
			}
		case tidbsql.DROP_COLUMN:
			ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", tableFullName, item.Before.Name))
		case tidbsql.MODIFY_COLUMN:
			ddl += fmt.Sprintf("ALTER TABLE %s ALTER COLUMN ", tableFullName)
			modifyStr, err := GetColumnModifyString(&item)
			if err != nil {
				return nil, errors.Trace(err)
			}
			ddl += modifyStr + ";"
			ddls = append(ddls, ddl)
		case tidbsql.RENAME_COLUMN:
			ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s;", tableFullName, item.Before.Name, item.After.Name))
		default:
			// UNCHANGE
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

// GetBigQueryColumnString returns a string describing the column in BigQuery, e.g.
// "id INT NOT NULL DEFAULT '0'"
// Refer to:
// https://dev.mysql.com/doc/refman/8.0/en/data-types.html
func GetBigQueryColumnString(column cloudstorage.TableCol, createTable bool) (string, error) {
	var sb strings.Builder
	colType, err := GetBigQueryColumnTypeString(column)
	if err != nil {
		return "", errors.Trace(err)
	}
	sb.WriteString(fmt.Sprintf("%s %s", column.Name, colType))
	if column.Nullable == "false" {
		sb.WriteString(" NOT NULL")
	}
	if createTable {
		if column.Default != nil {
			sb.WriteString(fmt.Sprintf(` DEFAULT %s`, getDefaultString(column.Default)))
		} else if column.Nullable == "true" {
			sb.WriteString(" DEFAULT NULL")
		}
	}
	return sb.String(), nil
}
