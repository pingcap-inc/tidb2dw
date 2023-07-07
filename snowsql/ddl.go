package snowsql

import (
	"fmt"
	"regexp"

	"github.com/pingcap/errors"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
)

func IsSnowflakeSupportedDDL(tp timodel.ActionType) bool {
	switch tp {
	// table
	case timodel.ActionCreateTable, timodel.ActionDropTable, timodel.ActionTruncateTable, timodel.ActionRenameTables:
		return true
	// column
	case timodel.ActionAddColumn, timodel.ActionDropColumn, timodel.ActionModifyColumn:
		return true
	// view
	case timodel.ActionCreateView, timodel.ActionDropView:
		return true
	// primary key
	case timodel.ActionAddPrimaryKey, timodel.ActionDropPrimaryKey:
		return true
	// chore
	case timodel.ActionSetDefaultValue, timodel.ActionModifyTableCharsetAndCollate:
		return true
	default:
	}
	return false
}

// use regexp to rewrite ddl
func RewriteDDL(query string) string {
	// remove "index xxx (xxx)"
	query = regexp.MustCompile(`(?i)index\s+.*\(.+\)`).ReplaceAllString(query, "")
	// remove "/*T!xxx */"
	query = regexp.MustCompile(`(?i)/\*T!.*\s+\*/`).ReplaceAllString(query, "")
	// remove `
	query = regexp.MustCompile("`").ReplaceAllString(query, "")
	// replace "AUTO_INCREMENT" with "AUTOINCREMENT"
	query = regexp.MustCompile(`(?i)AUTO_INCREMENT`).ReplaceAllString(query, "AUTOINCREMENT")
	// replace "[tiny|medium|long][text|blob]" with "TEXT"
	query = regexp.MustCompile(`(?i)(tiny|medium|long)(text|blob)`).ReplaceAllString(query, "TEXT")
	// replace "mediumint" with "INT"
	query = regexp.MustCompile(`(?i)mediumint`).ReplaceAllString(query, "INT")
	// replace "var_string" with "VARCHAR"
	query = regexp.MustCompile(`(?i)var_string`).ReplaceAllString(query, "VARCHAR")
	return query
}

type ColumnDiff struct {
	action timodel.ActionType
	before *cloudstorage.TableCol
	after  *cloudstorage.TableCol
}

func CompareColumn(lhs, rhs *cloudstorage.TableCol) bool {
	if lhs.Name != rhs.Name ||
		lhs.Tp != rhs.Tp ||
		lhs.Default != rhs.Default ||
		lhs.Precision != rhs.Precision ||
		lhs.Scale != rhs.Scale ||
		lhs.Nullable != rhs.Nullable ||
		lhs.IsPK != rhs.IsPK {
		return false
	}

	return true
}

func GetColumnDiff(before []cloudstorage.TableCol, after []cloudstorage.TableCol) []ColumnDiff {
	var columnDiff []ColumnDiff
	for _, prev := range before {
		found := false
		for _, next := range after {
			if prev.ID == next.ID {
				if !CompareColumn(&prev, &next) {
					columnDiff = append(columnDiff, ColumnDiff{
						action: timodel.ActionModifyColumn,
						before: &prev,
						after:  &next,
					})
				}
				found = true
				break
			}
		}
		// not found, drop column or rename column
		if !found {
			columnDiff = append(columnDiff, ColumnDiff{
				action: timodel.ActionDropColumn,
				before: &prev,
			})
		}
	}

	return nil
}

func GenDDLViaColumnsDiff(before []cloudstorage.TableCol, curTableDef cloudstorage.TableDefinition) (string, error) {
	if curTableDef.Type == timodel.ActionTruncateTable {
		return fmt.Sprintf("TRUNCATE TABLE %s", curTableDef.Table), nil
	}
	if curTableDef.Type == timodel.ActionDropTable {
		return fmt.Sprintf("DROP TABLE %s", curTableDef.Table), nil
	}
	if curTableDef.Type == timodel.ActionCreateTable {
		return "", errors.New("Received create table ddl, which should not happen") // FIXME: drop table and create table
	}
	if curTableDef.Type == timodel.ActionRenameTables {
		return "", errors.New("Received rename table ddl, new change data can not be capture by TiCDC any more." +
			"If you want to rename table, please start a new task to capture the new table") // FIXME: rename table to new table and rename back
	}
	if curTableDef.Type == timodel.ActionDropSchema {
		return fmt.Sprintf("DROP SCHEMA %s", curTableDef.Schema), nil
	}
	if curTableDef.Type == timodel.ActionCreateSchema {
		return "", errors.New("Received create schema ddl, which should not happen") // FIXME: drop schema and create schema
	}

	return "", nil
}
