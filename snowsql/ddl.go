package snowsql

import (
	"regexp"

	timodel "github.com/pingcap/tidb/parser/model"
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
