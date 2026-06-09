package snowsql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"go.uber.org/zap"
)

const unsupportedDDLPrefix = "unsupported Snowflake DDL"

// QuoteIdentifier always emits a Snowflake quoted identifier. This keeps TiDB
// names case-preserving and makes escaping deterministic; every Snowflake SQL
// generator in this package must use the same policy for table/column/schema
// identifiers.
func QuoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func unsupportedDDL(tableDef cloudstorage.TableDefinition, reason string) error {
	action := timodel.ActionMap[tableDef.Type]
	if action == "" {
		action = fmt.Sprintf("action %d", tableDef.Type)
	}
	return errors.Errorf("%s: %s for %s.%s is not supported: %s", unsupportedDDLPrefix, action, tableDef.Schema, tableDef.Table, reason)
}

func isMetadataOnlyDDL(tableDef cloudstorage.TableDefinition) bool {
	query := strings.ToLower(tableDef.Query)
	switch tableDef.Type {
	case timodel.ActionAddIndex, timodel.ActionDropIndex, timodel.ActionRenameIndex, timodel.ActionAlterIndexVisibility:
		return !strings.Contains(query, "unique")
	case timodel.ActionAddTablePartition, timodel.ActionReorganizePartition,
		timodel.ActionAlterTablePartitionAttributes, timodel.ActionAlterTablePartitionPlacement,
		timodel.ActionAlterTableAttributes, timodel.ActionAlterTablePlacement:
		return true
	default:
		return false
	}
}

func pkColumns(columns []cloudstorage.TableCol) []string {
	cols := make([]string, 0)
	for _, col := range columns {
		if col.IsPK == "true" {
			cols = append(cols, QuoteIdentifier(col.Name))
		}
	}
	return cols
}

func genCreateTableDDL(tableDef cloudstorage.TableDefinition) (string, error) {
	if len(tableDef.Columns) == 0 {
		return "", unsupportedDDL(tableDef, "create table event has no columns")
	}
	rows := make([]string, 0, len(tableDef.Columns)+1)
	for _, column := range tableDef.Columns {
		row, err := GetSnowflakeColumnString(column)
		if err != nil {
			return "", errors.Trace(err)
		}
		rows = append(rows, row)
	}
	if pk := pkColumns(tableDef.Columns); len(pk) > 0 {
		rows = append(rows, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pk, ", ")))
	}
	for i := range rows {
		rows[i] = "    " + rows[i]
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n);", QuoteIdentifier(tableDef.Table), strings.Join(rows, ",\n")), nil
}

func parsePositiveInt(value string) (int, bool) {
	if value == "" {
		return 0, false
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, false
	}
	return parsed, parsed > 0
}

func validateTypeChange(diff *tidbsql.ColumnDiff) error {
	beforeType := strings.ToLower(diff.Before.Tp)
	afterType := strings.ToLower(diff.After.Tp)
	if beforeType != afterType {
		return errors.Errorf("%s: incompatible type change for column %s from %s to %s", unsupportedDDLPrefix, diff.Before.Name, diff.Before.Tp, diff.After.Tp)
	}

	switch afterType {
	case "varchar", "char", "binary", "varbinary":
		beforePrecision, beforeOK := parsePositiveInt(diff.Before.Precision)
		afterPrecision, afterOK := parsePositiveInt(diff.After.Precision)
		if beforeOK && afterOK && afterPrecision < beforePrecision {
			return errors.Errorf("%s: narrowing column %s from precision %s to %s is not supported", unsupportedDDLPrefix, diff.Before.Name, diff.Before.Precision, diff.After.Precision)
		}
	case "decimal", "numeric":
		beforePrecision, beforePrecisionOK := parsePositiveInt(diff.Before.Precision)
		afterPrecision, afterPrecisionOK := parsePositiveInt(diff.After.Precision)
		beforeScale, beforeScaleOK := parsePositiveInt(diff.Before.Scale)
		afterScale, afterScaleOK := parsePositiveInt(diff.After.Scale)
		if beforePrecisionOK && afterPrecisionOK && afterPrecision < beforePrecision {
			return errors.Errorf("%s: narrowing column %s from precision %s to %s is not supported", unsupportedDDLPrefix, diff.Before.Name, diff.Before.Precision, diff.After.Precision)
		}
		if beforeScaleOK && afterScaleOK && afterScale < beforeScale {
			return errors.Errorf("%s: narrowing column %s from scale %s to %s is not supported", unsupportedDDLPrefix, diff.Before.Name, diff.Before.Scale, diff.After.Scale)
		}
	}
	return nil
}

func GetColumnModifyString(diff *tidbsql.ColumnDiff) (string, error) {
	strs := make([]string, 0, 3)
	if diff.Before.Tp != diff.After.Tp || diff.Before.Precision != diff.After.Precision || diff.Before.Scale != diff.After.Scale {
		if err := validateTypeChange(diff); err != nil {
			return "", errors.Trace(err)
		}
		colStr, err := GetSnowflakeTypeString(*diff.After)
		if err != nil {
			return "", errors.Trace(err)
		}
		strs = append(strs, fmt.Sprintf("COLUMN %s", colStr))
	}
	if diff.Before.Default != diff.After.Default {
		if diff.After.Default == nil {
			strs = append(strs, fmt.Sprintf("COLUMN %s DROP DEFAULT", QuoteIdentifier(diff.After.Name)))
		} else {
			return "", errors.Errorf("%s: changing default for existing column %s is deferred", unsupportedDDLPrefix, diff.After.Name)
		}
	}
	if diff.Before.Nullable != diff.After.Nullable {
		if diff.After.Nullable == "true" {
			strs = append(strs, fmt.Sprintf("COLUMN %s DROP NOT NULL", QuoteIdentifier(diff.After.Name)))
		} else {
			strs = append(strs, fmt.Sprintf("COLUMN %s SET NOT NULL", QuoteIdentifier(diff.After.Name)))
		}
	}
	return strings.Join(strs, ", "), nil
}

func GenDDLViaColumnsDiff(prevColumns []cloudstorage.TableCol, curTableDef cloudstorage.TableDefinition) ([]string, error) {
	if curTableDef.Type == timodel.ActionTruncateTable {
		return []string{fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s;", QuoteIdentifier(curTableDef.Table))}, nil
	}
	if curTableDef.Type == timodel.ActionDropTable {
		return []string{fmt.Sprintf("DROP TABLE IF EXISTS %s;", QuoteIdentifier(curTableDef.Table))}, nil
	}
	if curTableDef.Type == timodel.ActionCreateTable {
		ddl, err := genCreateTableDDL(curTableDef)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return []string{ddl}, nil
	}
	if curTableDef.Type == timodel.ActionRenameTable || curTableDef.Type == timodel.ActionRenameTables {
		return nil, unsupportedDDL(curTableDef, "renamed tables need a new task binding so later writes are captured under the new table name")
	}
	if curTableDef.Type == timodel.ActionDropSchema {
		return []string{fmt.Sprintf("DROP SCHEMA IF EXISTS %s;", QuoteIdentifier(curTableDef.Schema))}, nil
	}
	if curTableDef.Type == timodel.ActionCreateSchema {
		return []string{fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", QuoteIdentifier(curTableDef.Schema))}, nil
	}
	if isMetadataOnlyDDL(curTableDef) {
		log.Info("Snowflake ignores TiDB metadata-only DDL", zap.String("ddl", curTableDef.Query), zap.Any("type", curTableDef.Type))
		return nil, nil
	}
	switch curTableDef.Type {
	case timodel.ActionAddIndex, timodel.ActionDropIndex, timodel.ActionRenameIndex:
		return nil, unsupportedDDL(curTableDef, "constraint semantics are not enforced like TiDB")
	case timodel.ActionAddPrimaryKey, timodel.ActionDropPrimaryKey:
		return nil, unsupportedDDL(curTableDef, "primary key changes after snapshot can invalidate merge keys")
	case timodel.ActionAddForeignKey, timodel.ActionDropForeignKey, timodel.ActionAddCheckConstraint,
		timodel.ActionDropCheckConstraint, timodel.ActionAlterCheckConstraint:
		return nil, unsupportedDDL(curTableDef, "constraint semantics are not enforced like TiDB")
	case timodel.ActionDropTablePartition, timodel.ActionTruncateTablePartition, timodel.ActionExchangeTablePartition:
		return nil, unsupportedDDL(curTableDef, "partition DDL can move or remove data without row-level events")
	case timodel.ActionCreateView, timodel.ActionDropView:
		return nil, unsupportedDDL(curTableDef, "views are outside table replication")
	case timodel.ActionSetDefaultValue:
		return nil, unsupportedDDL(curTableDef, "changing defaults on existing columns is deferred")
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
			ddl += fmt.Sprintf("ALTER TABLE %s ADD COLUMN ", QuoteIdentifier(curTableDef.Table))
			if item.After.Default == nil && item.After.IsPK != "true" {
				ddl += "IF NOT EXISTS "
			}
			colStr, err := GetSnowflakeColumnString(*item.After)
			if err != nil {
				return nil, errors.Trace(err)
			}
			ddl += colStr
		case tidbsql.DROP_COLUMN:
			ddl += fmt.Sprintf("ALTER TABLE %s DROP COLUMN IF EXISTS %s", QuoteIdentifier(curTableDef.Table), QuoteIdentifier(item.Before.Name))
		case tidbsql.MODIFY_COLUMN:
			ddl += fmt.Sprintf("ALTER TABLE %s MODIFY ", QuoteIdentifier(curTableDef.Table))
			modifyStr, err := GetColumnModifyString(&item)
			if err != nil {
				return nil, errors.Trace(err)
			}
			ddl += modifyStr
		case tidbsql.RENAME_COLUMN:
			ddl += fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", QuoteIdentifier(curTableDef.Table), QuoteIdentifier(item.Before.Name), QuoteIdentifier(item.After.Name))
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
		sb.WriteString(fmt.Sprintf(` DEFAULT %s`, GetDefaultString(column.Default)))
	}
	return sb.String(), nil
}
