package icebergconsumer

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	sinkiceberg "github.com/pingcap/ticdc/pkg/sink/iceberg"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
)

const tableDefinitionVersion = 1
const actionRenameColumn timodel.ActionType = 78

func BuildTableDefinition(version *sinkiceberg.TableVersion) (cloudstorage.TableDefinition, error) {
	if version == nil {
		return cloudstorage.TableDefinition{}, fmt.Errorf("table version is nil")
	}

	columns := make([]cloudstorage.TableCol, 0, len(version.Columns))
	for _, column := range version.Columns {
		if column.OriginalTableCol == nil {
			return cloudstorage.TableDefinition{}, fmt.Errorf("column %q (%d) has nil OriginalTableCol", column.Name, column.ID)
		}
		columns = append(columns, cloudstorage.TableCol{
			ID:        column.OriginalTableCol.ID,
			Name:      column.OriginalTableCol.Name,
			Tp:        originalColumnType(column.OriginalTableCol.Tp, column.OriginalTableCol.Elems),
			Default:   column.OriginalTableCol.Default,
			Precision: column.OriginalTableCol.Precision,
			Scale:     column.OriginalTableCol.Scale,
			Nullable:  column.OriginalTableCol.Nullable,
			IsPK:      column.OriginalTableCol.IsPK,
		})
	}

	return cloudstorage.TableDefinition{
		Table:        version.TableName,
		Schema:       version.SchemaName,
		Version:      tableDefinitionVersion,
		TableVersion: uint64(version.MetadataVersion),
		Columns:      columns,
		TotalColumns: len(columns),
	}, nil
}

func BuildDDLDefinitions(prev, curr *sinkiceberg.TableVersion) ([]cloudstorage.TableDefinition, error) {
	if curr == nil {
		return nil, fmt.Errorf("current table version is nil")
	}

	currDef, err := BuildTableDefinition(curr)
	if err != nil {
		return nil, err
	}

	if prev == nil {
		createDef := currDef
		createDef.Type = timodel.ActionCreateTable
		createDef.Query = buildCreateTableQuery(currDef)
		return []cloudstorage.TableDefinition{createDef}, nil
	}

	prevDef, err := BuildTableDefinition(prev)
	if err != nil {
		return nil, err
	}

	prevByID := make(map[int]cloudstorage.TableCol, len(prev.Columns))
	for i, column := range prev.Columns {
		prevByID[column.ID] = prevDef.Columns[i]
	}
	currByID := make(map[int]cloudstorage.TableCol, len(curr.Columns))
	for i, column := range curr.Columns {
		currByID[column.ID] = currDef.Columns[i]
	}

	steps := make([]cloudstorage.TableDefinition, 0)
	for _, column := range prev.Columns {
		prevCol := prevByID[column.ID]
		currCol, ok := currByID[column.ID]
		if !ok {
			steps = append(steps, ddlStep(
				currDef,
				timodel.ActionDropColumn,
				fmt.Sprintf(
					"ALTER TABLE %s DROP COLUMN `%s`",
					qualifiedTableName(currDef),
					prevCol.Name,
				),
			))
			continue
		}

		action, err := tidbsql.CompareColumn(&prevCol, &currCol)
		if err != nil {
			return nil, err
		}
		switch action {
		case tidbsql.RENAME_COLUMN:
			steps = append(steps, ddlStep(
				currDef,
				actionRenameColumn,
				fmt.Sprintf(
					"ALTER TABLE %s RENAME COLUMN `%s` TO `%s`",
					qualifiedTableName(currDef),
					prevCol.Name,
					currCol.Name,
				),
			))
		case tidbsql.MODIFY_COLUMN:
			steps = append(steps, ddlStep(
				currDef,
				timodel.ActionModifyColumn,
				fmt.Sprintf(
					"ALTER TABLE %s MODIFY COLUMN %s",
					qualifiedTableName(currDef),
					columnDefinition(currCol),
				),
			))
		}
	}

	for _, column := range curr.Columns {
		if _, ok := prevByID[column.ID]; ok {
			continue
		}
		currCol := currByID[column.ID]
		steps = append(steps, ddlStep(
			currDef,
			timodel.ActionAddColumn,
			fmt.Sprintf(
				"ALTER TABLE %s ADD COLUMN %s",
				qualifiedTableName(currDef),
				columnDefinition(currCol),
			),
		))
	}

	return steps, nil
}

func ddlStep(base cloudstorage.TableDefinition, action timodel.ActionType, query string) cloudstorage.TableDefinition {
	step := base
	step.Type = action
	step.Query = query
	return step
}

func buildCreateTableQuery(tableDef cloudstorage.TableDefinition) string {
	columnDefs := make([]string, 0, len(tableDef.Columns))
	for _, column := range tableDef.Columns {
		columnDefs = append(columnDefs, columnDefinition(column))
	}
	return fmt.Sprintf(
		"CREATE TABLE %s (%s)",
		qualifiedTableName(tableDef),
		strings.Join(columnDefs, ", "),
	)
}

func qualifiedTableName(tableDef cloudstorage.TableDefinition) string {
	return fmt.Sprintf("`%s`.`%s`", tableDef.Schema, tableDef.Table)
}

func columnDefinition(column cloudstorage.TableCol) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("`%s` %s", column.Name, typeDefinition(column)))
	if column.Nullable == "false" {
		builder.WriteString(" NOT NULL")
	}
	if column.Default != nil {
		builder.WriteString(" DEFAULT ")
		builder.WriteString(defaultValueString(column.Default))
	}
	return builder.String()
}

func typeDefinition(column cloudstorage.TableCol) string {
	if column.Precision == "" && column.Scale == "" {
		return column.Tp
	}
	if column.Precision == "" {
		return fmt.Sprintf("%s(%s)", column.Tp, column.Scale)
	}
	if column.Scale == "" {
		return fmt.Sprintf("%s(%s)", column.Tp, column.Precision)
	}
	return fmt.Sprintf("%s(%s,%s)", column.Tp, column.Precision, column.Scale)
}

func originalColumnType(tp string, elems []string) string {
	if len(elems) == 0 {
		return tp
	}

	quotedElems := make([]string, 0, len(elems))
	for _, elem := range elems {
		quotedElems = append(quotedElems, defaultValueString(elem))
	}
	return fmt.Sprintf("%s(%s)", tp, strings.Join(quotedElems, ","))
}

func defaultValueString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return "'" + strings.ReplaceAll(v, "'", "''") + "'"
	case fmt.Stringer:
		return "'" + strings.ReplaceAll(v.String(), "'", "''") + "'"
	case bool:
		return strconv.FormatBool(v)
	default:
		return fmt.Sprint(v)
	}
}
