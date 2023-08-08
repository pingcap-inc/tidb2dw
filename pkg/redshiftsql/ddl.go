package redshiftsql

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
)

func getDefaultString(val interface{}) string {
	_, err := strconv.ParseFloat(fmt.Sprintf("%v", val), 64)
	if err != nil {
		return fmt.Sprintf("'%v'", val) // FIXME: escape
	}
	return fmt.Sprintf("%v", val)
}

// GetRedshiftColumnString returns a string describing the column in Redshift, e.g.
// "id INT NOT NULL DEFAULT '0'"
// Refer to:
// https://dev.mysql.com/doc/refman/8.0/en/data-types.html
// https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
func GetRedshiftColumnString(column cloudstorage.TableCol) (string, error) {
	var sb strings.Builder
	typeStr, err := GetRedshiftTypeString(column)
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

// something wrong with the DATA_TYPE
func GetRedshiftTableColumn(db *sql.DB, sourceTable string) ([]cloudstorage.TableCol, error) {
	columnQuery := fmt.Sprintf(`SELECT COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, 
CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION
FROM information_schema.columns
WHERE table_name = '%s'`, sourceTable) // need to replace "" to ''
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
