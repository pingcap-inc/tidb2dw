package redshiftsql

import (
	"fmt"
	"strings"

	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/pkg/errors"
)

// TiDB2RedshiftTypeMap is a map from TiDB type to Redshift type.
var TiDB2RedshiftTypeMap map[string]string = map[string]string{
	"text":       "TEXT",
	"tinytext":   "TEXT",
	"mediumtext": "TEXT",
	"longtext":   "TEXT",
	"blob":       "VARBYTE",
	"tinyblob":   "VARBYTE",
	// The maximum size of Redshift's VARBYTE type is 100 KB, so can not support mediumblob and longblob.
	// "mediumblob": "TEXT",
	// "longblob":   "TEXT",
	"varchar":            "VARCHAR",
	"char":               "CHAR",
	"binary":             "VARBYTE",
	"varbinary":          "VARBYTE",
	"tinyint":            "SMALLINT",
	"tinyint unsigned":   "SMALLINT",
	"smallint":           "SMALLINT",
	"smallint unsigned":  "INT",
	"int":                "INT",
	"int unsigned":       "BIGINT",
	"mediumint":          "INT",
	"mediumint unsigned": "BIGINT",
	"bigint":             "BIGINT",
	"bigint unsigned":    "DECIMAL(20,0)",
	"float":              "REAL",
	"float unsigned":     "REAL",
	"double":             "DOUBLE PRECISION",
	"double unsigned":    "DOUBLE PRECISION",
	"decimal":            "DECIMAL",
	"numeric":            "NUMERIC",
	"bool":               "BOOLEAN",
	"boolean":            "BOOLEAN",
	"date":               "DATE",
	"datetime":           "TIMESTAMP",
	"timestamp":          "TIMESTAMP",
	"time":               "TIME",
}

// GetRedshiftColumnString returns the column string for Redshift.
// For example, "id INT", "name VARCHAR(255) and "age INT".
func GetRedshiftTypeString(column cloudstorage.TableCol) (string, error) {
	tp := strings.ToLower(column.Tp)
	switch tp {
	case "text", "longtext", "mediumtext", "tinytext":
		return fmt.Sprintf("%s %s", column.Name, TiDB2RedshiftTypeMap[tp]), nil
	case "tinyblob", "blob":
		return fmt.Sprintf("%s %s(%s)", column.Name, TiDB2RedshiftTypeMap[tp], column.Precision), nil
	case "int", "mediumint", "bigint", "tinyint", "smallint", "float", "double", "bool", "boolean", "date":
		return fmt.Sprintf("%s %s", column.Name, TiDB2RedshiftTypeMap[tp]), nil
	case "int unsigned", "mediumint unsigned", "tinyint unsigned", "smallint unsigned", "bigint unsigned", "float unsigned", "double unsigned":
		return fmt.Sprintf("%s %s", column.Name, TiDB2RedshiftTypeMap[tp]), nil
	case "varchar", "char", "binary", "varbinary":
		return fmt.Sprintf("%s %s(%s)", column.Name, TiDB2RedshiftTypeMap[tp], column.Precision), nil
	case "decimal", "numeric":
		return fmt.Sprintf("%s %s(%s, %s)", column.Name, TiDB2RedshiftTypeMap[tp], column.Precision, column.Scale), nil
	case "datetime", "timestamp", "time":
		return fmt.Sprintf("%s %s", column.Name, TiDB2RedshiftTypeMap[tp]), nil
	default:
		return "", errors.Errorf("Unsupported data type: %s", column.Tp)
	}
}
