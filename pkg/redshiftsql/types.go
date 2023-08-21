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
	"blob":       "TEXT",
	"tinyblob":   "TEXT",
	"mediumblob": "TEXT",
	"longblob":   "TEXT",
	"varchar":    "VARCHAR",
	"char":       "CHAR",
	"binary":     "VARBYTE",
	"varbinary":  "VARBYTE",
	"int":        "INT",
	"mediumint":  "INT",
	"tinyint":    "SMALLINT",
	"smallint":   "SMALLINT",
	"bigint":     "BIGINT",
	"float":      "FLOAT",
	"double":     "FLOAT",
	"decimal":    "DECIMAL",
	"numeric":    "NUMERIC",
	"bool":       "BOOLEAN",
	"boolean":    "BOOLEAN",
	"date":       "DATE",
	"datetime":   "TIMESTAMP",
	"timestamp":  "TIMESTAMP",
	"time":       "TIME",
}

func GetRedshiftTypeString(column cloudstorage.TableCol) (string, error) {
	tp := strings.ToLower(column.Tp)
	switch tp {
	case "text", "longtext", "mediumtext", "tinytext", "blob", "longblob", "mediumblob", "tinyblob":
		return fmt.Sprintf("%s %s", column.Name, TiDB2RedshiftTypeMap[tp]), nil
	case "int", "mediumint", "bigint", "tinyint", "smallint", "float", "double", "bool", "boolean", "date":
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
