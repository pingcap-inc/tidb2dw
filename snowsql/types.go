package snowsql

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
)

// TiDB2SnowflakeTypeMap is a map from TiDB type to Snowflake type.
var TiDB2SnowflakeTypeMap map[string]string = map[string]string{
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
	"binary":     "BINARY",
	"varbinary":  "VARBINARY",
	"int":        "INT",
	"mediumint":  "INT",
	"tinyint":    "TINYINT",
	"smallint":   "SMALLINT",
	"bigint":     "BIGINT",
	"float":      "FLOAT",
	"double":     "DOUBLE",
	"decimal":    "DECIMAL",
	"numeric":    "NUMERIC",
	"bool":       "BOOLEAN",
	"boolean":    "BOOLEAN",
	"date":       "DATE",
	"datetime":   "DATETIME",
	"timestamp":  "TIMESTAMP",
	"time":       "TIME",
}

func GetSnowflakeTypeString(column cloudstorage.TableCol) (string, error) {
	tp := strings.ToLower(column.Tp)
	switch tp {
	case "text", "longtext", "mediumtext", "tinytext", "blob", "longblob", "mediumblob", "tinyblob":
		return fmt.Sprintf("%s %s", column.Name, TiDB2SnowflakeTypeMap[tp]), nil
	case "int", "mediumint", "bigint", "tinyint", "smallint", "float", "double", "bool", "boolean", "date":
		return fmt.Sprintf("%s %s", column.Name, TiDB2SnowflakeTypeMap[tp]), nil
	case "varchar", "char", "binary", "varbinary":
		return fmt.Sprintf("%s %s(%s)", column.Name, TiDB2SnowflakeTypeMap[tp], column.Precision), nil
	case "decimal", "numeric":
		return fmt.Sprintf("%s %s(%s, %s)", column.Name, TiDB2SnowflakeTypeMap[tp], column.Precision, column.Scale), nil
	case "datetime", "timestamp", "time":
		return fmt.Sprintf("%s %s(%s)", column.Name, TiDB2SnowflakeTypeMap[tp], column.Precision), nil
	default:
		return "", errors.Errorf("Unsupported data type: %s", column.Tp)
	}
}
