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
	"blob":       "BINARY",
	"tinyblob":   "BINARY",
	// The maximum size of Snowflake's BINARY type is 8 MB, so can not support mediumblob and longblob.
	// "mediumblob": "TEXT",
	// "longblob":   "TEXT",
	"varchar":            "VARCHAR",
	"char":               "CHAR",
	"binary":             "BINARY",
	"varbinary":          "BINARY",
	"tinyint":            "NUMBER",
	"smallint":           "NUMBER",
	"int":                "NUMBER",
	"mediumint":          "NUMBER",
	"bigint":             "NUMBER",
	"tinyint unsigned":   "NUMBER",
	"smallint unsigned":  "NUMBER",
	"int unsigned":       "NUMBER",
	"mediumint unsigned": "NUMBER",
	"bigint unsigned":    "NUMBER",
	"float":              "FLOAT",
	"float unsigned":     "FLOAT",
	"double":             "FLOAT",
	"double unsigned":    "FLOAT",
	"decimal":            "NUMBER",
	"numeric":            "NUMBER",
	"bool":               "BOOLEAN",
	"boolean":            "BOOLEAN",
	"date":               "DATE",
	"datetime":           "DATETIME",
	"timestamp":          "TIMESTAMP",
	"time":               "TIME",
}

func GetSnowflakeTypeString(column cloudstorage.TableCol) (string, error) {
	tp := strings.ToLower(column.Tp)
	name := QuoteIdentifier(column.Name)
	switch tp {
	case "text", "longtext", "mediumtext", "tinytext":
		return fmt.Sprintf("%s %s", name, TiDB2SnowflakeTypeMap[tp]), nil
	case "tinyblob", "blob":
		if column.Precision == "" {
			return fmt.Sprintf("%s %s", name, TiDB2SnowflakeTypeMap[tp]), nil
		}
		return fmt.Sprintf("%s %s(%s)", name, TiDB2SnowflakeTypeMap[tp], column.Precision), nil
	case "longblob", "mediumblob":
		return "", errors.Errorf("The maximum size of Snowflake's BINARY type is 8 MB, so can not support mediumblob and longblob.")
	case "int", "mediumint", "bigint", "tinyint", "smallint", "float", "double", "bool", "boolean", "date":
		return fmt.Sprintf("%s %s", name, TiDB2SnowflakeTypeMap[tp]), nil
	case "int unsigned", "mediumint unsigned", "tinyint unsigned", "smallint unsigned", "bigint unsigned", "float unsigned", "double unsigned":
		return fmt.Sprintf("%s %s", name, TiDB2SnowflakeTypeMap[tp]), nil
	case "varchar", "char", "binary", "varbinary":
		if column.Precision == "" {
			return fmt.Sprintf("%s %s", name, TiDB2SnowflakeTypeMap[tp]), nil
		}
		return fmt.Sprintf("%s %s(%s)", name, TiDB2SnowflakeTypeMap[tp], column.Precision), nil
	case "decimal", "numeric":
		if column.Precision == "" {
			return fmt.Sprintf("%s %s", name, TiDB2SnowflakeTypeMap[tp]), nil
		}
		if column.Scale == "" {
			return fmt.Sprintf("%s %s(%s)", name, TiDB2SnowflakeTypeMap[tp], column.Precision), nil
		}
		return fmt.Sprintf("%s %s(%s, %s)", name, TiDB2SnowflakeTypeMap[tp], column.Precision, column.Scale), nil
	case "datetime", "timestamp", "time":
		if column.Precision == "" {
			return fmt.Sprintf("%s %s", name, TiDB2SnowflakeTypeMap[tp]), nil
		}
		return fmt.Sprintf("%s %s(%s)", name, TiDB2SnowflakeTypeMap[tp], column.Precision), nil
	default:
		return "", errors.Errorf("Unsupported data type: %s", column.Tp)
	}
}
