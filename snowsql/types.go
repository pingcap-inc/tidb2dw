package snowsql

import (
	"strings"

	"github.com/pingcap/errors"
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

// A safer way to get Snowflake type from TiDB type.
func GetSnowflakeType(t string) (string, error) {
	t = strings.ToLower(t)
	if v, ok := TiDB2SnowflakeTypeMap[t]; ok {
		return v, nil
	}
	return "", errors.Errorf("Unsupported data type: %s", t)
}
