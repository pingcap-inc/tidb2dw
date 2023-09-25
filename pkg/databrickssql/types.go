package databrickssql

import (
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"strings"
)

// TiDB2DatabricksTypeMap is a map from TiDB type to Databricks type.
// Please refer to https://docs.databricks.com/en/sql/language-manual/sql-ref-datatypes.html
var TiDB2DatabricksTypeMap = map[string]string{
	"text":       "STRING",
	"tinytext":   "STRING",
	"mediumtext": "STRING",
	"longtext":   "STRING",
	"blob":       "STRING",
	"tinyblob":   "STRING",
	"mediumblob": "STRING",
	"longblob":   "STRING",
	"varchar":    "STRING",
	"char":       "STRING",
	"binary":     "BINARY",
	"varbinary":  "BINARY",
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
	"datetime":   "TIMESTAMP_NTZ",
	"timestamp":  "TIMESTAMP",
	"time":       "TIMESTAMP_NTZ",
}

func GetDatabricksTypeString(column cloudstorage.TableCol) (string, error) {
	tp := strings.ToLower(column.Tp)
	switch tp {
	case "decimal", "numeric":
		return fmt.Sprintf("%s(%s, %s)", TiDB2DatabricksTypeMap[tp], column.Precision, column.Scale), nil
	default:
		if databricksTp, exist := TiDB2DatabricksTypeMap[tp]; exist {
			return fmt.Sprintf("%s", databricksTp), nil
		} else {
			return "", errors.Errorf("Unsupported data type: %s", column.Tp)
		}
	}
}
