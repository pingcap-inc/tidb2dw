package databrickssql

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
)

// TiDB2DatabricksTypeMap is a map from TiDB type to Databricks type.
// Please refer to https://docs.databricks.com/en/sql/language-manual/sql-ref-datatypes.html
// Databricks don't support the `BINARY` type in the external table with the CSV file which are `tidb2dw` used.
// So we drop the `binary` and `varbinary` type in the map.
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
			return databricksTp, nil
		} else {
			return "", errors.Errorf("Unsupported data type: %s", column.Tp)
		}
	}
}
