package bigquerysql

import (
	"strings"

	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/pkg/errors"
)

// TiDB2BigQueryTypeMap is a map from TiDB type to BigQuery type.
// Reference: https://cloud.google.com/data-fusion/docs/reference/replication-data-types#mysql
var TiDB2BigQueryTypeMap map[string]string = map[string]string{
	"bigint":             "INT64",
	"bigint unsigned":    "NUMERIC",
	"binary":             "BYTES",
	"bit":                "BOOL",
	"blob":               "BYTES",
	"char":               "STRING",
	"date":               "DATE",
	"datetime":           "DATETIME",
	"decimal":            "NUMERIC",
	"double":             "FLOAT64",
	"float":              "FLOAT64",
	"int":                "INT64",
	"int unsigned":       "INT64",
	"json":               "STRING",
	"longblob":           "BYTES",
	"longtext":           "STRING",
	"mediumblob":         "BYTES",
	"mediumint":          "INT64",
	"mediumint unsigned": "INT64",
	"mediumtext":         "STRING",
	"set":                "STRING",
	"smallint":           "INT64",
	"smallint unsigned":  "INT64",
	"text":               "STRING",
	"time":               "TIME",
	"timestamp":          "TIMESTAMP",
	"tinyblob":           "BYTES",
	"tinyint":            "INT64",
	"tinyint unsigned":   "INT64",
	"tinytext":           "STRING",
	"varbinary":          "BYTES",
	"varchar":            "STRING",
	"year":               "INT64",
}

func GetBigQueryColumnTypeString(column cloudstorage.TableCol) (string, error) {
	tp := strings.ToLower(column.Tp)
	bqType, ok := TiDB2BigQueryTypeMap[tp]
	if !ok {
		return bqType, errors.Errorf("Unsupported TiDB type %s", tp)
	}
	return bqType, nil
}
