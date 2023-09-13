package bigquerysql

import (
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/pkg/errors"
)

// TiDB2BigQueryTypeMap is a map from TiDB type to BigQuery type.
// Reference: https://cloud.google.com/data-fusion/docs/reference/replication-data-types#mysql
var TiDB2BigQueryTypeMap map[string]bigquery.FieldType = map[string]bigquery.FieldType{
	"bigint":     bigquery.IntegerFieldType,
	"binary":     bigquery.BytesFieldType,
	"bit":        bigquery.BooleanFieldType,
	"blob":       bigquery.BytesFieldType,
	"char":       bigquery.StringFieldType,
	"date":       bigquery.DateFieldType,
	"datetime":   bigquery.DateTimeFieldType,
	"decimal":    bigquery.NumericFieldType,
	"double":     bigquery.FloatFieldType,
	"float":      bigquery.FloatFieldType,
	"int":        bigquery.IntegerFieldType,
	"json":       bigquery.StringFieldType,
	"longblob":   bigquery.BytesFieldType,
	"longtext":   bigquery.StringFieldType,
	"mediumblob": bigquery.BytesFieldType,
	"mediumint":  bigquery.IntegerFieldType,
	"mediumtext": bigquery.StringFieldType,
	"set":        bigquery.StringFieldType,
	"smallint":   bigquery.IntegerFieldType,
	"text":       bigquery.StringFieldType,
	"time":       bigquery.TimeFieldType,
	"timestamp":  bigquery.TimestampFieldType,
	"tinyblob":   bigquery.BytesFieldType,
	"tinyint":    bigquery.IntegerFieldType,
	"tinytext":   bigquery.StringFieldType,
	"varbinary":  bigquery.BytesFieldType,
	"varchar":    bigquery.StringFieldType,
	"year":       bigquery.IntegerFieldType,
}

func GetBigQueryColumnType(column cloudstorage.TableCol) (bigquery.FieldType, error) {
	tp := strings.ToLower(column.Tp)
	bqType, ok := TiDB2BigQueryTypeMap[tp]
	if !ok {
		return bqType, errors.Errorf("Unsupported TiDB type %s", tp)
	}
	return bqType, nil
}

func GetBigQueryColumnTypeString(column cloudstorage.TableCol) (string, error) {
	colType, err := GetBigQueryColumnType(column)
	if err != nil {
		return "", err
	}

	switch colType {
	case bigquery.FloatFieldType:
		return "FLOAT64", nil
	default:
		return string(colType), nil
	}
}
