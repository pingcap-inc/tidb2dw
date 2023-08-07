package redshiftsql

import (
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
