// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
