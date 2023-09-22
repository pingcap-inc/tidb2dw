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

// GetDatabricksColumnString returns a string describing the column in Databricks, e.g.
// "id INT NOT NULL DEFAULT '0'"
// Refer to:
// https://dev.mysql.com/doc/refman/8.0/en/data-types.html
// https://docs.databricks.com/en/sql/language-manual/sql-ref-datatypes.html
func GetDatabricksColumnString(column cloudstorage.TableCol) (string, error) {
	var sb strings.Builder
	typeStr, err := GetDatabricksTypeString(column)
	if err != nil {
		return "", errors.Trace(err)
	}
	sb.WriteString(fmt.Sprintf("%s %s", column.Name, typeStr))
	if column.Nullable == "false" {
		sb.WriteString(" NOT NULL")
	}
	// Delta table not support default value yet.
	return sb.String(), nil
}
