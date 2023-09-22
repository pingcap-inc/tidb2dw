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
	"database/sql"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	_ "github.com/databricks/databricks-sql-go"
)

type DataBricksConfig struct {
	Host     string
	Port     int
	Token    string
	Endpoint string
	Catalog  string
	Schema   string
}

func (config *DataBricksConfig) OpenDB() (*sql.DB, error) {
	var connStr = fmt.Sprintf("token:%s@%s:%d/sql/1.0/endpoints/%s?catalog=%s&schema=%s",
		config.Token, config.Host, config.Port, config.Endpoint, config.Catalog, config.Schema)

	db, err := sql.Open("databricks", connStr)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to open Databricks Warehouse connection")
	}
	// make sure the connection is available
	if err = db.Ping(); err != nil {
		return nil, errors.Annotate(err, "Failed to ping Databricks Warehouse")
	}

	log.Info("Databricks Warehouse connection established")
	return db, nil
}
