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
