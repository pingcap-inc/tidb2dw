package snowsql

import (
	"database/sql"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/snowflakedb/gosnowflake"
)

type SnowflakeConfig struct {
	AccountId string
	Warehouse string
	User      string
	Pass      string
	Database  string
	Schema    string
}

/// Implement the Config interface.

// Open a connection to Snowflake.
func (config *SnowflakeConfig) OpenDB() (*sql.DB, error) {
	sfConfig := gosnowflake.Config{
		Account:   config.AccountId,
		User:      config.User,
		Password:  config.Pass,
		Database:  config.Database,
		Schema:    config.Schema,
		Warehouse: config.Warehouse,
	}
	dsn, err := gosnowflake.DSN(&sfConfig)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to generate Snowflake DSN")
	}
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to open Snowflake connection")
	}
	// make sure the connection is available
	if err = db.Ping(); err != nil {
		return nil, errors.Annotate(err, "Failed to ping Snowflake")
	}
	log.Info("Snowflake connection established")
	return db, nil
}
