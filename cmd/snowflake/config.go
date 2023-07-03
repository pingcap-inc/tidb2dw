package snowflake

import (
	"database/sql"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/snowflakedb/gosnowflake"
)

type SnowflakeConfig struct {
	SnowflakeAccountId string
	SnowflakeWarehouse string
	SnowflakeUser      string
	SnowflakePass      string
	SnowflakeDatabase  string
	SnowflakeSchema    string
}

func OpenSnowflake(config *SnowflakeConfig) (*sql.DB, error) {
	sfConfig := gosnowflake.Config{
		Account:   config.SnowflakeAccountId,
		User:      config.SnowflakeUser,
		Password:  config.SnowflakePass,
		Database:  config.SnowflakeDatabase,
		Schema:    config.SnowflakeSchema,
		Warehouse: config.SnowflakeWarehouse,
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

// We always need snowflake config,
// so we can use this as a global variable
var snowflakeConfigFromCli SnowflakeConfig
