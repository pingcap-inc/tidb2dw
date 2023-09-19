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

func testSnowflakeConnection(sfConfig *gosnowflake.Config) (*sql.DB, error) {
	dsn, err := gosnowflake.DSN(sfConfig)
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
	return db, nil
}

/// Implement the Config interface.

// Open a connection to Snowflake.
func (config *SnowflakeConfig) OpenDB() (*sql.DB, error) {
	sfConfig := gosnowflake.Config{
		Account:  config.AccountId,
		User:     config.User,
		Password: config.Pass,
	}
	db, err := testSnowflakeConnection(&sfConfig)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to connect to Snowflake")
	}
	// make sure database exists, if not then create
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS ?", config.Database)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to create database")
	}
	// make sure schema exists, if not then create
	_, err = db.Exec("CREATE SCHEMA IF NOT EXISTS ?.?", config.Database, config.Schema)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to create schema")
	}
	sfConfig.Database = config.Database
	sfConfig.Schema = config.Schema
	sfConfig.Warehouse = config.Warehouse
	db, err = testSnowflakeConnection(&sfConfig)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to connect to Snowflake with database and schema")
	}
	log.Info("Snowflake connection established")
	return db, nil
}
