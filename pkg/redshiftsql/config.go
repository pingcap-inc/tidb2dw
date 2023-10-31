package redshiftsql

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
)

type RedshiftConfig struct {
	Host     string
	Port     int
	User     string
	Pass     string
	Database string
	Schema   string
}

// Open a connection to Redshift.
// can not specify one schema in redshift
func (config *RedshiftConfig) OpenDB() (*sql.DB, error) {
	var connStr = fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		config.Host, config.Port, config.User, config.Pass, config.Database)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to open Redshift connection")
	}
	// make sure the connection is available
	if err = db.Ping(); err != nil {
		return nil, errors.Annotate(err, "Failed to ping Redshift")
	}
	if err := CreateSchema(db, config.Schema); err != nil {
		return nil, errors.Trace(err)
	}
	log.Info("Redshift connection established")
	return db, nil
}
