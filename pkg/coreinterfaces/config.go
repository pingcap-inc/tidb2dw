package coreinterfaces

import "database/sql"

/// Config is the interface for configuration

type Config interface {
	// OpenDB opens a connection to the database
	OpenDB() (*sql.DB, error)
}
