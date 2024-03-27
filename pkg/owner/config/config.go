package config

type OwnerConfig struct {
	// Log Level
	LogLevel string
	// Server Address
	Host string
	// Server Port
	Port int

	MetaDB MetaDBConfig

	// Owner Lease Duration
	LeaseDuration int
	// Owner Lease Renew Interval
	LeaseRenewInterval int
}

type MetaDBConfig struct {
	// Connect to the metaDB
	Host     string
	Port     int
	Db       string
	User     string
	Password string

	// Max Open Connections
	MaxOpenConns int
	// Max Idle Connections
	MaxIdleConns int
}
