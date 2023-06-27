package snowflake

// TODO: refactor this
type Config struct {
	TiDBHost            string
	TiDBPort            int
	TiDBUser            string
	TiDBPass            string
	TiDBSSLCA           string
	SnowflakeAccountId  string
	SnowflakeWarehouse  string
	SnowflakeUser       string
	SnowflakePass       string
	SnowflakeDatabase   string
	SnowflakeSchema     string
	TableFQN            string
	SnapshotConcurrency int
	S3StoragePath       string
	StartTSO            string
}

var configFromCli Config
