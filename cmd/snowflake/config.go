package snowflake

type SnowflakeConfig struct {
	SnowflakeAccountId string
	SnowflakeWarehouse string
	SnowflakeUser      string
	SnowflakePass      string
	SnowflakeDatabase  string
	SnowflakeSchema    string
}

type TiDBConfig struct {
	TiDBHost  string
	TiDBPort  int
	TiDBUser  string
	TiDBPass  string
	TiDBSSLCA string
}

// We always need snowflake config,
// so we can use this as a global variable
var snowflakeConfigFromCli SnowflakeConfig
