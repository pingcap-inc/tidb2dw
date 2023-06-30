package tidbsql

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"os"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
)

type TiDBConfig struct {
	TiDBHost  string
	TiDBPort  int
	TiDBUser  string
	TiDBPass  string
	TiDBSSLCA string
}

func OpenTiDB(config *TiDBConfig) (*sql.DB, error) {
	tidbConfig := mysql.NewConfig()
	tidbConfig.User = config.TiDBUser
	tidbConfig.Passwd = config.TiDBPass
	tidbConfig.Net = "tcp"
	tidbConfig.Addr = fmt.Sprintf("%s:%d", config.TiDBHost, config.TiDBPort)
	if config.TiDBSSLCA != "" {
		rootCertPool := x509.NewCertPool()
		pem, err := os.ReadFile(config.TiDBSSLCA)
		if err != nil {
			log.Fatal(err.Error())
		}
		if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
			log.Fatal("Failed to append PEM.")
		}
		mysql.RegisterTLSConfig("tidb", &tls.Config{
			RootCAs:    rootCertPool,
			MinVersion: tls.VersionTLS12,
			ServerName: config.TiDBHost,
		})
		tidbConfig.TLSConfig = "tidb"
	}
	db, err := sql.Open("mysql", tidbConfig.FormatDSN())
	if err != nil {
		return nil, errors.Annotate(err, "Failed to open TiDB connection")
	}
	// make sure the connection is available
	if err = db.Ping(); err != nil {
		return nil, errors.Annotate(err, "Failed to ping TiDB")
	}
	log.Info("TiDB connection established")
	return db, nil
}
