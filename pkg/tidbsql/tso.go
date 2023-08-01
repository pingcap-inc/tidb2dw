package tidbsql

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func GetCurrentTSO(config *TiDBConfig) (uint64, error) {
	db, err := config.OpenDB()
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer db.Close()
	row := db.QueryRow("SELECT @@tidb_current_ts")
	var tso uint64
	err = row.Scan(&tso)
	if err != nil {
		return 0, errors.Annotate(err, "failed to get current tso")
	}
	log.Info("Successfully get current tso", zap.Uint64("tso", tso))
	return tso, nil
}
