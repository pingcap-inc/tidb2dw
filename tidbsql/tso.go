package tidbsql

import (
	"github.com/pingcap/errors"
)

func GetCurrentTSO(config *TiDBConfig) (uint64, error) {
	db, err := OpenTiDB(config)
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
	return tso, nil
}
