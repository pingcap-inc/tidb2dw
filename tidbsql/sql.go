package tidbsql

import (
	"database/sql"

	"github.com/pingcap/errors"
)

func GetCurrentTSO(db *sql.DB) (uint64, error) {
	row := db.QueryRow("SELECT @@tidb_current_ts")
	var tso uint64
	if err := row.Scan(&tso); err != nil {
		return 0, errors.Annotate(err, "failed to get current tso")
	}
	return tso, nil
}

func EnlargeGCDuration(db *sql.DB) error {
	if _, err := db.Exec("SET GLOBAL tidb_gc_life_time = '720h'"); err != nil {
		return errors.Annotate(err, "failed to set gc life time")
	}
	return nil
}

func ResetGCDuration(db *sql.DB) error {
	if _, err := db.Exec("SET GLOBAL tidb_gc_life_time = '10m'"); err != nil {
		return errors.Annotate(err, "failed to reset gc life time")
	}
	return nil
}
