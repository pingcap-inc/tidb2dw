package dumpling

import (
	"context"
	"database/sql"
	"net/url"
	"sync"
	"time"

	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/dumpling/export"
	putil "github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

func buildDumperConfig(
	tidbConfig *tidbsql.TiDBConfig,
	concurrency int,
	storageURI *url.URL,
	snapshotTSO string,
	tableNames []string,
	csvOutputDialect export.CSVDialect,
) (*export.Config, error) {
	conf := export.DefaultConfig()
	conf.Logger = log.L()
	conf.User = tidbConfig.User
	conf.Password = tidbConfig.Pass
	conf.Host = tidbConfig.Host
	conf.Port = tidbConfig.Port
	conf.Threads = concurrency
	conf.NoHeader = true
	conf.FileType = "csv"
	conf.CsvSeparator = ","
	conf.CsvDelimiter = "\""
	conf.EscapeBackslash = false
	conf.TransactionalConsistency = true
	conf.CsvOutputDialect = csvOutputDialect
	// pass any positive integer to `Rows` enable concurrent dumping
	conf.Rows = 1
	conf.OutputDirPath = storageURI.String()
	if snapshotTSO != "0" {
		conf.Snapshot = snapshotTSO
	}

	filesize, err := export.ParseFileSize("5GiB")
	if err != nil {
		return nil, errors.Trace(err)
	}
	conf.FileSize = filesize

	conf.SpecifiedTables = true
	tables, err := export.GetConfTables(tableNames)
	if err != nil {
		return nil, errors.Trace(err) // Should not happen
	}
	conf.Tables = tables

	externalStorage, err := putil.GetExternalStorageFromURI(context.Background(), storageURI.String())
	if err != nil {
		return nil, errors.Trace(err)
	}
	conf.ExtStorage = externalStorage

	return conf, nil
}

func buildDumper(conf *export.Config, db *sql.DB) (*export.Dumper, error) {
	dumper, err := export.NewDumper(context.Background(), conf)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to create dumpling instance")
	}

	_, err = db.ExecContext(context.Background(), "SET SESSION tidb_snapshot = ?", conf.Snapshot)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Info("Using snapshot", zap.String("snapshot", conf.Snapshot))

	return dumper, nil
}

func RunDump(
	tidbConfig *tidbsql.TiDBConfig,
	concurrency int,
	storageURI *url.URL,
	snapshotTSO string,
	tableNames []string,
	csvOutputDialect export.CSVDialect,
	onSnapshotDumpProgress func(dumpedRows, totalRows int64),
) error {
	dumpConfig, err := buildDumperConfig(tidbConfig, concurrency, storageURI, snapshotTSO, tableNames, csvOutputDialect)
	if err != nil {
		return errors.Trace(err)
	}
	db, err := tidbConfig.OpenDB()
	if err != nil {
		return errors.Trace(err)
	}
	defer db.Close()
	dumper, err := buildDumper(dumpConfig, db)
	if err != nil {
		return errors.Trace(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	dumpFinished := make(chan struct{})

	go func() {
		// This is a goroutine to monitor the dump progress.
		defer wg.Done()

		if onSnapshotDumpProgress == nil {
			return
		}

		checkInterval := 10 * time.Second
		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()

		for {
			select {
			case <-dumpFinished:
				return
			case <-ticker.C:
				status := dumper.GetStatus()
				onSnapshotDumpProgress(int64(status.FinishedRows), int64(status.EstimateTotalRows))
			}
		}
	}()

	err = dumper.Dump()
	dumpFinished <- struct{}{}

	wg.Wait()

	_ = dumper.Close()
	if err != nil {
		return errors.Annotate(err, "Failed to dump table from TiDB")
	}
	status := dumper.GetStatus()
	log.Info("Successfully dumped table from TiDB", zap.Any("status", status))
	return nil
}
