package replicate

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap-inc/tidb2dw/pkg/metrics"
	"github.com/pingcap-inc/tidb2dw/pkg/utils"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	putil "github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

const (
	CSVFileExtension              = ".csv"
	fakePartitionNumForSchemaFile = -1
)

// fileIndexRange defines a range of files. eg. CDC000002.csv ~ CDC000005.csv
type fileIndexRange struct {
	start uint64
	end   uint64
}

type IncrementReplicateSession struct {
	dwConnector     coreinterfaces.Connector
	externalStorage storage.ExternalStorage
	ctx             context.Context
	// tableDMLIdxMap maintains a map of <dmlPathKey, max file index>
	tableDMLIdxMap map[cloudstorage.DmlPathKey]uint64
	// tableDefMap maintains a map of <tableVersion, tableDef>
	tableDefMap map[uint64]*cloudstorage.TableDefinition
	// dataFileMap maintains a map of <dataFilePath, fileSize>
	dataFileMap    map[string]int64
	fileExtension  string
	sourceDatabase string
	sourceTable    string
	tableFQN       string
	logger         *zap.Logger
}

func NewIncrementReplicateSession(
	ctx context.Context,
	dwConnector coreinterfaces.Connector,
	fileExtension string,
	storageURI *url.URL,
	tableFQN string,
	logger *zap.Logger,
) (*IncrementReplicateSession, error) {
	externalStorage, err := putil.GetExternalStorageFromURI(ctx, storageURI.String())
	if err != nil {
		return nil, errors.Trace(err)
	}
	sourceDatabase, sourceTable := utils.SplitTableFQN(tableFQN)
	return &IncrementReplicateSession{
		dwConnector:     dwConnector,
		externalStorage: externalStorage,
		ctx:             ctx,
		tableDMLIdxMap:  make(map[cloudstorage.DmlPathKey]uint64),
		tableDefMap:     make(map[uint64]*cloudstorage.TableDefinition),
		fileExtension:   fileExtension,
		sourceDatabase:  sourceDatabase,
		sourceTable:     sourceTable,
		tableFQN:        tableFQN,
		logger:          logger,
	}, nil
}

func (sess *IncrementReplicateSession) parseDMLFilePath(path string) error {
	var dmlkey cloudstorage.DmlPathKey
	fileIdx, err := dmlkey.ParseDMLFilePath(
		config.DateSeparatorDay.String(),
		path,
	)
	if err != nil {
		return errors.Trace(err)
	}
	if _, ok := sess.tableDMLIdxMap[dmlkey]; !ok || fileIdx >= sess.tableDMLIdxMap[dmlkey] {
		sess.tableDMLIdxMap[dmlkey] = fileIdx
	}
	return nil
}

func (sess *IncrementReplicateSession) parseSchemaFilePath(path string) error {
	var schemaKey cloudstorage.SchemaPathKey
	checksumInFile, err := schemaKey.ParseSchemaFilePath(path)
	if err != nil {
		return errors.Trace(err)
	}
	if _, ok := sess.tableDefMap[schemaKey.TableVersion]; ok {
		// Skip if tableDef already exists.
		return nil
	}
	if schemaKey.Schema != sess.sourceDatabase || schemaKey.Table != sess.sourceTable {
		// ignore schema files do not belong to the current table.
		// This should not happen.
		sess.logger.Error("schema file path not match", zap.String("path", path))
		return nil
	}

	// Read tableDef from schema file and check checksum.
	var tableDef cloudstorage.TableDefinition
	schemaContent, err := sess.externalStorage.ReadFile(sess.ctx, path)
	if err != nil {
		return errors.Trace(err)
	}
	if err = json.Unmarshal(schemaContent, &tableDef); err != nil {
		return errors.Trace(err)
	}
	checksumInMem, err := tableDef.Sum32(nil)
	if err != nil {
		return errors.Trace(err)
	}
	if checksumInMem != checksumInFile || schemaKey.TableVersion != tableDef.TableVersion {
		sess.logger.Error("checksum mismatch",
			zap.Uint32("checksumInMem", checksumInMem),
			zap.Uint32("checksumInFile", checksumInFile),
			zap.Uint64("tableversionInMem", schemaKey.TableVersion),
			zap.Uint64("tableversionInFile", tableDef.TableVersion),
			zap.String("path", path))
		return errors.Errorf("checksum mismatch")
	}

	// Update tableDefMap.
	sess.tableDefMap[tableDef.TableVersion] = &tableDef

	// Fake a dml key for schema.json file, which is useful for putting DDL
	// in front of the DML files when sorting.
	// e.g, for the partitioned table:
	//
	// test/test1/439972354120482843/schema.json					(partitionNum = -1)
	// test/test1/439972354120482843/55/2023-03-09/CDC000001.csv	(partitionNum = 55)
	// test/test1/439972354120482843/66/2023-03-09/CDC000001.csv	(partitionNum = 66)
	//
	// and for the non-partitioned table:
	// test/test2/439972354120482843/schema.json				(partitionNum = -1)
	// test/test2/439972354120482843/2023-03-09/CDC000001.csv	(partitionNum = 0)
	// test/test2/439972354120482843/2023-03-09/CDC000002.csv	(partitionNum = 0)
	//
	// the DDL event recorded in schema.json should be executed first, then the DML events
	// in csv files can be executed.
	dmlkey := cloudstorage.DmlPathKey{
		SchemaPathKey: schemaKey,
		PartitionNum:  fakePartitionNumForSchemaFile,
		Date:          "",
	}
	if _, ok := sess.tableDMLIdxMap[dmlkey]; !ok {
		sess.tableDMLIdxMap[dmlkey] = 0
	} else {
		// duplicate table schema file found, this should not happen.
		sess.logger.Panic("duplicate schema file found",
			zap.String("path", path), zap.Any("tableDef", tableDef),
			zap.Any("schemaKey", schemaKey), zap.Any("dmlkey", dmlkey))
	}
	return nil
}

// map1 - map2
func diffDMLMaps(
	map1, map2 map[cloudstorage.DmlPathKey]uint64,
) map[cloudstorage.DmlPathKey]fileIndexRange {
	resMap := make(map[cloudstorage.DmlPathKey]fileIndexRange)
	for k, v := range map1 {
		if _, ok := map2[k]; !ok {
			resMap[k] = fileIndexRange{
				start: 1,
				end:   v,
			}
		} else if v > map2[k] {
			resMap[k] = fileIndexRange{
				start: map2[k] + 1,
				end:   v,
			}
		}
	}
	return resMap
}

// getNewFiles returns newly created dml files in specific ranges
func (sess *IncrementReplicateSession) getNewFiles() (map[cloudstorage.DmlPathKey]fileIndexRange, error) {
	tableDMLMap := make(map[cloudstorage.DmlPathKey]fileIndexRange)
	origDMLIdxMap := make(map[cloudstorage.DmlPathKey]uint64, len(sess.tableDMLIdxMap))
	for k, v := range sess.tableDMLIdxMap {
		origDMLIdxMap[k] = v
	}
	sess.dataFileMap = make(map[string]int64)
	opt := &storage.WalkOption{SubDir: fmt.Sprintf("%s/%s", sess.sourceDatabase, sess.sourceTable)}
	err := sess.externalStorage.WalkDir(sess.ctx, opt, func(path string, size int64) error {
		if cloudstorage.IsSchemaFile(path) {
			if err := sess.parseSchemaFilePath(path); err != nil {
				sess.logger.Error("failed to parse schema file path", zap.Error(err))
				// skip handling this file
				return nil
			}
		} else if strings.HasSuffix(path, sess.fileExtension) {
			if err := sess.parseDMLFilePath(path); err != nil {
				sess.logger.Error("failed to parse dml file path", zap.Error(err))
				// skip handling this file
				return nil
			}
			if !sess.CheckpointExists(path) {
				sess.dataFileMap[path] = size
				metrics.AddGauge(metrics.IncrementPendingSizeGauge, float64(size), sess.tableFQN)
			}
		} else {
			sess.logger.Debug("ignore handling file", zap.String("path", path))
		}
		return nil
	})
	if err != nil {
		return tableDMLMap, err
	}

	tableDMLMap = diffDMLMaps(sess.tableDMLIdxMap, origDMLIdxMap)
	return tableDMLMap, err
}

func (sess *IncrementReplicateSession) getTableDef(tableVersion uint64) cloudstorage.TableDefinition {
	if td, ok := sess.tableDefMap[tableVersion]; ok {
		return *td
	} else {
		sess.logger.Panic("tableDef not found", zap.Any("table version", tableVersion), zap.Any("tableDefMap", sess.tableDefMap))
		return cloudstorage.TableDefinition{}
	}
}

func (sess *IncrementReplicateSession) CheckpointExists(filePath string) bool {
	checkpointFileName := strings.TrimSuffix(filePath, sess.fileExtension) + ".checkpoint"
	exist, err := sess.externalStorage.FileExists(sess.ctx, checkpointFileName)
	if err != nil {
		return false
	}
	return exist
}

func (sess *IncrementReplicateSession) syncExecDMLEvents(
	tableDef cloudstorage.TableDefinition,
	key cloudstorage.DmlPathKey,
	fileIdx uint64,
) error {
	filePath := key.GenerateDMLFilePath(fileIdx, sess.fileExtension, config.DefaultFileIndexWidth)
	checkpointFileName := strings.TrimSuffix(filePath, sess.fileExtension) + ".checkpoint"

	// check if the file has been loaded into data warehouse
	exist, err := sess.externalStorage.FileExists(sess.ctx, checkpointFileName)
	if err != nil {
		return errors.Annotate(err, "failed to check if checkpoint file exists")
	}
	if exist {
		sess.logger.Info("file has been loaded into data warehouse, just ignore", zap.String("filePath", filePath))
		return nil
	}

	// merge file into data warehouse
	if err := sess.dwConnector.LoadIncrement(tableDef, filePath); err != nil {
		return errors.Trace(err)
	}

	// upload a checkpoint file to indicate that the file has been loaded into data warehouse
	if err := sess.externalStorage.WriteFile(sess.ctx, checkpointFileName, []byte{}); err != nil {
		return errors.Trace(err)
	}

	// update metrics
	metrics.SubGauge(metrics.IncrementPendingSizeGauge, float64(sess.dataFileMap[filePath]), sess.tableFQN)
	metrics.AddCounter(metrics.IncrementLoadedSizeCounter, float64(sess.dataFileMap[filePath]), sess.tableFQN)
	return nil
}

func (sess *IncrementReplicateSession) syncExecDDLEvents(tableDef cloudstorage.TableDefinition) error {
	if len(tableDef.Query) == 0 {
		// schema.json file without query is used to initialize the schema.
		err := sess.dwConnector.InitSchema(tableDef.Columns)
		return errors.Wrap(err, "failed to init schema")
	}

	if err := sess.dwConnector.ExecDDL(tableDef); err != nil {
		// FIXME: if there is a DDL before all the DMLs, will return error here.
		return errors.Annotate(err,
			fmt.Sprintf("Please check the DDL query, "+
				"if necessary, please manually execute the DDL query in data warehouse, "+
				"update the `query` of the %s/%s/%s/meta/schema_%d_{hash}.json to empty, "+
				"and restart the program",
				sess.externalStorage.URI(), tableDef.Schema, tableDef.Table, tableDef.TableVersion))
	}
	metrics.AddCounter(metrics.TableVersionsCounter, float64(tableDef.TableVersion), fmt.Sprintf("%s/%s", sess.sourceDatabase, sess.sourceTable))

	// The following logic is used to handle pause and resume.
	// Keep the current table definition file with len(query) == 0 and delete all the outdated files.

	// Delete all the outdated table definition files.
	for _, item := range sess.tableDefMap {
		if item.TableVersion < tableDef.TableVersion {
			filePath, err := item.GenerateSchemaFilePath()
			if err != nil {
				return errors.Trace(err)
			}
			if err = sess.externalStorage.DeleteFile(sess.ctx, filePath); err != nil {
				return errors.Trace(err)
			}
			delete(sess.tableDefMap, item.TableVersion)
		}
	}
	// clear the query in the current table definition file.
	tableDef.Query = ""
	data, err := tableDef.MarshalWithQuery()
	if err != nil {
		return errors.Trace(err)
	}
	filePath, err := tableDef.GenerateSchemaFilePath()
	if err != nil {
		return errors.Trace(err)
	}
	// update the current table definition file.
	return sess.externalStorage.WriteFile(sess.ctx, filePath, data)
}

func (sess *IncrementReplicateSession) handleNewFiles(dmlFileMap map[cloudstorage.DmlPathKey]fileIndexRange) error {
	keys := make([]cloudstorage.DmlPathKey, 0, len(dmlFileMap))
	for k := range dmlFileMap {
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		sess.logger.Info("no new files found since last round")
		return nil
	}
	slices.SortStableFunc(keys, func(x, y cloudstorage.DmlPathKey) int {
		if r := cmp.Compare(x.TableVersion, y.TableVersion); r != 0 {
			return r
		}
		if r := cmp.Compare(x.PartitionNum, y.PartitionNum); r != 0 {
			return r
		}
		return cmp.Compare(x.Date, y.Date)
	})
	sess.logger.Info("new files found since last round", zap.Any("keys", keys))

	for _, key := range keys {
		tableDef := sess.getTableDef(key.SchemaPathKey.TableVersion)
		// if the key is a fake dml path key which is mainly used for
		// sorting schema.json file before the dml files, which means it is a schema.json file.
		if key.PartitionNum == fakePartitionNumForSchemaFile && len(key.Date) == 0 {
			if err := sess.syncExecDDLEvents(tableDef); err != nil {
				return errors.Trace(err)
			}
			continue
		}

		fileRange := dmlFileMap[key]
		for i := fileRange.start; i <= fileRange.end; i++ {
			if err := sess.syncExecDMLEvents(tableDef, key, i); err != nil {
				return errors.Trace(err)
			}
		}
	}

	return nil
}

func (sess *IncrementReplicateSession) Run(flushInterval time.Duration) error {
	ticker := time.NewTicker(flushInterval)
	for {
		select {
		case <-sess.ctx.Done():
			return sess.ctx.Err()
		case <-ticker.C:
		}
		dmlFileMap, err := sess.getNewFiles()
		if err != nil {
			return errors.Trace(err)
		}

		if err = sess.handleNewFiles(dmlFileMap); err != nil {
			return errors.Trace(err)
		}
	}
}

func StartReplicateIncrement(
	ctx context.Context,
	dwConnector coreinterfaces.Connector,
	tableFQN string,
	storageURI *url.URL,
	flushInterval time.Duration,
) error {
	fileExtension := CSVFileExtension

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// init metric IncrementPendingSizeGauge
	metrics.AddGauge(metrics.IncrementPendingSizeGauge, 0, tableFQN)
	logger := log.L().With(zap.String("table", tableFQN))
	session, err := NewIncrementReplicateSession(ctx, dwConnector, fileExtension, storageURI, tableFQN, logger)
	if err != nil {
		logger.Error("error occurred while creating increment replicate session", zap.Error(err))
		return errors.Trace(err)
	}
	if err = session.Run(flushInterval); err != nil {
		logger.Error("error occurred while running increment replicate session", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}
