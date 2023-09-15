package replicate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
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
	tableDefMap    map[uint64]*cloudstorage.TableDefinition
	fileExtension  string
	sourceDatabase string
	sourceTable    string
	storageURI     *url.URL
	logger         *zap.Logger
}

func NewIncrementReplicateSession(
	ctx context.Context,
	dwConnector coreinterfaces.Connector,
	fileExtension string,
	storageURI *url.URL,
	sourceDatabase string,
	sourceTable string,
	logger *zap.Logger,
) (*IncrementReplicateSession, error) {
	externalStorage, err := putil.GetExternalStorageFromURI(ctx, storageURI.String())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &IncrementReplicateSession{
		dwConnector:     dwConnector,
		externalStorage: externalStorage,
		ctx:             ctx,
		tableDMLIdxMap:  make(map[cloudstorage.DmlPathKey]uint64),
		tableDefMap:     make(map[uint64]*cloudstorage.TableDefinition),
		fileExtension:   fileExtension,
		sourceDatabase:  sourceDatabase,
		sourceTable:     sourceTable,
		storageURI:      storageURI,
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

func (sess *IncrementReplicateSession) GenManifestFile(path string, size int64) error {
	fileName := strings.TrimSuffix(path, sess.fileExtension) + ".manifest"
	content := fmt.Sprintf("{\"entries\":[{\"url\":\"%s%s\",\"mandatory\":true, \"meta\": { \"content_length\": %d } }]}", sess.externalStorage.URI(), path, size)
	sess.externalStorage.WriteFile(sess.ctx, fileName, []byte(content))
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
	opt := &storage.WalkOption{SubDir: fmt.Sprintf("%s/%s", sess.sourceDatabase, sess.sourceTable)}

	origDMLIdxMap := make(map[cloudstorage.DmlPathKey]uint64, len(sess.tableDMLIdxMap))
	for k, v := range sess.tableDMLIdxMap {
		origDMLIdxMap[k] = v
	}

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
			// generate manifest file for each dml file
			manifestFileName := strings.TrimSuffix(path, sess.fileExtension) + ".manifest"
			exist, err := sess.externalStorage.FileExists(sess.ctx, manifestFileName)
			if err != nil {
				return err
			}
			if !exist {
				if err = sess.GenManifestFile(path, size); err != nil {
					return nil
				}
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

func (sess *IncrementReplicateSession) syncExecDMLEvents(
	tableDef cloudstorage.TableDefinition,
	key cloudstorage.DmlPathKey,
	fileIdx uint64,
) error {
	filePath := key.GenerateDMLFilePath(fileIdx, sess.fileExtension, config.DefaultFileIndexWidth)
	exist, err := sess.externalStorage.FileExists(sess.ctx, filePath)
	if err != nil {
		return errors.Trace(err)
	}
	// We will remove the file after flush complete, so if the program restarts,
	// the file range will start from 1 again, but the file may not exist.
	// So we just ignore the non-exist file.
	if !exist {
		sess.logger.Warn("file not exists", zap.String("path", filePath))
		return nil
	}

	// merge file into data warehouse
	if err := sess.dwConnector.LoadIncrement(tableDef, sess.storageURI, filePath); err != nil {
		return errors.Trace(err)
	}

	// delete file after merge complete in order to avoid duplicate merge when program restarts
	if err = sess.externalStorage.DeleteFile(sess.ctx, filePath); err != nil {
		return errors.Trace(err)
	}
	// delete manifest file after merge complete
	manifestFilePath := strings.TrimSuffix(filePath, sess.fileExtension) + ".manifest"
	if err = sess.externalStorage.DeleteFile(sess.ctx, manifestFilePath); err != nil {
		return errors.Trace(err)
	}

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
	slices.SortStableFunc(keys, func(x, y cloudstorage.DmlPathKey) bool {
		if x.TableVersion != y.TableVersion {
			return x.TableVersion < y.TableVersion
		}
		if x.PartitionNum != y.PartitionNum {
			return x.PartitionNum < y.PartitionNum
		}
		return x.Date < y.Date
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

func (sess *IncrementReplicateSession) Run(flushInterval time.Duration, wg *sync.WaitGroup) error {
	defer wg.Done()

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

func (sess *IncrementReplicateSession) Close() {
	if sess.dwConnector != nil {
		sess.dwConnector.Close()
	}
}

func StartReplicateIncrement(
	dwConnectorMap map[string]coreinterfaces.Connector,
	storageURI *url.URL,
	flushInterval time.Duration,
) error {
	fileExtension := CSVFileExtension
	if storageURI.Scheme == "gcs" {
		log.Error("Skip replicating increment. GCS does not supprt data warehouse connector now...")
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup
	for tableFQN, dwConnector := range dwConnectorMap {
		go func(tableFQN string, dwConnector coreinterfaces.Connector) {
			logger := log.L().With(zap.String("table", tableFQN))
			sourceDatabase, sourceTable := utils.SplitTableFQN(tableFQN)
			session, err := NewIncrementReplicateSession(ctx, dwConnector, fileExtension, storageURI, sourceDatabase, sourceTable, logger)
			if err != nil {
				logger.Error("error occurred while creating increment replicate session", zap.Error(err), zap.String("tableFQN", tableFQN))
				return
			}
			if err = session.Run(flushInterval, &wg); err != nil {
				logger.Error("error occurred while running increment replicate session", zap.Error(err), zap.String("tableFQN", tableFQN))
				return
			}
		}(tableFQN, dwConnector)
	}

	// Wait for the termination signal
	<-sigCh

	// Cancel the context to stop the goroutines
	cancel()

	// Wait for all the goroutines to exit
	wg.Wait()
	return nil
}
