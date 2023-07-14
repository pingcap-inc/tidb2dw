package snowflake

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap-inc/tidb2dw/snowsql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	sinkutil "github.com/pingcap/tiflow/cdc/sink/util"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/quotes"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	putil "github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

const (
	fakePartitionNumForSchemaFile = -1
)

// fileIndexRange defines a range of files. eg. CDC000002.csv ~ CDC000005.csv
type fileIndexRange struct {
	start uint64
	end   uint64
}

type consumer struct {
	sfConfig *snowsql.SnowflakeConfig

	replicationCfg  *config.ReplicaConfig
	externalStorage storage.ExternalStorage
	fileExtension   string
	// tableDMLIdxMap maintains a map of <dmlPathKey, max file index>
	tableDMLIdxMap map[cloudstorage.DmlPathKey]uint64
	// tableDefMap maintains a map of <`schema`.`table`, tableDef slice sorted by TableVersion>
	tableDefMap      map[string]map[uint64]*cloudstorage.TableDefinition
	tableIDGenerator *fakeTableIDGenerator
	errCh            chan error
	// snowflakeConnectorMap maintains a map of <TableID, snowflakeConnector>, each table has a snowflakeConnector
	snowflakeConnectorMap map[model.TableID]*snowsql.SnowflakeConnector
	awsCredential         *credentials.Value // aws credential, resolved from current env
	sinkURI               *url.URL
}

func newConsumer(ctx context.Context, sfConfig *snowsql.SnowflakeConfig, sinkUri *url.URL, configFile, timezone string, credential *credentials.Value) (*consumer, error) {
	tz, err := putil.GetTimezone(timezone)
	if err != nil {
		return nil, errors.Annotate(err, "can not load timezone")
	}
	ctx = contextutil.PutTimezoneInCtx(ctx, tz)
	replicaConfig := config.GetDefaultReplicaConfig()
	if len(configFile) > 0 {
		err := util.StrictDecodeFile(configFile, "storage consumer", replicaConfig)
		if err != nil {
			log.Error("failed to decode config file", zap.Error(err))
			return nil, err
		}
	}

	if err = replicaConfig.ValidateAndAdjust(sinkUri); err != nil {
		log.Error("failed to validate replica config", zap.Error(err))
		return nil, err
	}

	switch putil.GetOrZero(replicaConfig.Sink.Protocol) {
	case config.ProtocolCsv.String():
	case config.ProtocolCanalJSON.String():
	default:
		return nil, fmt.Errorf(
			"data encoded in protocol %s is not supported yet",
			putil.GetOrZero(replicaConfig.Sink.Protocol),
		)
	}

	protocol, err := config.ParseSinkProtocolFromString(putil.GetOrZero(replicaConfig.Sink.Protocol))
	if err != nil {
		return nil, err
	}
	extension := sinkutil.GetFileExtension(protocol)

	storage, err := putil.GetExternalStorageFromURI(ctx, sinkUri.String())
	if err != nil {
		log.Error("failed to create external storage", zap.Error(err))
		return nil, err
	}

	errCh := make(chan error, 1)
	return &consumer{
		sfConfig:        sfConfig,
		replicationCfg:  replicaConfig,
		externalStorage: storage,
		fileExtension:   extension,
		errCh:           errCh,
		tableDMLIdxMap:  make(map[cloudstorage.DmlPathKey]uint64),
		tableDefMap:     make(map[string]map[uint64]*cloudstorage.TableDefinition),
		// tableSinkMap:    make(map[model.TableID]tablesink.TableSink),
		tableIDGenerator: &fakeTableIDGenerator{
			tableIDs: make(map[string]int64),
		},
		snowflakeConnectorMap: make(map[model.TableID]*snowsql.SnowflakeConnector),
		awsCredential:         credential,
		sinkURI:               sinkUri,
	}, nil
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
func (c *consumer) getNewFiles(
	ctx context.Context,
) (map[cloudstorage.DmlPathKey]fileIndexRange, error) {
	tableDMLMap := make(map[cloudstorage.DmlPathKey]fileIndexRange)
	opt := &storage.WalkOption{SubDir: ""}

	origDMLIdxMap := make(map[cloudstorage.DmlPathKey]uint64, len(c.tableDMLIdxMap))
	for k, v := range c.tableDMLIdxMap {
		origDMLIdxMap[k] = v
	}

	err := c.externalStorage.WalkDir(ctx, opt, func(path string, size int64) error {
		if cloudstorage.IsSchemaFile(path) {
			err := c.parseSchemaFilePath(ctx, path)
			if err != nil {
				log.Error("failed to parse schema file path", zap.Error(err))
				// skip handling this file
				return nil
			}
		} else if strings.HasSuffix(path, c.fileExtension) {
			err := c.parseDMLFilePath(ctx, path)
			if err != nil {
				log.Error("failed to parse dml file path", zap.Error(err))
				// skip handling this file
				return nil
			}
		} else {
			log.Debug("ignore handling file", zap.String("path", path))
		}
		return nil
	})
	if err != nil {
		return tableDMLMap, err
	}

	tableDMLMap = diffDMLMaps(c.tableDMLIdxMap, origDMLIdxMap)
	return tableDMLMap, err
}

func (c *consumer) waitTableFlushComplete(
	ctx context.Context,
	tableID model.TableID,
	filePath string,
	tableDef cloudstorage.TableDefinition,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-c.errCh:
		return err
	default:
	}

	if err := c.snowflakeConnectorMap[tableID].MergeFile(tableDef, c.sinkURI, filePath); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *consumer) syncExecDMLEvents(
	ctx context.Context,
	tableDef cloudstorage.TableDefinition,
	tableID int64,
	key cloudstorage.DmlPathKey,
	fileIdx uint64,
) error {
	filePath := key.GenerateDMLFilePath(fileIdx, c.fileExtension, config.DefaultFileIndexWidth)
	exist, err := c.externalStorage.FileExists(ctx, filePath)
	if err != nil {
		return errors.Trace(err)
	}
	// We will remove the file after flush complete, so if the program restarts,
	// the file range will start from 1 again, but the file may not exist.
	// So we just ignore the non-exist file.
	if !exist {
		log.Warn("file not exists", zap.String("path", filePath))
		return nil
	}

	if err = c.waitTableFlushComplete(ctx, tableID, filePath, tableDef); err != nil {
		return errors.Trace(err)
	}

	// delete file after flush complete in order to avoid duplicate flush
	if err = c.externalStorage.DeleteFile(ctx, filePath); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (c *consumer) parseDMLFilePath(_ context.Context, path string) error {
	var dmlkey cloudstorage.DmlPathKey
	fileIdx, err := dmlkey.ParseDMLFilePath(
		putil.GetOrZero(c.replicationCfg.Sink.DateSeparator),
		path,
	)
	if err != nil {
		return errors.Trace(err)
	}
	if _, ok := c.tableDMLIdxMap[dmlkey]; !ok || fileIdx >= c.tableDMLIdxMap[dmlkey] {
		c.tableDMLIdxMap[dmlkey] = fileIdx
	}
	return nil
}

func (c *consumer) parseSchemaFilePath(ctx context.Context, path string) error {
	var schemaKey cloudstorage.SchemaPathKey
	checksumInFile, err := schemaKey.ParseSchemaFilePath(path)
	if err != nil {
		return errors.Trace(err)
	}
	key := schemaKey.GetKey()
	if tableDefs, ok := c.tableDefMap[key]; ok {
		if _, ok := tableDefs[schemaKey.TableVersion]; ok {
			// Skip if tableDef already exists.
			return nil
		}
	} else {
		c.tableDefMap[key] = make(map[uint64]*cloudstorage.TableDefinition)
	}

	// Read tableDef from schema file and check checksum.
	var tableDef cloudstorage.TableDefinition
	schemaContent, err := c.externalStorage.ReadFile(ctx, path)
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
		log.Panic("checksum mismatch",
			zap.Uint32("checksumInMem", checksumInMem),
			zap.Uint32("checksumInFile", checksumInFile),
			zap.Uint64("tableversionInMem", schemaKey.TableVersion),
			zap.Uint64("tableversionInFile", tableDef.TableVersion),
			zap.String("path", path))
	}

	// Update tableDefMap.
	c.tableDefMap[key][tableDef.TableVersion] = &tableDef

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
	if _, ok := c.tableDMLIdxMap[dmlkey]; !ok {
		c.tableDMLIdxMap[dmlkey] = 0
	} else {
		// duplicate table schema file found, this should not happen.
		log.Panic("duplicate schema file found",
			zap.String("path", path), zap.Any("tableDef", tableDef),
			zap.Any("schemaKey", schemaKey), zap.Any("dmlkey", dmlkey))
	}
	return nil
}

func (c *consumer) mustGetTableDef(key cloudstorage.SchemaPathKey) cloudstorage.TableDefinition {
	var tableDef *cloudstorage.TableDefinition
	if tableDefs, ok := c.tableDefMap[key.GetKey()]; ok {
		tableDef = tableDefs[key.TableVersion]
	}
	if tableDef == nil {
		log.Panic("tableDef not found", zap.Any("key", key), zap.Any("tableDefMap", c.tableDefMap))
	}
	return *tableDef
}

func (c *consumer) handleNewFiles(
	ctx context.Context,
	dmlFileMap map[cloudstorage.DmlPathKey]fileIndexRange,
) error {
	keys := make([]cloudstorage.DmlPathKey, 0, len(dmlFileMap))
	for k := range dmlFileMap {
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		log.Info("no new dml files found since last round")
		return nil
	}
	log.Info("new dml files found since last round", zap.Any("keys", keys))
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].TableVersion != keys[j].TableVersion {
			return keys[i].TableVersion < keys[j].TableVersion
		}
		if keys[i].PartitionNum != keys[j].PartitionNum {
			return keys[i].PartitionNum < keys[j].PartitionNum
		}
		if keys[i].Date != keys[j].Date {
			return keys[i].Date < keys[j].Date
		}
		if keys[i].Schema != keys[j].Schema {
			return keys[i].Schema < keys[j].Schema
		}
		return keys[i].Table < keys[j].Table
	})

	// TODO: support handling dml events of different tables concurrently.
	// Note: dml events of the same table should be handled sequentially.
	//       so we can not just pipeline this loop.
	for _, key := range keys {
		tableDef := c.mustGetTableDef(key.SchemaPathKey)
		tableID := c.tableIDGenerator.generateFakeTableID(key.Schema, key.Table, key.PartitionNum)
		if _, ok := c.snowflakeConnectorMap[tableID]; !ok {
			db, err := snowsql.OpenSnowflake(c.sfConfig)
			if err != nil {
				return errors.Trace(err)
			}
			connector, err := snowsql.NewSnowflakeConnector(
				db,
				fmt.Sprintf("increment_stage_%s", tableDef.Table),
				c.sinkURI,
				c.awsCredential,
			)
			if err != nil {
				return errors.Trace(err)
			}
			c.snowflakeConnectorMap[tableID] = connector
		}

		// if the key is a fake dml path key which is mainly used for
		// sorting schema.json file before the dml files, then execute the ddl query.
		if key.PartitionNum == fakePartitionNumForSchemaFile &&
			len(key.Date) == 0 && len(tableDef.Query) > 0 {
			return errors.Errorf("Please check the DDL query: [%s], "+
				"if necessary, please manually execute the DDL query in Snowflake, "+
				"remove the %s/%s/%s/meta/schema_%d_{hash}.json, "+
				"and restart the program.",
				tableDef.Query, c.externalStorage.URI(), tableDef.Schema, tableDef.Table, tableDef.TableVersion)
		}

		fileRange := dmlFileMap[key]
		for i := fileRange.start; i <= fileRange.end; i++ {
			if err := c.syncExecDMLEvents(ctx, tableDef, tableID, key, i); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *consumer) run(ctx context.Context, flushInterval time.Duration) error {
	ticker := time.NewTicker(flushInterval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-c.errCh:
			return err
		case <-ticker.C:
		}

		dmlFileMap, err := c.getNewFiles(ctx)
		if err != nil {
			return errors.Trace(err)
		}

		if err = c.handleNewFiles(ctx, dmlFileMap); err != nil {
			return errors.Trace(err)
		}
	}
}

// copied from kafka-consumer
type fakeTableIDGenerator struct {
	tableIDs       map[string]int64
	currentTableID int64
	mu             sync.Mutex
}

func (g *fakeTableIDGenerator) generateFakeTableID(schema, table string, partition int64) int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	key := quotes.QuoteSchema(schema, table)
	if partition > 0 {
		key = fmt.Sprintf("%s.`%d`", key, partition)
	}
	if tableID, ok := g.tableIDs[key]; ok {
		return tableID
	}
	g.currentTableID++
	g.tableIDs[key] = g.currentTableID
	return g.currentTableID
}

func StartReplicateIncrement(sfConfig *snowsql.SnowflakeConfig, sinkUri *url.URL, flushInterval time.Duration, configFile, timezone string, credential *credentials.Value) error {
	var consumer *consumer
	var err error

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	deferFunc := func() int {
		stop()
		if err != nil && err != context.Canceled {
			return 1
		}
		// close all snowflake connections
		if consumer != nil {
			for _, db := range consumer.snowflakeConnectorMap {
				db.Close()
			}
		}
		return 0
	}
	defer deferFunc()

	consumer, err = newConsumer(ctx, sfConfig, sinkUri, configFile, timezone, credential)
	if err != nil {
		return errors.Annotate(err, "failed to create storage consumer")
	}
	if consumer.sinkURI.Scheme == "gcs" {
		log.Info("Skip replicating increment. GCS does not supprt snowflake connector now...")
		return nil
	}
	if err = consumer.run(ctx, flushInterval); err != nil {
		return errors.Annotate(err, "error occurred while running consumer")
	}
	return nil
}
