package cdc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	apiv2 "github.com/pingcap/tiflow/cdc/api/v2"
	"github.com/pingcap/tiflow/pkg/config"
	putil "github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

type CDCConnector struct {
	cdcServer     string
	tables        []string
	startTSO      uint64
	sinkURIConfig *SinkURIConfig
	SinkURI       *url.URL
}

func NewCDCConnector(cdcHost string, cdcPort int, tables []string, startTSO uint64, storageUri *url.URL, flushInterval time.Duration, fileSize int) (*CDCConnector, error) {
	sinkURIConfig := &SinkURIConfig{
		storageUri:    storageUri,
		flushInterval: flushInterval,
		fileSize:      fileSize,
		protocol:      "csv",
	}
	sinkURI, err := sinkURIConfig.genSinkURI()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &CDCConnector{
		cdcServer:     fmt.Sprintf("http://%s:%d", cdcHost, cdcPort),
		tables:        tables,
		startTSO:      startTSO,
		sinkURIConfig: sinkURIConfig,
		SinkURI:       sinkURI,
	}, nil
}

func (c *CDCConnector) CreateChangefeed() error {
	client := &http.Client{}
	replicateCfg := apiv2.GetDefaultReplicaConfig()
	replicateCfg.Sink.CSVConfig.IncludeCommitTs = true
	replicateCfg.Sink.CSVConfig.BinaryEncodingMethod = config.BinaryEncodingHex
	replicateCfg.Sink.CloudStorageConfig = &apiv2.CloudStorageConfig{
		FlushInterval:  putil.AddressOf(c.sinkURIConfig.flushInterval.String()),
		FileSize:       putil.AddressOf(c.sinkURIConfig.fileSize),
		OutputColumnID: putil.AddressOf(true),
	}
	replicateCfg.Sink.DateSeparator = putil.AddressOf(config.DateSeparatorDay.String())
	replicateCfg.EnableOldValue = false
	replicateCfg.Filter = &apiv2.FilterConfig{Rules: c.tables}
	cfCfg := &ChangefeedConfig{
		SinkURI:       c.SinkURI.String(),
		ReplicaConfig: replicateCfg,
	}
	if c.startTSO != 0 {
		cfCfg.StartTs = c.startTSO
	}
	bytesData, _ := json.Marshal(cfCfg)
	url, err := url.JoinPath(c.cdcServer, "api/v2/changefeeds")
	if err != nil {
		return errors.Annotate(err, "join url failed")
	}
	httpReq, _ := http.NewRequest("POST", url, bytes.NewReader(bytesData))
	resp, err := client.Do(httpReq)
	if err != nil {
		return errors.Trace(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("create changefeed failed, status code: %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Trace(err)
	}
	respData := make(map[string]interface{})
	if err = json.Unmarshal(body, &respData); err != nil {
		return errors.Trace(err)
	}
	changefeedID := respData["id"].(string)
	replicateConfig := respData["config"].(map[string]interface{})
	log.Info("create changefeed success", zap.String("changefeed-id", changefeedID), zap.Any("replica-config", replicateConfig))

	return nil
}
