package config_test

import (
	"encoding/json"
	"strings"
	"testing"

	cfg "github.com/pingcap-inc/tidb2dw/config"
)

func TestChangeFeedConfig(t *testing.T) {
	cfCfg := &cfg.ChangeFeedCfg{
		SinkURI: "s3://sinkuri",
		ReplicaCfg: &cfg.ReplicateCfg{
			Filter: &cfg.CfFilterCfg{Rules: []string{"rule"}},
			Sink: &cfg.SinkCfg{
				CSV:          &cfg.CSVCfg{IncludeCommitTs: true},
				CloudStorage: &cfg.CloudStorageCfg{OutputColumnId: true},
			},
			EnableOldValue: false,
		},
		StartTs: 0,
	}

	cfgStr, _ := json.Marshal(cfCfg)
	expectedStr := "{\"sink_uri\":\"s3://sinkuri\",\"replica_config\":{\"filter\":" +
		"{\"rules\":[\"rule\"]},\"sink\":{\"csv\":{\"include_commit_ts\":true}," +
		"\"cloud_storage_config\":{\"output_column_id\":true}},\"enable_old_value\":false}," +
		"\"start_ts\":0}"

	if strings.Compare(string(cfgStr), expectedStr) != 0 {
		t.Errorf("cfgStr %s not equal to expectedStr %s", string(cfgStr), expectedStr)
	}
}
