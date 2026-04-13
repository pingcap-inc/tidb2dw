package icebergconsumer

import (
	"context"
	"encoding/json"
	"net/url"
	"path"

	"github.com/pingcap/tidb/br/pkg/storage"
)

type Checkpoint struct {
	SourceID            string `json:"source_id"`
	Schema              string `json:"schema"`
	Table               string `json:"table"`
	LastMetadataVersion int    `json:"last_metadata_version"`
	DDLApplied          bool   `json:"ddl_applied"`
	LastDataFile        string `json:"last_data_file"`
}

func checkpointPath(schema, table string) string {
	return path.Join("iceberg-checkpoints", url.PathEscape(schema), url.PathEscape(table)+".json")
}

func SaveCheckpoint(ext storage.ExternalStorage, checkpoint Checkpoint) error {
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}

	return ext.WriteFile(context.Background(), checkpointPath(checkpoint.Schema, checkpoint.Table), data)
}

func LoadCheckpoint(ext storage.ExternalStorage, schema, table string) (Checkpoint, bool, error) {
	filePath := checkpointPath(schema, table)
	exists, err := ext.FileExists(context.Background(), filePath)
	if err != nil {
		return Checkpoint{}, false, err
	}
	if !exists {
		return Checkpoint{}, false, nil
	}

	data, err := ext.ReadFile(context.Background(), filePath)
	if err != nil {
		return Checkpoint{}, false, err
	}

	var checkpoint Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return Checkpoint{}, false, err
	}

	return checkpoint, true, nil
}
