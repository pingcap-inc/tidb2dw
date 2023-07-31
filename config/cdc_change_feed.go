package config

type CfFilterCfg struct {
	Rules []string `json:"rules"`
}

type CloudStorageCfg struct {
	OutputColumnId bool `json:"output_column_id"`
}

type CSVCfg struct {
	IncludeCommitTs bool `json:"include_commit_ts"`
}

type SinkCfg struct {
	CSV          *CSVCfg          `json:"csv"`
	CloudStorage *CloudStorageCfg `json:"cloud_storage_config"`
}

type ReplicateCfg struct {
	Filter         *CfFilterCfg `json:"filter"`
	Sink           *SinkCfg     `json:"sink"`
	EnableOldValue bool         `json:"enable_old_value"`
}

type ChangeFeedCfg struct {
	SinkURI    string        `json:"sink_uri"`
	ReplicaCfg *ReplicateCfg `json:"replica_config"`
	StartTs    uint64        `json:"start_ts"`
}
