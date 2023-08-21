package cdc

import (
	"fmt"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
)

type FilterConfig struct {
	Rules []string `json:"rules,omitempty"`
}

type CSVConfig struct {
	Quote           string `json:"quote"`
	IncludeCommitTs bool   `json:"include_commit_ts"`
}

type CloudStorageConfig struct {
	OutputColumnID *bool `json:"output_column_id,omitempty"`
}

type SinkConfig struct {
	CSVConfig          *CSVConfig          `json:"csv,omitempty"`
	CloudStorageConfig *CloudStorageConfig `json:"cloud_storage_config,omitempty"`
}

type ReplicaConfig struct {
	EnableOldValue bool          `json:"enable_old_value"`
	Filter         *FilterConfig `json:"filter"`
	Sink           *SinkConfig   `json:"sink"`
}

type ChangefeedConfig struct {
	ReplicaConfig *ReplicaConfig `json:"replica_config"`
	SinkURI       string         `json:"sink_uri"`
	StartTs       uint64         `json:"start_ts"`
}

type SinkURIConfig struct {
	storageUri    *url.URL
	flushInterval time.Duration
	fileSize      int64
	protocol      string
	cred          *credentials.Value
}

func (s *SinkURIConfig) genSinkURI() (*url.URL, error) {
	if s.storageUri.Scheme != "s3" {
		return nil, errors.Errorf("Only support s3 storage")
	}
	values := s.storageUri.Query()
	values.Add("flush-interval", s.flushInterval.String())
	values.Add("file-size", fmt.Sprint(s.fileSize))
	values.Add("protocol", s.protocol)
	values.Add("access-key", s.cred.AccessKeyID)
	values.Add("secret-access-key", s.cred.SecretAccessKey)
	if s.cred.SessionToken != "" {
		values.Add("session-token", s.cred.SessionToken)
	}
	s.storageUri.RawQuery = values.Encode()
	return s.storageUri, nil
}
