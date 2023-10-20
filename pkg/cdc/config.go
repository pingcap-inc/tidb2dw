package cdc

import (
	"fmt"
	"net/url"
	"time"

	apiv2 "github.com/pingcap/tiflow/cdc/api/v2"
)

type ChangefeedConfig struct {
	ReplicaConfig *apiv2.ReplicaConfig `json:"replica_config"`
	SinkURI       string               `json:"sink_uri"`
	StartTs       uint64               `json:"start_ts"`
}

type SinkURIConfig struct {
	storageUri    *url.URL
	flushInterval time.Duration
	fileSize      int
	protocol      string
}

func (s *SinkURIConfig) genSinkURI() (*url.URL, error) {
	values := s.storageUri.Query()
	values.Add("flush-interval", s.flushInterval.String())
	values.Add("file-size", fmt.Sprint(s.fileSize))
	values.Add("protocol", s.protocol)
	s.storageUri.RawQuery = values.Encode()
	return s.storageUri, nil
}
