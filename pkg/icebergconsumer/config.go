package icebergconsumer

import (
	"errors"
	"net/url"
	"time"
)

type Mode string

const (
	ModeFull            Mode = "full"
	ModeSnapshotOnly    Mode = "snapshot-only"
	ModeIncrementalOnly Mode = "incremental-only"
)

type Config struct {
	SourceURI    *url.URL
	PollInterval time.Duration
}

func NewConfig(sourceURI string, pollInterval time.Duration) (*Config, error) {
	if sourceURI == "" {
		return nil, errors.New("iceberg source uri is required")
	}

	parsedSourceURI, err := url.Parse(sourceURI)
	if err != nil {
		return nil, err
	}

	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}

	return &Config{
		SourceURI:    parsedSourceURI,
		PollInterval: pollInterval,
	}, nil
}
