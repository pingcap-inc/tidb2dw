package icebergconsumer

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
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
	trimmedSourceURI := strings.TrimSpace(sourceURI)
	if trimmedSourceURI == "" {
		return nil, errors.New("iceberg source uri is required")
	}

	parsedSourceURI, err := url.Parse(trimmedSourceURI)
	if err != nil || parsedSourceURI.Scheme == "" {
		return nil, fmt.Errorf("invalid iceberg source uri: %q", sourceURI)
	}

	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}

	return &Config{
		SourceURI:    parsedSourceURI,
		PollInterval: pollInterval,
	}, nil
}
