package cmd

import (
	"time"

	"github.com/spf13/cobra"
)

type SourceFormat string

const (
	SourceFormatCSV     SourceFormat = "csv"
	SourceFormatIceberg SourceFormat = "iceberg"
)

type sourceOptions struct {
	format              SourceFormat
	icebergSourceURI    string
	icebergPollInterval time.Duration
}

func defaultSourceOptions() sourceOptions {
	return sourceOptions{
		format:              SourceFormatCSV,
		icebergPollInterval: 5 * time.Second,
	}
}

func addSourceFlags(cmd *cobra.Command, opts *sourceOptions) {
	cmd.Flags().StringVar((*string)(&opts.format), "source-format", string(opts.format), "source format: csv, iceberg")
	cmd.Flags().StringVar(&opts.icebergSourceURI, "iceberg.source-uri", "", "iceberg source URI")
	cmd.Flags().DurationVar(&opts.icebergPollInterval, "iceberg.poll-interval", opts.icebergPollInterval, "iceberg poll interval")
}
