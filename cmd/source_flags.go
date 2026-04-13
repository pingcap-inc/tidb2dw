package cmd

import (
	"fmt"
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

	previousPreRunE := cmd.PreRunE
	cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		if previousPreRunE != nil {
			if err := previousPreRunE(cmd, args); err != nil {
				return err
			}
		}
		return validateSourceOptions(*opts)
	}
}

func validateSourceOptions(opts sourceOptions) error {
	switch opts.format {
	case "", SourceFormatCSV:
		return nil
	case SourceFormatIceberg:
		return fmt.Errorf("source-format=iceberg is not implemented yet")
	default:
		return fmt.Errorf("unsupported source-format %q", opts.format)
	}
}
