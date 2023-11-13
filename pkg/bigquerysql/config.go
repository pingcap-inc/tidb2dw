package bigquerysql

import (
	"context"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

type BigQueryConfig struct {
	ProjectID           string
	DatasetID           string
	CredentialsFilePath string // path to google credentials file
}

func (cfg *BigQueryConfig) NewClient() (*bigquery.Client, error) {
	opts := []option.ClientOption{}
	if cfg.CredentialsFilePath != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFilePath))
	}
	return bigquery.NewClient(context.Background(), cfg.ProjectID, opts...)
}
