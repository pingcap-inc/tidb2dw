package bigquerysql

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/pingcap/errors"
)

func runQuery(ctx context.Context, client *bigquery.Client, query string) error {
	job, err := client.Query(query).Run(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if status.Err() != nil {
		return errors.Trace(errors.Errorf("Bigquery job completed with error: %v", status.Err()))
	}
	return nil
}

func deleteTable(ctx context.Context, client *bigquery.Client, datasetID, tableID string) error {
	tableRef := client.Dataset(datasetID).Table(tableID)
	err := tableRef.Delete(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func loadGCSFileToBigQuery(ctx context.Context, client *bigquery.Client, datasetID, tableID, gcsFilePath string) error {
	gcsRef := bigquery.NewGCSReference(gcsFilePath)
	gcsRef.SourceFormat = bigquery.CSV
	gcsRef.NullMarker = "\\N"

	loader := client.Dataset(datasetID).Table(tableID).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteAppend

	job, err := loader.Run(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if status.Err() != nil {
		return errors.Trace(errors.Errorf("Bigquery load snapshot job completed with error: %v", status.Err()))
	}
	return nil
}
