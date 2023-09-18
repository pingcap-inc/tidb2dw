package bigquerysql

import (
	"context"
	"fmt"
	"net/http"

	"cloud.google.com/go/bigquery"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"google.golang.org/api/googleapi"
)

func getIncrementTableColumns(columns []cloudstorage.TableCol) []cloudstorage.TableCol {
	return append([]cloudstorage.TableCol{
		{
			Name: CDC_FLAG_COLUMN_NAME,
			Tp:   "varchar",
		},
		{
			Name: CDC_TABLENAME_COLUMN_NAME,
			Tp:   "varchar",
		},
		{
			Name: CDC_SCHEMANAME_COLUMN_NAME,
			Tp:   "varchar",
		},
		{
			// is timestamp?
			Name: CDC_TIMESTAMP_COLUMN_NAME,
			Tp:   "varchar",
		},
	}, columns...)
}

func genTableMetadata(columns []cloudstorage.TableCol, bqTableID string) (*bigquery.TableMetadata, error) {
	schema := bigquery.Schema{}
	for _, column := range columns {
		bqType, err := GetBigQueryColumnType(column)
		if err != nil {
			return nil, errors.Trace(err)
		}
		schema = append(schema, &bigquery.FieldSchema{
			Name: column.Name,
			Type: bqType,
		})
	}
	return &bigquery.TableMetadata{
		Name:   bqTableID,
		Schema: schema,
	}, nil
}

func checkTableExists(ctx context.Context, client *bigquery.Client, datasetID, tableID string) (bool, error) {
	tableRef := client.Dataset(datasetID).Table(tableID)
	_, err := tableRef.Metadata(ctx)
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok && e.Code == http.StatusNotFound {
			return false, nil
		}
		return false, errors.Trace(err)
	}
	// It means table exists if no error returned
	return true, nil
}

func createNativeTable(ctx context.Context, client *bigquery.Client, datasetID, tableID string, columns []cloudstorage.TableCol) error {
	tableMetadata, err := genTableMetadata(columns, tableID)
	if err != nil {
		return errors.Trace(err)
	}
	tableRef := client.Dataset(datasetID).Table(tableID)
	if err := tableRef.Create(context.Background(), tableMetadata); err != nil {
		return errors.Trace(err)
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
	loader.WriteDisposition = bigquery.WriteEmpty

	job, err := loader.Run(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if status.Err() != nil {
		return errors.Trace(fmt.Errorf("Bigquery load snapshot job completed with error: %v", status.Err()))
	}
	return nil
}
