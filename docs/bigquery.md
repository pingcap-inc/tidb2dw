# BigQuery

## Replicate

To replicate snapshot and incremental data of a TiDB Table to BigQuery:

```shell
./tidb2dw bigquery \
    --storage gs://my-demo-bucket/prefix \
    --table <database_name>.<table_name> \
    --credentials-file-path <google_credentials_json_file_path> \
    --bq.project-id <bigquery.project_id> \
    --bq.dataset-id <bigquery.dataset_id>

# Note that you may also need to specify these parameters:
#   --cdc.host x.x.x.x
#   --tidb.host x.x.x.x
#   --tidb.user <user>
#   --tidb.pass <pass>
# Use --help for details.
```

## Supported DDL Operations

All DDL which will change the schema of table are supported (except index related), including:

- Add column
- Drop column
- Rename column
- Drop table
- Truncate table

> **Note**
>
> 1. BigQuery has some limitations on modifying table schemas, like BigQuery does not support add a REQUIRED column to an existing table schema, refer to [BigQuery Docs](https://cloud.google.com/bigquery/docs/managing-table-schemas), in some cases, its better to recreate the table.
> 2. The type mapping from TiDB to BigQuery is defined [here](https://github.com/pingcap-inc/tidb2dw/blob/main/pkg/bigquerysql/types.go).
