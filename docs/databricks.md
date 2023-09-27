# Databricks

## Replicate

To replicate snapshot and incremental data of a TiDB Table to Databricks:

```shell
export AWS_ACCESS_KEY_ID=<ACCESS_KEY>
export AWS_SECRET_ACCESS_KEY=<SECRET_KEY>

./tidb2dw databricks \
    --storage s3://my-demo-bucket/prefix \
    --table <database_name>.<table_name> \
    --databricks.host dbc-********-****.cloud.databricks.com \
    --databricks.endpoint 2**************4 \
    --databricks.catalog <catalog> \
    --databricks.schema <schema> \
    --databricks.token dapi******************************** \
    --databricks.credential <storage-credential> 

# You can use 'SHOW STORAGE CREDENTIALS' in databricks to check what credential names are available.
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

## Noteworthy

1. [How to give Databricks sufficient permission in AWS](https://docs.databricks.com/en/data-governance/unity-catalog/get-started.html)
2. `tidb2dw` uses these key features in Databricks below

   - [Databricks Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
   - [`COPY INTO` Syntax](https://docs.databricks.com/en/ingestion/copy-into/index.html)
   - [Storage Credentials](https://docs.databricks.com/en/sql/language-manual/sql-ref-storage-credentials.html)
   - [External Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-external-tables.html)

3. Databricks don't support the `BINARY` type in the external table with the CSV file which are `tidb2dw` used. So please ensure that the table you want to replicate doesn't have the `BINARY` or `VARBINARY` type column.
4. The type mapping from TiDB to Databricks is defined [here](/pkg/databrickssql/types.go).
5. Databricks has some limitations on modifying table schemas, like Databricks does [not support primary key and foreign key](https://docs.databricks.com/en/tables/constraints.html#declare-primary-key-and-foreign-key-relationships), not support default value in all kind of storage layers yet. 
