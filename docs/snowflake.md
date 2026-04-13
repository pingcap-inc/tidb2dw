# Snowflake

## Replicate

To replicate snapshot and incremental data of a TiDB Table to Snowflake:

```shell
export AWS_ACCESS_KEY_ID=<ACCESS_KEY>
export AWS_SECRET_ACCESS_KEY=<SECRET_KEY>
export AWS_SESSION_TOKEN=<SESSION_TOKEN>  # Optional

./tidb2dw snowflake \
    --storage s3://my-demo-bucket/prefix \
    --table <database_name>.<table_name> \
    --snowflake.account-id <organization>-<account> \
    --snowflake.user <username> \
    --snowflake.pass <password> \
    --snowflake.database <database> \
    --snowflake.schema <schema> \

# Note that you may also need to specify these parameters:
#   --cdc.host x.x.x.x
#   --tidb.host x.x.x.x
#   --tidb.user <user>
#   --tidb.pass <pass>
# Use --help for details.
```

## Replicate From TiCDC Iceberg Storage Sink

To consume an existing TiCDC Iceberg storage sink and apply it to Snowflake:

```shell
export AWS_ACCESS_KEY_ID=<ACCESS_KEY>
export AWS_SECRET_ACCESS_KEY=<SECRET_KEY>
export AWS_SESSION_TOKEN=<SESSION_TOKEN>  # Optional

./tidb2dw snowflake \
    --source-format iceberg \
    --iceberg.source-uri '<ticdc_iceberg_sink_uri>' \
    --storage s3://my-staging-bucket/prefix \
    --table <database_name>.<table_name> \
    --snowflake.account-id <organization>-<account> \
    --snowflake.user <username> \
    --snowflake.pass <password> \
    --snowflake.database <database> \
    --snowflake.schema <schema>
```

`--iceberg.source-uri` must be the exact TiCDC Iceberg sink URI, including any catalog or warehouse query parameters. In Iceberg mode, `--storage` is a writable S3 workspace used for temporary staged files and checkpoints; it is not the source Iceberg warehouse location.

## Supported DDL Operations

All DDL which will change the schema of table are supported (except index related), including:

- Add column
- Drop column
- Rename column
- Modify column type
- Drop table
- Truncate table

> **Note**
>
> 1. Snowflake does not support partition table, tidb2dw will view table with multiple partitions as ordinary table.
> 2. Snowflake has a lot of limitations on modifying column type, like Snowflake does not support update column default value, refer to [Snowflake Docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table-column).
> 3. The type mapping from TiDB to Snowflake is defined [here](https://github.com/pingcap-inc/tidb2dw/blob/main/pkg/snowsql/types.go).
