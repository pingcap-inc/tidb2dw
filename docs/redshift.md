# Redshift

## Replicate

To replicate snapshot and incremental data of a TiDB Table to Redshift:

```shell
export AWS_ACCESS_KEY_ID=<ACCESS_KEY>
export AWS_SECRET_ACCESS_KEY=<SECRET_KEY>
export AWS_SESSION_TOKEN=<SESSION_TOKEN>  # Optional

./tidb2dw redshift \
    --storage s3://my-demo-bucket/prefix \
    --table <database_name>.<table_name> \
    --redshift.host <hostname>.<region>.redshift.amazonaws.com \
    --redshift.port <port> \
    --redshift.user <username> \
    --redshift.pass <password> \
    --redshift.database <database> \
    --redshift.schema <schema> \

# Note that you may also need to specify these parameters:
#   --cdc.host x.x.x.x
#   --tidb.host x.x.x.x
#   --tidb.user <user>
#   --tidb.pass <pass>
# Use --help for details.
```

## Replicate From TiCDC Iceberg Storage Sink

To consume an existing TiCDC Iceberg storage sink and apply it to Redshift:

```shell
export AWS_ACCESS_KEY_ID=<ACCESS_KEY>
export AWS_SECRET_ACCESS_KEY=<SECRET_KEY>
export AWS_SESSION_TOKEN=<SESSION_TOKEN>  # Optional

./tidb2dw redshift \
    --source-format iceberg \
    --iceberg.source-uri '<ticdc_iceberg_sink_uri>' \
    --storage s3://my-staging-bucket/prefix \
    --table <database_name>.<table_name> \
    --redshift.host <hostname>.<region>.redshift.amazonaws.com \
    --redshift.port <port> \
    --redshift.user <username> \
    --redshift.pass <password> \
    --redshift.database <database> \
    --redshift.schema <schema>
```

`--iceberg.source-uri` must be the exact TiCDC Iceberg sink URI, including any catalog or warehouse query parameters. In Iceberg mode, `--storage` is a writable S3 workspace used for temporary staged files and checkpoints; it is not the source Iceberg warehouse location.

## Supported DDL Operations

All DDL which will change the schema of table are supported (except index related), including:

- Add column
- Drop column
- Rename column
- Drop table
- Truncate table

> **Note**
>
> 1. The type mapping from TiDB to Redshift is defined [here](https://github.com/pingcap-inc/tidb2dw/blob/main/pkg/redshiftsql/types.go).
