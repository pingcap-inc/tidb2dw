# Snowflake

## Replicate

To replicate snapshot and incremental data of a TiDB Table to Snowflake:

```shell
export AWS_ACCESS_KEY_ID=<from-secret-store>
export AWS_SECRET_ACCESS_KEY=<from-secret-store>
export AWS_SESSION_TOKEN=<from-secret-store>  # Optional
export TIDB2DW_SNOWFLAKE_ACCOUNT_ID=<organization-account>
export TIDB2DW_SNOWFLAKE_USER=<from-secret-store>
export TIDB2DW_SNOWFLAKE_PASSWORD=<from-secret-store>
export TIDB2DW_SNOWFLAKE_DATABASE=<database>
export TIDB2DW_SNOWFLAKE_SCHEMA=<schema>

./tidb2dw snowflake \
    --storage 's3://<bucket>/<prefix>' \
    --table <database_name>.<table_name>

# Note that you may also need to specify these parameters:
#   --cdc.host x.x.x.x
#   --tidb.host x.x.x.x
#   --tidb.user <user>
#   --tidb.pass <pass>
# Use --help for details.
```

For real PIN-14 Snowflake/S3 release evidence, use the config names, wrapper,
and sanitization rules in [PIN-14 Snowflake/S3 E2E Runbook](pin14-e2e-runbook.md).
Do not commit or attach secret values, presigned URLs, or raw DSNs.

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
