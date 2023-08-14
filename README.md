# tidb2dw

A tool to replicate data change from TiDB to Data Warehouses in real-time.

> **Note**
> Only support TiDB v7.1.0 or later, and only support Snowflake as target Data Warehouse now.
> To support DDL, TiDB v7.3.0 or later is required.

## Build

```bash
make build
```

## Getting Started

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

## Supported DDL Operations

All DDL which will change the schema of table are supported (except index related), including:

- Add column
- Drop column
- Rename column
- Modify column type
- Drop table
- Truncate table

> **Note**
> 1. Snowflake does not support partition table, tidb2dw will view table with multiple partitions as ordinary table.
> 2. Snowflake has a lot of limitations on modifying column type, like Snowflake does not support update column default value, refer to [Snowflake Docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table-column).
> 3. The type mapping from TiDB to Snowflake is defined [here](./pkg/snowsql/types.go).
> 4. Should execute at least one DML before DDL or will report error.
