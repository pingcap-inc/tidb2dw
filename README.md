# tidb2dw

A tool to replicate data from TiDB to Data Warehouse.

> **Note**
> Only support TiDB v7.1.0 or later, and only support Snowflake as target Data Warehouse now.

## build

```bash
make build
```

## replicate snapshot data from TiDB to Snowflake

```bash
AWS_SDK_LOAD_CONFIG=true ./bin/tidb2dw snowflake snapshot --storage s3://test/dump --table <database_name>.<table_name> --snowflake.account-id <organization>-<account> --snowflake.user <use_name> --snowflake.pass <password> --snowflake.database <database> --snowflake.schema <schema>
```

## replicate incremental data from TiDB to Snowflake

> **Warning**
> We do not support ddl replication yet. Any ddl operation will cause the incremental replication stop. You need to manually run the DDL on target table and then restart the incremental replication.
> Restart incremental replication without manually runing DDL on target table may cause data loss.

```bash
# create a change feed
tiup cdc cli changefeed create --server=http://127.0.0.1:8300 --sink-uri="s3://test/cdc?protocol=csv&flush-interval=5m&file-size=268435456"

# start the replication
AWS_SDK_LOAD_CONFIG=true ./bin/tidb2dw snowflake increment --upstream-uri="s3://test/cdc?protocol=csv&flush-interval=5m&file-size=268435456" --downstream-uri="<use_name>:<password>@<organization>-<account>/<database>/<schema>?warehouse=<warehouse>"

# run any dml operation in tidb
...
```
