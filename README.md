# tidb2dw

A tool to replicate data change from TiDB to Data Warehouses in real-time.

## Supported Data Warehouses

- [Snowflake](/docs/snowflake.md)
- [Amazon Redshift](/docs/redshift.md)
- [Bigquery](/docs/bigquery.md)
- [Databricks](/docs/databricks.md)

## Supported Storage Service

You can also export data to cloud object storage.

- [AWS S3](https://github.com/pingcap-inc/tidb2dw/blob/main/docs/s3.md)
- [Google Cloud Storage](https://github.com/pingcap-inc/tidb2dw/blob/main/docs/gcs.md)

## Download

```bash
# Linux AMD64:
wget https://github.com/pingcap-inc/tidb2dw/releases/download/v0.0.3/tidb2dw-v0.0.3-linux-amd64

# Linux ARM64:
wget https://github.com/pingcap-inc/tidb2dw/releases/download/v0.0.3/tidb2dw-v0.0.3-linux-arm64

# MacOS AMD64:
wget https://github.com/pingcap-inc/tidb2dw/releases/download/v0.0.3/tidb2dw-v0.0.3-darwin-amd64

# MacOS ARM64:
wget https://github.com/pingcap-inc/tidb2dw/releases/download/v0.0.3/tidb2dw-v0.0.3-darwin-arm64
```

## Build from source

```bash
git clone https://github.com/pingcap-inc/tidb2dw.git
cd tidb2dw && make build
```

## Known limitations

1. Only support TiDB v7.1.0 or later, and TiDB v7.3.0 or later is required to support DDL.
2. Only tables with primary key are supported.
3. Although tidb2dw support replicate DDL, Data Warehouses and TiDB are not fully compatible, so not all DDLs are supported.
4. Should execute at least one DML before DDL or will report error.
