# Google Cloud Storage

## Export

To export snapshot and incremental data of a TiDB Table to Google Cloud Storage:

```shell
./tidb2dw bigquery \
    --storage gs://my-demo-bucket/prefix \
    --table <database_name>.<table_name> \
    --credentials-file-path <google_credentials_json_file_path>

# Note that you may also need to specify these parameters:
#   --cdc.host x.x.x.x
#   --tidb.host x.x.x.x
#   --tidb.user <user>
#   --tidb.pass <pass>
# Use --help for details.
```
