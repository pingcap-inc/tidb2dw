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

- Add column without default value
- Drop column
- Rename column
- Drop table
- Truncate table

> **Note**
>
> 1. BigQuery has some limitations on modifying table schemas, like BigQuery does not support add a REQUIRED column to an existing table schema, refer to [BigQuery Docs](https://cloud.google.com/bigquery/docs/managing-table-schemas), in some cases, its better to recreate the table.
> 2. The type mapping from TiDB to BigQuery is defined [here](https://github.com/pingcap-inc/tidb2dw/blob/main/pkg/bigquerysql/types.go).
> 3. tidb2dw will not sync column's default value to BigQuery, when you add a new column with default value, it will throw an error, you'd better to recreate the table in BigQuery and start a new replication task.

## Usage Guide

### Prerequisites

- A Google Cloud Platform (GCP) service account
- A Google Cloud Platform (GCP) project with following APIs:
  - BigQuery
  - Cloud Storage (GCS) Bucket

tidb2dw uses the Google Cloud Platform (GCP) service account to authenticate with Google Cloud services. The service account must have the necessary permissions to access the BigQuery and Cloud Storage (GCS) Bucket. It will first use dumpling & ticdc to upload the snapshot and incremental data to the GCS bucket, then use the BigQuery API to load the data into BigQuery.

### Create a Service Account

1. Go to the [Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts) page in the Google Cloud Console.
2. Select your project and click `Create Service Account`.
3. Enter a name for the service account and click `Create`.

After creating the service account, navigate to the `Details` page, switch to the `Keys` tab, then click `Add Key` > `Create new key`, select `JSON` and click `Create`, now you are downloading the JSON key file for the service account, please save it in a safe place, it will be used as the `--credentials-file-path` parameter.

### Grant permission to Service Account

#### Grant GCS Bucket Access

1. In `IAM & Admin` > `Roles` > `Create Role`, create a custom role with the following permissions:
    - storage.buckets.get
    - storage.objects.create
    - storage.objects.delete
    - storage.objects.get
    - storage.objects.list
    - storage.objects.update

2. Go to the Bucket page, and choose a GCS bucket you want to use, note that the GCS bucket must be in the save region as your BigQuery.

3. On the Bucket details page, click the Permissions tab, and then click Grant access.

4. Fill in the following information to grant access to your bucket, and then click Save.
    - In the New Principals field, paste the Service Account ID you created at first step.
    - In the Select a role drop-down list, type the name of the IAM role you just created, and then choose the name from the filter result.

For this part you can also refer to the [TiDB Cloud Sink to Cloud Storage - GCS](https://docs.pingcap.com/tidbcloud/changefeed-sink-to-cloud-storage).

#### Grant BigQuery Access

1. Navigate to `IAM & Admin` > `Roles` > `Create Role`

2. Create first custom role (bq-role-1) with the following permission
    - bigquery.datasets.get
    - bigquery.tables.create
    - bigquery.tables.createIndex
    - bigquery.tables.delete
    - bigquery.tables.deleteIndex
    - bigquery.tables.getData
    - bigquery.tables.update
    - bigquery.tables.updateData

3. Create second custom role (bq-role-2) with the following permission
    - bigquery.jobs.create

4. Go to the BigQuery page, choose a dataset you want to use, tidb2dw will create a table with the same name in it.

5. On the dataset details page, click `Sharing` > `Permissions` < `Add Principal` to grant access, fill the following information then click save.
    - In the New Principals field, paste the Service Account ID you created at first step.
    - In the Select a role drop-down list, type the name of the IAM role (bq-role-1) you just created, and then choose the name from the filter result.

6. Navigate to `IAM & Admin` > `IAM` [page](https://console.cloud.google.com/iam-admin/iam?project=personal-wd)

7. Click `Grant Access`, fill the following information then click save.
    - In the New Principals field, paste the Service Account ID you created at first step.
    - In the Select a role drop-down list, type the name of the IAM role (bq-role-2) you just created, and then choose the name from the filter result.

### Run tidb2dw

After you have created the service account and granted the necessary permissions, you can run tidb2dw to replicate data from TiDB to BigQuery.

```shell
./tidb2dw bigquery \
    --storage gs://my-demo-bucket/prefix \
    --table <database_name>.<table_name> \
    --credentials-file-path <google_credentials_json_file_path> \
    --bq.project-id <bigquery.project_id> \
    --bq.dataset-id <bigquery.dataset_id>
```

tidb2dw will replicate data from TiDB to BigQuery by following these steps:

1. Use dumpling to dump the snapshot data of the specified table into a GCS bucket.
2. Create a cdc changefeed task to replicate the incremental data of the specified table to the GCS bucket.
3. Copy the table schema from TiDB to BigQuery.
4. Create a BigQuery task to load the snapshot data from the GCS bucket into BigQuery.
5. Cronically load the incremental data from the GCS bucket into BigQuery.
