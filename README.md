Server-Side, Data Layer Validation, Google Cloud Platform Setup Guide 
==============
This repo complements the https://github.com/loldenburg/datalayertest repository with a set of example google cloud functions.
# Setup Cloud Function via Cloud Build
# todo: add instructions for local setup


# BigQuery integration
With 
See the #todos in the log_datalayer_error.py file on how to enable the BigQuery integration.
You also need to create a BigQuery dataset and table.

1. Name the dataset "datalayer_errors" and the table "datalayer_error_logs".
2. Execute the following SQL query to create the table:
```sql
   CREATE TABLE IF NOT EXISTS `{table_id}`
    (
      event_id STRING NOT NULL,
      event_name STRING NOT NULL, 
      error_types STRING,
      error_vars STRING,
      logged_at TIMESTAMP NOT NULL,
      url_full STRING,
      user_id STRING,
      tealium_profile STRING 
    )
```
