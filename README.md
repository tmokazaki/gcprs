# Google Cloud Platform API library

This library supports parts of `bigquery`, `cloud storage`, `sheet` APIs with OAuth2 authentication.
API return object can serialize/deserialize to/from JSON. To setup OAuth2, please refer to the [Google Document](https://developers.google.com/identity/protocols/oauth2).

## BigQuery

You can use following APIs via this library.

- list_project
- list_dataset
- list_tables
- create_table
- delete_table
- insert_all
  - This will upload rust object into table. Table shcema will generate by trait. Creating schema by using derive would be a future work.
- list_tabledata
- query

## Cloud Storage

You can use following APIs via this library.

- list_objects
- get_object
