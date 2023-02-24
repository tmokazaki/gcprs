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
  - This will upload rust object into table. Table shcema will be generated by trait. Creating schema by using derive macro would be a future work.
- list_tabledata
- query

## Cloud Storage

You can use following APIs via this library.

- list_objects
- get_object
- get_object_metadata
- get_object_stream
- delete_object
- insert_object
  - insert_string: wrap insert_object function to call with String object
  - insert_file: wrap insert_object function to call with file name

## Spread Sheet
