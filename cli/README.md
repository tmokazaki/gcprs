# Google Cloud Platform API utility

Currently only small parts of `bigquery`, `cloud storage`, `sheet` APIs.
This library needs OAuth2 user authentication.

## Cli

`cli` execute some apis in a terminal. The results will be *table* or JSON format.

```
$ cli help

Usage: cli <COMMAND>

Commands:
  bq    Execute BigQuery APIs
  gcs   Execute GCS APIs
  df    Execute DataFusion
  help  Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help information
```

- bq
```
$ cli bq --help
Execute BigQuery APIs

Usage: cli bq [OPTIONS] <COMMAND>

Commands:
  list-project     Show available Project list
  list-dataset     Show available Dataset list
  list-tables      Show available Table list
  table-schema     Show Table Schema JSON
  table-delete     Delete Table
  list-table-data  Show Table Data
  query            Show Query result
  help             Print this message or the help of the given subcommand(s)

Options:
  -p, --project <PROJECT>  GCP Project ID to use
  -r, --raw_json           Output raw JSON
  -a, --auth_user        Authenticate with user application. otherwise authenticate with service account
  -h, --help               Print help information
```

- gcs
```
$ cli gcs --help
Execute GCS APIs

Usage: cli gcs [OPTIONS] --bucket <BUCKET> <COMMAND>

Commands:
  list             Show list objects
  object-metadata  Get object metadata
  get              Get object
  upload-file      Upload file
  delete           Delete object
  help             Print this message or the help of the given subcommand(s)

Options:
  -b, --bucket <BUCKET>  Bucket name
  -r, --raw_json         Output raw JSON
  -a, --auth_user        Authenticate with user application. otherwise authenticate with service account
  -h, --help             Print help
```

- df

Load CSV/new line delimited JSON/Parquet file and query the data using [Apache DataFusion](https://arrow.apache.org/datafusion/).

Input file will have a special name `t[0..n]` in SQL. For example, if you pass the input like `-i data_1.json -i data_2.json`, the query must be "select * from t0, t1". The `t0` is the first file `data_1.json` and `t1` is the second `data_2.json`.

Inpu/Output filename must have extension. Supported extensions are `json`, `csv` and `parquet`.

```
$ cli df --help
Execute DataFusion

Usage: cli df [OPTIONS] <COMMAND>

Commands:
  query  Query
  help   Print this message or the help of the given subcommand(s)

Options:
  -i, --inputs <INPUTS>  Input files
  -o, --output <OUTPUT>  Output file
  -h, --help             Print help
```
