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
  -h, --help               Print help information
```

- gcs
```
$ cli gcs --help
Usage: cli gcs --bucket <BUCKET> <COMMAND>

Commands:
  list-object  Show list objects
  help         Print this message or the help of the given subcommand(s)

Options:
  -b, --bucket <BUCKET>
  -h, --help             Print help information

```
