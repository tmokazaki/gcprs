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

Load CSV/new line delimitted JSON/Parquet file and query the data using [Apache DataFusion](https://arrow.apache.org/datafusion/).

Input file will have a special table name like `t[0..n]` in SQL. For example, if you pass the input like `-i data_1.json -i data_2.json`, the query must be "select * from t0, t1". The `t0` is the first file `data_1.json` and `t1` is the second `data_2.json`.

Inpu/Output filename must have extension. Supported extensions are `json`, `csv` and `parquet`.

```
$ cli df --help
Execute DataFusion

Usage: cli df [OPTIONS] <COMMAND>

Commands:
  query
          Execute query
  schema
          Show schema
  help
          Print this message or the help of the given subcommand(s)

Options:
  -i, --inputs <INPUTS>
          Input files.

          You can use glob format for a single table. Multiple tables are also supported. To use it, add `-i <filename>` arguments as you need.

  -j, --json
          Output raw JSON

  -o, --output <OUTPUT>
          Output file. Optional.

          The result is always shown in stdout. This option write the result to the file.

  -r, --remove
          If Output argument file exists, force to remove

  -h, --help
          Print help (see a summary with '-h')
```

- ml

Execute machine learning algorithm. Load CSV/new line delimitted JSON/Parquet file and exeucte. Loading file is same as `df` commoand.

The features used by the algorthm are set by column name. If you want to use multiple features, neet to set multiple column arguments. The column must be a numeric data type like UInt16, Float64 etc.
Result table in stdout has `label` column accordingly.

```
$ cli ml --help
Execute ML

Usage: cli ml [OPTIONS] <COMMAND>

Commands:
  dbscan
          DBScan
  kmeans
          KMeans
  help
          Print this message or the help of the given subcommand(s)

Options:
  -i, --inputs <INPUTS>
          Input files.

          You can use glob format for a single table. Multiple tables are also supported. To use it, add `-i <filename>` arguments as you need.

  -j, --json
          Output raw JSON

  -s, --stats
          Output statistics

  -o, --output <OUTPUT>
          Output file. Optional.

          The result is always shown in stdout. This option write the result to the file.

  -r, --remove
          If Output argument file exists, force to remove

  -h, --help
          Print help (see a summary with '-h')
```
