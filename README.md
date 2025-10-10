# gcprs

Rust wrapper library for Google Cloud Platform APIs.

## Overview

`gcprs` provides a simplified, type-safe interface to interact with various Google Cloud Platform services. This library wraps the official Google API client libraries and provides idiomatic Rust APIs with proper error handling and async support.

## Features

The library is organized into feature-gated modules, allowing you to include only the APIs you need:

- **`bigquery`** - BigQuery API for data warehousing and analytics
- **`gcs`** - Google Cloud Storage API for object storage
- **`drive`** - Google Drive API for file storage and management
- **`pubsub`** - Cloud Pub/Sub API for messaging
- **`sheets`** - Google Sheets API for spreadsheet operations
- **`secretmanager`** - Secret Manager API for sensitive data storage
- **`run`** - Cloud Run API for serverless container deployment
- **`aiplatform`** - AI Platform API for machine learning services
- **`full`** - Enables all features

## Installation

Add `gcprs` to your `Cargo.toml`:

```toml
[dependencies]
gcprs = { path = "path/to/gcprs", features = ["bigquery", "gcs"] }
```

Or to enable all features:

```toml
[dependencies]
gcprs = { path = "path/to/gcprs", features = ["full"] }
```

## Authentication

The library supports two authentication methods. API return objects can be serialized/deserialized to/from JSON.

### Service Account Authentication

Uses Application Default Credentials (ADC). Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to your service account key file, or run on GCP infrastructure (Compute Engine, Cloud Run, etc.) to use metadata server authentication.

```rust
use gcprs::auth::GcpAuth;

let auth = GcpAuth::from_service_account().await?;
```

### OAuth2 User Authentication

For applications requiring user consent. Set the `GOOGLE_APPLICATION_SECRET` environment variable to your OAuth2 client secret file path. Please refer to the [Google Document](https://developers.google.com/identity/protocols/oauth2) for OAuth2 setup.

```rust
use gcprs::auth::GcpAuth;

let auth = GcpAuth::from_user_auth().await?;
```

## API Modules

### BigQuery (`bigquery`)

Interact with BigQuery for data warehousing and analytics.

**Available APIs:**
- `list_project` - List accessible BigQuery projects
- `list_dataset` - List datasets in a project
- `list_tables` - List tables in a dataset
- `get_table` - Get table metadata
- `create_table` - Create a new table
- `delete_table` - Delete a table
- `insert_all` - Upload Rust objects into table (streaming insert). Table schema is generated from trait
- `list_tabledata` - Retrieve table data
- `query` - Execute SQL query
- `query_to_table` - Execute query and save results to a table
- `wait_job_complete` - Wait for async job completion

**Example:**
```rust
use gcprs::{auth::GcpAuth, bigquery::{Bq, BqQueryParam}};

let auth = GcpAuth::from_service_account().await?;
let bq = Bq::new(&auth, "my-project")?;

let mut param = BqQueryParam::new(&"SELECT * FROM dataset.table LIMIT 10".to_string());
let result = bq.query(&param).await?;
```

**Related Google Cloud APIs:**
- [BigQuery REST API](https://cloud.google.com/bigquery/docs/reference/rest)
- Endpoints: Projects, Datasets, Tables, Jobs, Tabledata

### Google Cloud Storage (`gcs`)

Object storage for unstructured data.

**Available APIs:**
- `list_buckets` - List buckets in a project
- `list_objects` - List objects in a bucket with filtering
- `get_object` - Download object content
- `get_object_metadata` - Retrieve object metadata only
- `get_object_stream` - Get object as a stream for large files
- `delete_object` - Delete an object
- `insert_object` - Upload object (resumable upload)
  - `insert_string` - Upload from String
  - `insert_file` - Upload from file path

**Example:**
```rust
use gcprs::{auth::GcpAuth, gcs::{Gcs, GcsObject, GcsListParam}};

let auth = GcpAuth::from_service_account().await?;
let gcs = Gcs::new(&auth, "my-bucket".to_string());

let mut param = GcsListParam::new();
param.prefix("path/to/folder/");
let objects = gcs.list_objects(&param).await?;
```

**Related Google Cloud APIs:**
- [Cloud Storage JSON API](https://cloud.google.com/storage/docs/json_api)
- Endpoints: Buckets, Objects

### Google Drive (`drive`)

File storage and collaboration with Google Workspace.

**Available APIs:**
- `create_file` - Upload a new file
- `update_file` - Update existing file content
- `list_files` - Search files with query support
- `get_file_meta_by_id` - Get file metadata by ID
- `get_file` - Download file content
  - `get_file_by_id` - Download by file ID
- `export_file` - Export Google Workspace documents (Docs, Sheets, Slides) to various formats
  - `export_file_by_id` - Export by file ID

**Export Formats:**
- Documents: Word (.docx), PDF, HTML, Plain Text, EPUB, RTF, OpenDocument
- Spreadsheets: Excel (.xlsx), PDF, CSV, TSV, HTML, OpenDocument
- Presentations: PowerPoint (.pptx), PDF, Plain Text, ODP

**Example:**
```rust
use gcprs::{auth::GcpAuth, drive::{Drive, DriveListParam, SheetExportMimeType}};

let auth = GcpAuth::from_user_auth().await?;
let drive = Drive::new(&auth);

let mut param = DriveListParam::new();
param.query("mimeType='application/vnd.google-apps.spreadsheet'");
let files = drive.list_files(&param).await?;

// Export a Google Sheet to Excel
drive.export_file_by_id("file_id", SheetExportMimeType::Excel).await?;
```

**Related Google Cloud APIs:**
- [Drive API v3](https://developers.google.com/drive/api/v3/reference)
- Endpoints: Files

### Cloud Pub/Sub (`pubsub`)

Asynchronous messaging and event streaming.

**Available APIs:**
- `publish` - Publish messages to a topic (with automatic retry)
- `pull_subscription` - Pull messages from subscription with automatic acknowledgment

**Example:**
```rust
use gcprs::{auth::GcpAuth, pubsub::{PubSub, PublishParam, SubscriptionParam}};

let auth = GcpAuth::from_service_account().await?;
let pubsub = PubSub::new(&auth)?;

// Publish
let param = PublishParam::new("my-project", "my-topic");
pubsub.publish(&param, b"Hello, Pub/Sub!".to_vec()).await?;

// Subscribe and process
let param = SubscriptionParam::new("my-project", "my-subscription");
let messages = pubsub.pull_subscription(param, |data| {
    // Process message
    println!("Received: {:?}", data);
    Ok(())
}).await?;
```

**Related Google Cloud APIs:**
- [Pub/Sub REST API](https://cloud.google.com/pubsub/docs/reference/rest)
- Endpoints: Topics, Subscriptions

### Google Sheets (`sheets`)

Spreadsheet operations for data manipulation and reporting.

**Available APIs:**
- Read and write spreadsheet data
- Batch updates
- Cell formatting and styling
- Sheet management

**Example:**
```rust
use gcprs::{auth::GcpAuth, sheets::Sheets};

let auth = GcpAuth::from_user_auth().await?;
let sheets = Sheets::new(&auth);

// Read data
let values = sheets.get_values("spreadsheet_id", "Sheet1!A1:D10").await?;
```

**Related Google Cloud APIs:**
- [Sheets API v4](https://developers.google.com/sheets/api/reference/rest)
- Endpoints: Spreadsheets, Values

### Secret Manager (`secretmanager`)

Secure storage and management of sensitive data like API keys, passwords, and certificates.

**Available APIs:**
- Access secret values
- List secrets and versions
- Secure retrieval with proper authentication

**Related Google Cloud APIs:**
- [Secret Manager API](https://cloud.google.com/secret-manager/docs/reference/rest)
- Endpoints: Secrets, Versions

### Cloud Run (`run`)

Fully managed serverless platform for deploying containerized applications.

**Available APIs:**
- **Services:**
  - `services_get` - Get service details
  - `services_list` - List services in a location

- **Jobs:**
  - `jobs_get` - Get job details
  - `jobs_list` - List jobs in a location
  - `jobs_create` - Create a new job
  - `jobs_delete` - Delete a job
  - `jobs_run` - Execute a job on-demand

- **Executions:**
  - `executions_get` - Get execution details
  - `executions_list` - List executions for a job
  - `executions_delete` - Delete an execution

**Example:**
```rust
use gcprs::{auth::GcpAuth, run::{CloudRun, job::{Job, RunJobName}}};

let auth = GcpAuth::from_service_account().await?;
let run = CloudRun::new(&auth)?;

let job_name = RunJobName::new("my-project", "us-central1", "my-job");
let job = run.jobs_get(&job_name).await?;

// Run the job
run.jobs_run(&job_name).await?;
```

**Related Google Cloud APIs:**
- [Cloud Run API v2](https://cloud.google.com/run/docs/reference/rest)
- Endpoints: Services, Jobs, Executions

### AI Platform (`aiplatform`)

Machine learning model deployment and serving (Beta).

**Available APIs:**
- Model management
- Endpoint deployment
- Prediction requests

**Related Google Cloud APIs:**
- [Vertex AI API](https://cloud.google.com/vertex-ai/docs/reference/rest)

## API Relationships and Integration Patterns

### Authentication Flow
All APIs use the same authentication mechanism provided by `gcprs::auth`:

```
┌─────────────────────────────────────────┐
│         GcpAuth                         │
│  ┌───────────────────────────────────┐  │
│  │ Service Account / OAuth2          │  │
│  │ (yup-oauth2 + IAM Credentials)    │  │
│  └───────────────────────────────────┘  │
└─────────────────────────────────────────┘
                    │
        ┌───────────┴───────────┐
        │                       │
        ▼                       ▼
┌──────────────┐        ┌──────────────┐
│  BigQuery    │        │    GCS       │
│  Pub/Sub     │        │    Drive     │
│  Sheets      │        │  Cloud Run   │
│  Secret Mgr  │        │  AI Platform │
└──────────────┘        └──────────────┘
```

### Common Integration Patterns

#### 1. Data Pipeline: BigQuery ↔ Cloud Storage
```
BigQuery (export) → GCS → BigQuery (load)
```
- Export BigQuery query results to GCS for archiving
- Load CSV/JSON data from GCS into BigQuery tables
- Transfer data between projects via GCS

#### 2. Event-Driven Architecture: Pub/Sub → Cloud Run
```
Event Source → Pub/Sub Topic → Cloud Run Job → Data Processing
```
- Trigger Cloud Run jobs from Pub/Sub messages
- Process events asynchronously at scale
- Decouple event producers from consumers

#### 3. Secure Configuration: Secret Manager → Cloud Run
```
Secret Manager (secrets) → Cloud Run (env vars/volumes)
```
- Store API keys, database credentials in Secret Manager
- Mount secrets as environment variables or files in Cloud Run
- Rotate secrets without redeploying

#### 4. Data Export & Reporting: BigQuery → Sheets
```
BigQuery (query) → Process → Drive/Sheets (report)
```
- Export query results for business reporting
- Generate dashboards in Google Sheets
- Automate periodic reports

#### 5. Data Lake Pattern: Multiple Sources → GCS → BigQuery
```
Drive/Sheets → GCS → BigQuery → Analysis
```
- Centralize data from various sources in GCS
- Load into BigQuery for analytics
- Query and visualize results

## Error Handling

All APIs use `anyhow::Result<T>` for comprehensive error handling. The library properly converts underlying Google API errors.

```rust
use anyhow::Result;

async fn example() -> Result<()> {
    let auth = GcpAuth::from_service_account().await?;
    let bq = Bq::new(&auth, "my-project")?;

    match bq.query(&param).await {
        Ok(result) => {
            println!("Query successful");
            // Process result
        }
        Err(e) => {
            eprintln!("Query failed: {:?}", e);
            // Handle error
        }
    }
    Ok(())
}
```

## Project Structure

```
gcprs/
├── src/
│   ├── lib.rs               # Library entry point
│   ├── auth.rs              # Modern authentication (OAuth2, Service Account)
│   ├── auth_legacy.rs       # Legacy authentication support
│   ├── metadata.rs          # GCP metadata server client
│   ├── common/              # Common utilities
│   │   └── error.rs         # Error types and handling
│   ├── bigquery.rs          # BigQuery API wrapper
│   ├── gcs.rs               # Cloud Storage API wrapper
│   ├── drive.rs             # Drive API wrapper
│   ├── pubsub.rs            # Pub/Sub API wrapper
│   ├── sheets.rs            # Sheets API wrapper
│   ├── secretmanager.rs    # Secret Manager API wrapper
│   ├── aiplatform.rs        # AI Platform API wrapper
│   └── run/                 # Cloud Run API wrapper
│       ├── mod.rs           # Cloud Run client
│       ├── job.rs           # Job management
│       ├── service.rs       # Service management
│       └── execution.rs     # Execution tracking
├── cli/                     # Command-line interface
│   ├── src/
│   │   └── main.rs
│   └── Cargo.toml
└── Cargo.toml               # Main workspace configuration
```

## Core Dependencies

- **Google APIs**: `google-bigquery2`, `google-storage1`, `google-drive3`, `google-pubsub1`, `google-run2`, etc.
- **Authentication**: `yup-oauth2`, `google-iamcredentials1`
- **HTTP Client**: `hyper`, `hyper-rustls`, `http-body-util`
- **Async Runtime**: `tokio` (with full features)
- **Serialization**: `serde`, `serde_json`
- **Error Handling**: `anyhow`
- **Other**: `chrono`, `rayon`, `urlencoding`, `uuid`, `mime`, `jsonwebtoken`

## CLI Tool

The workspace includes a CLI tool (`cli/`) that demonstrates API usage and provides command-line access to GCP services. It includes DataFusion integration for advanced data processing.

## License

See the LICENSE file in the repository root for license information.

## Contributing

Contributions are welcome! Please ensure:
- All tests pass
- Code follows existing style conventions
- Documentation is updated for new features
- Error handling is comprehensive

## References

- [Google Cloud Platform Documentation](https://cloud.google.com/docs)
- [Google APIs Explorer](https://developers.google.com/apis-explorer)
- [Rust Google APIs (google-apis-rs)](https://github.com/Byron/google-apis-rs)
- [OAuth 2.0 for Client-side Web Applications](https://developers.google.com/identity/protocols/oauth2)
