use crate::auth;
use bigquery::api::{
    JsonObject, JsonValue, QueryRequest, Table, TableCell, TableDataInsertAllRequest,
    TableDataInsertAllRequestRows, TableFieldSchema, TableReference, TableRow, TableSchema,
};
use bigquery::{hyper, hyper_rustls, Bigquery, Error, Result as GcpResult};
use chrono::prelude::*;
use google_bigquery2 as bigquery;

use anyhow;
use anyhow::Result;
use async_recursion::async_recursion;
use rayon::prelude::*;
use serde::ser::{Serialize as Serialize1, SerializeMap, SerializeSeq, Serializer};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use std::{string, thread};
use uuid::Uuid;

/// Project ID
type ProjectId = String;

/// Dataset ID
type DatasetId = String;

/// Table ID
type TableId = String;

pub struct Bq {
    /// BigQuery API endpoint
    api: Bigquery<hyper_rustls::HttpsConnector<hyper::client::connect::HttpConnector>>,

    /// GCP Project ID
    project: ProjectId,
    max_data: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BqProject {
    pub friendly_name: String,
    pub id: String,
    pub numeric_id: String,
}

#[derive(Clone, Debug)]
pub struct BqListParam {
    max_results: Option<u32>,
    page_token: Option<String>,
}

impl BqListParam {
    pub fn new() -> BqListParam {
        BqListParam {
            max_results: Default::default(),
            page_token: Default::default(),
        }
    }

    pub fn max_results(&mut self, max_results: u32) -> &mut Self {
        self.max_results = Some(max_results);
        self
    }

    pub fn page_token(&mut self, page_token: &str) -> &mut Self {
        self.page_token = Some(page_token.to_string());
        self
    }
}

#[derive(Clone, Debug)]
pub struct BqGetQueryResultParam {
    job_id: String,
    page_token: String,
    _max_results: u32,
}

impl BqGetQueryResultParam {
    pub fn new(job_id: &String, page_token: &String) -> Self {
        BqGetQueryResultParam {
            job_id: job_id.to_owned(),
            page_token: page_token.to_owned(),
            _max_results: 1000,
        }
    }

    pub fn max_results(&mut self, max_results: u32) -> &mut Self {
        self._max_results = max_results;
        self
    }
}

#[derive(Clone, Debug)]
pub struct BqQueryParam {
    _query: String,
    _use_legacy_sql: bool,
    _max_results: u32,
    _dry_ryn: bool,
}

impl BqQueryParam {
    pub fn new(query: &String) -> Self {
        BqQueryParam {
            _query: query.to_owned(),
            _use_legacy_sql: false,
            _max_results: 1000,
            _dry_ryn: false,
        }
    }

    pub fn use_legacy_sql(&mut self, legacy_sql: bool) -> &mut Self {
        self._use_legacy_sql = legacy_sql;
        self
    }

    pub fn max_results(&mut self, max_results: u32) -> &mut Self {
        self._max_results = max_results;
        self
    }

    pub fn dry_run(&mut self, dry_run: bool) -> &mut Self {
        self._dry_ryn = dry_run;
        self
    }

    fn to_query_request(&self) -> QueryRequest {
        let mut req = QueryRequest::default();
        req.query = Some(self._query.clone());
        req.max_results = Some(self._max_results);
        req.use_legacy_sql = Some(self._use_legacy_sql);
        req.dry_run = Some(self._dry_ryn);
        req
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BqDataset {
    pub dataset: DatasetId,
    pub project: ProjectId,
}

impl BqDataset {
    pub fn new(project: &str, dataset: &str) -> Self {
        BqDataset {
            dataset: dataset.to_owned(),
            project: project.to_owned(),
        }
    }
}

#[derive(Debug, Default)]
pub struct BqCreateTableParam {
    /// description about the table.
    description: Option<String>,

    /// Table Schema. You need to implement `BqSchemaBuilder` to set schema in the request.
    schema: Option<TableSchema>,
}

impl BqCreateTableParam {
    pub fn new() -> Self {
        BqCreateTableParam {
            description: None,
            schema: None,
        }
    }

    pub fn schema<T: BqSchemaBuilder>(&mut self) -> &mut Self {
        self.schema = Some(TableSchema {
            fields: Some(
                T::bq_schema()
                    .iter()
                    .map(|s| s.to_table_field_schema())
                    .collect(),
            ),
        });
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct BqInsertAllParam {
    dataset: DatasetId,
    table: TableId,
    skip_invalid_rows: bool,
    ignore_unknown_values: bool,
    trace_id: Option<String>,
}

impl BqInsertAllParam {
    pub fn new(dataset: &str, table: &str) -> Self {
        BqInsertAllParam {
            dataset: dataset.to_owned(),
            table: table.to_owned(),
            skip_invalid_rows: false,
            ignore_unknown_values: false,
            trace_id: None,
        }
    }

    pub fn skip_invalid_rows(&mut self, v: bool) -> &mut Self {
        self.skip_invalid_rows = v;
        self
    }

    pub fn ignore_unknown_value(&mut self, v: bool) -> &mut Self {
        self.ignore_unknown_values = v;
        self
    }

    pub fn set_trace_id(&mut self) -> &Option<String> {
        let uuid = Uuid::new_v4();
        self.trace_id = Some(uuid.to_string());
        &self.trace_id
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BqTable {
    pub dataset: BqDataset,
    pub table_id: TableId,
    pub schemas: Option<Vec<BqTableSchema>>,
    pub created_at: Option<u64>,
    pub expired_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BqTableSchema {
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub type_: BqType,
    pub mode: BqMode,
    pub fields: Box<Vec<BqTableSchema>>,
    pub description: Option<String>,
}

pub trait BqSchemaBuilder {
    fn bq_schema() -> Vec<BqTableSchema>;
}

impl BqTableSchema {
    fn to_table_field_schema(&self) -> TableFieldSchema {
        let mut schema = TableFieldSchema::default();
        schema.name = self.name.as_ref().map(|n| n.clone());
        schema.mode = match self.mode {
            BqMode::REQUIRED => Some("REQUIRED".to_string()),
            BqMode::NULLABLE => Some("NULLABLE".to_string()),
            BqMode::REPEATED => Some("REPEATED".to_string()),
            _ => None,
        };
        schema.type_ = match self.type_ {
            BqType::STRING => Some("STRING".to_string()),
            BqType::FLOAT => Some("NUMERIC".to_string()),
            BqType::INTEGER => Some("INTEGER".to_string()),
            BqType::BOOLEAN => Some("BOOLEAN".to_string()),
            BqType::TIMESTAMP => Some("TIMESTAMP".to_string()),
            BqType::RECORD => Some("RECORD".to_string()),
            _ => None,
        };
        let fields: Vec<TableFieldSchema> = self
            .fields
            .iter()
            .map(|f| f.to_table_field_schema())
            .collect();
        schema.fields = if 0 < fields.len() { Some(fields) } else { None };
        schema
    }

    fn from_table_field_schema(s: &TableFieldSchema) -> Self {
        let name = s.name.as_ref().unwrap_or(&"".to_string()).to_string();
        let type_ = match s.type_.as_ref().unwrap().as_str() {
            "STRING" => BqType::STRING,
            "FLOAT" => BqType::FLOAT,
            "INTEGER" => BqType::INTEGER,
            "NUMERIC" => BqType::FLOAT,
            "BOOLEAN" => BqType::BOOLEAN,
            "TIMESTAMP" => BqType::TIMESTAMP,
            "RECORD" => BqType::RECORD,
            _ => BqType::UNKNOWN,
        };
        let mode = match s.mode.as_ref().unwrap().as_str() {
            "REQUIRED" => BqMode::REQUIRED,
            "NULLABLE" => BqMode::NULLABLE,
            "REPEATED" => BqMode::REPEATED,
            _ => BqMode::UNKNOWN,
        };
        let schemas = s
            .fields
            .as_ref()
            .map(|fs| {
                fs.iter()
                    .map(|f| BqTableSchema::from_table_field_schema(&f))
                    .collect()
            })
            .unwrap_or(vec![]);

        BqTableSchema {
            name: Some(name),
            type_,
            mode,
            fields: Box::new(schemas),
            description: s.description.as_ref().map(|s| s.clone()),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BqMode {
    REQUIRED,
    NULLABLE,
    REPEATED,
    UNKNOWN,
}

/// BigQuery column type
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BqType {
    STRING,
    INTEGER,
    FLOAT,
    BOOLEAN,
    TIMESTAMP,
    RECORD,
    UNKNOWN,
}

#[derive(Debug, Deserialize)]
pub struct BqRow {
    /// To keep column order
    _name_index: HashMap<String, i32>,

    /// Actual columns
    columns: Vec<BqColumn>,
}

impl BqRow {
    pub fn new(columns: Vec<BqColumn>) -> Self {
        let name_index: HashMap<String, i32> = HashMap::from_iter(
            columns
                .iter()
                .enumerate()
                .map(|(i, c)| (c.name.as_ref().unwrap_or(&"".to_string()).clone(), i as i32)),
        );
        BqRow {
            _name_index: name_index,
            columns: columns,
        }
    }
    pub fn get(&self, key: &str) -> Option<&BqValue> {
        self._name_index
            .get(key)
            .map(|idx| &self.columns[*idx as usize].value)
    }

    pub fn columns(&self) -> &Vec<BqColumn> {
        &self.columns
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }
}

impl string::ToString for BqRow {
    fn to_string(&self) -> String {
        let columns_str = self
            .columns
            .iter()
            .map(|c| c.to_string())
            .filter(|v| 0 < v.len())
            .collect::<Vec<_>>()
            .join(",");
        format!("{{{}}}", columns_str)
    }
}

#[derive(Debug, Deserialize)]
pub struct BqColumn {
    /// column name
    name: Option<String>,
    /// value
    value: BqValue,
}

impl Serialize1 for BqColumn {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(&self.name.as_ref().unwrap_or(&"".to_string()), &self.value)?;
        map.end()
    }
}

impl Serialize1 for BqRow {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.columns.len()))?;
        for c in &self.columns {
            map.serialize_entry(&c.name.as_ref().unwrap_or(&"".to_string()), &c.value)?;
        }
        map.end()
    }
}

impl string::ToString for BqColumn {
    fn to_string(&self) -> String {
        match self.value {
            BqValue::BqNull => "".to_string(),
            _ => match &self.name {
                Some(name) => format!("\"{}\": {}", name, self.value.to_string()),
                None => format!("{}", self.value.to_string()),
            },
        }
    }
}

impl BqColumn {
    fn value_to_bq_value(v: Option<Value>, schema: &BqTableSchema) -> BqValue {
        v.map(|val| match val {
            Value::String(s) => match schema.type_ {
                BqType::STRING => BqValue::BqString(s),
                BqType::INTEGER => BqValue::BqInteger(s.parse::<i64>().unwrap_or(0)),
                BqType::FLOAT => BqValue::BqFloat(s.parse::<f64>().unwrap_or(0.0)),
                BqType::BOOLEAN => BqValue::BqBool(s == "true"),
                BqType::TIMESTAMP => BqValue::BqTimestamp(DateTime::from_utc(
                    NaiveDateTime::from_timestamp_opt(s.parse::<f64>().unwrap_or(0.0) as i64, 0)
                        .unwrap(),
                    Utc,
                )),
                _ => BqValue::BqNull,
            },
            Value::Number(n) => match schema.type_ {
                BqType::STRING => BqValue::BqString(n.to_string()),
                BqType::INTEGER => BqValue::BqInteger(n.as_i64().unwrap_or(0)),
                BqType::FLOAT => BqValue::BqFloat(n.as_f64().unwrap_or(0.0)),
                BqType::TIMESTAMP => BqValue::BqTimestamp(DateTime::from_utc(
                    NaiveDateTime::from_timestamp_opt(n.as_i64().unwrap_or(0), 0).unwrap(),
                    Utc,
                )),
                _ => BqValue::BqNull,
            },
            Value::Bool(b) => match schema.type_ {
                BqType::BOOLEAN => BqValue::BqBool(b),
                _ => BqValue::BqNull,
            },
            Value::Array(arr) => {
                let columns: Vec<Box<BqValue>> = arr
                    .iter()
                    .map(|s| Box::new(Self::value_to_bq_value(Some(s.clone()), &schema)))
                    .collect();
                BqValue::BqRepeated(columns)
            }
            Value::Object(o) => {
                if let Some(Value::Array(arr)) = &o.get("f") {
                    let columns: Vec<BqColumn> = arr
                        .iter()
                        .enumerate()
                        .map(|(i, s)| {
                            BqColumn::new(&TableCell { v: Some(s.clone()) }, &schema.fields[i])
                        })
                        .collect();
                    BqValue::BqStruct(BqRow::new(columns))
                } else if o.get("v").is_some() {
                    Self::value_to_bq_value(o.get("v").map(|v| v.clone()), &schema)
                } else {
                    BqValue::BqNull
                }
            }
            Value::Null => BqValue::BqNull,
        })
        .unwrap_or(BqValue::BqNull)
    }

    fn new(cell: &TableCell, schema: &BqTableSchema) -> Self {
        let name = schema.name.clone();
        let value = Self::value_to_bq_value(cell.v.as_ref().map(|v| v.clone()), schema);
        BqColumn { name, value }
    }

    pub fn name(&self) -> Option<String> {
        self.name.as_ref().map(|n| n.clone())
    }

    pub fn value(&self) -> &BqValue {
        &self.value
    }
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub enum BqValue {
    /// STRING
    BqString(String),
    /// INTEGER
    BqInteger(i64),
    /// FLOAT
    BqFloat(f64),
    /// BOOLEAN
    BqBool(bool),
    /// TIMESTAMP
    BqTimestamp(DateTime<Utc>),
    /// STRUCT
    BqStruct(BqRow),
    /// REPEATED(Array)
    BqRepeated(Vec<Box<BqValue>>),
    /// NULL
    BqNull,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Serialize)]
pub enum QueryResult {
    #[serde(rename = "schema")]
    Schema(Vec<BqTableSchema>),
    #[serde(rename = "data")]
    Data(Vec<BqRow>),
}

impl Serialize1 for BqValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            BqValue::BqString(s) => serializer.serialize_str(s),
            BqValue::BqInteger(n) => serializer.serialize_i64(*n),
            BqValue::BqFloat(n) => serializer.serialize_f64(*n),
            BqValue::BqBool(b) => serializer.serialize_bool(*b),
            BqValue::BqTimestamp(t) => serializer.serialize_str(&t.to_rfc3339()),
            BqValue::BqStruct(rs) => {
                let mut map = serializer.serialize_map(Some(rs.len()))?;
                for elem in &rs.columns {
                    map.serialize_entry(
                        &elem.name.as_ref().unwrap_or(&"".to_string()),
                        &elem.value,
                    )?;
                }
                map.end()
            }
            BqValue::BqRepeated(rs) => {
                let mut seq = serializer.serialize_seq(Some(rs.len()))?;
                for column in rs {
                    seq.serialize_element(&column)?;
                }
                seq.end()
            }
            BqValue::BqNull => serializer.serialize_none(),
        }
    }
}

impl string::ToString for BqValue {
    fn to_string(&self) -> String {
        match self {
            BqValue::BqString(s) => format!("\"{}\"", s),
            BqValue::BqInteger(n) => format!("{}", n),
            BqValue::BqFloat(n) => format!("{}", n),
            BqValue::BqBool(b) => format!("{}", b),
            BqValue::BqTimestamp(t) => format!("\"{}\"", t),
            BqValue::BqStruct(rs) => {
                let rs_str = rs
                    .columns
                    .iter()
                    .map(|r| r.to_string())
                    .filter(|v| 0 < v.len())
                    .collect::<Vec<_>>()
                    .join(",");
                format!("{{{}}}", rs_str)
            }
            BqValue::BqRepeated(rs) => {
                let rs_str = rs
                    .iter()
                    .map(|r| r.to_string())
                    .filter(|v| 0 < v.len())
                    .collect::<Vec<_>>()
                    .join(",");
                format!("[{}]", rs_str)
            }
            BqValue::BqNull => format!("null"),
        }
    }
}

impl BqTable {
    pub fn new(project_id: &str, dataset_id: &str, table_id: &str) -> BqTable {
        BqTable {
            dataset: BqDataset {
                project: project_id.to_owned(),
                dataset: dataset_id.to_owned(),
            },
            table_id: table_id.to_owned(),
            schemas: Default::default(),
            created_at: Default::default(),
            expired_at: Default::default(),
        }
    }
}

impl Bq {
    pub fn new(auth: &auth::GcpAuth, project: &str) -> Result<Bq> {
        let client = hyper::Client::builder().build(
            hyper_rustls::HttpsConnectorBuilder::new()
                .with_native_roots()
                .https_only()
                .enable_http1()
                .enable_http2()
                .build(),
        );
        let hub = Bigquery::new(client, auth.authenticator());
        Ok(Bq {
            api: hub,
            project: project.to_string(),
            max_data: 10,
        })
    }

    pub fn max_data(&mut self, max_data: usize) -> &mut Self {
        self.max_data = max_data;
        self
    }

    /// call list_project API.
    /// this will return list of project.
    pub async fn list_project(auth: auth::GcpAuth) -> Result<Vec<BqProject>> {
        let client = hyper::Client::builder().build(
            hyper_rustls::HttpsConnectorBuilder::new()
                .with_native_roots()
                .https_only()
                .enable_http1()
                .enable_http2()
                .build(),
        );
        let hub = Bigquery::new(client, auth.authenticator());
        // TODO: handle nex_page_token
        let res = hub.projects().list().doit().await;
        match Bq::handle_error(res) {
            Ok(result) => {
                let pss: Vec<BqProject> = match result.1.projects {
                    Some(ps) => ps
                        .par_iter()
                        .map(|p| BqProject {
                            friendly_name: p.friendly_name.as_ref().unwrap().clone(),
                            id: p.id.as_ref().unwrap().clone(),
                            numeric_id: p
                                .numeric_id
                                .map(|id| format!("{}", id))
                                .unwrap_or("".to_string()),
                        })
                        .collect(),
                    None => vec![],
                };
                Ok(pss)
            }
            Err(e) => Err(anyhow::anyhow!("{}", e)),
        }
    }

    /// call list_dataset API.
    /// this will return list of dataset.
    ///
    /// # Arguments
    ///
    /// * `p` - request parameters
    #[async_recursion]
    pub async fn list_dataset(
        &'async_recursion self,
        p: &'async_recursion BqListParam,
    ) -> Result<Vec<BqDataset>> {
        let mut list_api = self.api.datasets().list(&self.project);
        if let Some(max_results) = p.max_results {
            list_api = list_api.max_results(max_results);
        }
        if let Some(token) = &p.page_token {
            list_api = list_api.page_token(&token);
        }
        list_api = list_api.param(
            "fields",
            "datasets/id, datasets/datasetReference, nextPageToken",
        );
        let res = list_api.doit().await;
        match Bq::handle_error(res) {
            Ok(result) => {
                let mut dss: Vec<BqDataset> = match result.1.datasets {
                    Some(ds) => ds
                        .par_iter()
                        .map(|d| {
                            d.dataset_reference.as_ref().map(|dr| {
                                let dataset = dr
                                    .dataset_id
                                    .as_ref()
                                    .unwrap_or(&"".to_string())
                                    .to_string();
                                let project = dr
                                    .project_id
                                    .as_ref()
                                    .unwrap_or(&"".to_string())
                                    .to_string();
                                BqDataset { dataset, project }
                            })
                        })
                        .filter_map(|v| v)
                        .collect(),
                    None => vec![],
                };
                if let Some(token) = result.1.next_page_token {
                    let mut param = p.clone();
                    param.page_token(&token);
                    let additionals = self.list_dataset(&param).await?;
                    dss.extend(additionals);
                };

                Ok(dss)
            }
            Err(e) => Err(anyhow::anyhow!("{}", e)),
        }
    }

    fn to_bq_table(&self, t: Table) -> BqTable {
        let default = "".to_string();
        let schemas = if let Some(schema) = t.schema {
            self.to_schemas(&schema)
        } else {
            vec![]
        };
        let (dataset_id, table_id) = t
            .table_reference
            .as_ref()
            .map(|tr| {
                let dataset_id = tr.dataset_id.as_ref().unwrap_or(&default);
                let table_id = tr.table_id.as_ref().unwrap_or(&default);
                (dataset_id, table_id)
            })
            .unwrap_or((&default, &default));
        BqTable {
            dataset: BqDataset {
                project: self.project.clone(),
                dataset: dataset_id.clone(),
            },
            table_id: table_id.clone(),
            schemas: Some(schemas),
            created_at: t.creation_time.map(|t| t as u64),
            expired_at: t.expiration_time.map(|t| t as u64),
        }
    }

    pub async fn get_table(&self, dataset: &DatasetId, table: &TableId) -> Result<BqTable> {
        let api = self.api.tables().get(&self.project, &dataset, table);
        let res = api.doit().await;
        match Bq::handle_error(res) {
            Ok(result) => {
                let table = self.to_bq_table(result.1);
                Ok(table)
            }
            Err(e) => Err(anyhow::anyhow!("{}", e)),
        }
    }

    /// Call tables insert API.
    ///
    /// # Arguments
    ///
    /// * `dataset` - dataset for table
    /// * `table` - target table name
    /// * `p` - request parameters
    pub async fn create_table(
        &self,
        dataset: &DatasetId,
        table: &TableId,
        p: BqCreateTableParam,
    ) -> Result<BqTable> {
        let mut req = Table::default();
        req.table_reference = Some(TableReference {
            dataset_id: Some(dataset.clone()),
            project_id: Some(self.project.clone()),
            table_id: Some(table.clone()),
        });
        if let Some(desc) = p.description {
            req.description = Some(desc);
        }
        if let Some(schema) = p.schema {
            req.schema = Some(schema);
        }
        let api = self.api.tables().insert(req, &self.project, &dataset);
        let res = api.doit().await;
        match Bq::handle_error(res) {
            Ok(result) => {
                println!("{:?}", result.1);
                Ok(self.to_bq_table(result.1))
            }
            Err(e) => Err(anyhow::anyhow!("{}", e)),
        }
    }

    /// Call tables delete API.
    ///
    /// # Arguments
    ///
    /// * `dataset` - dataset for table
    /// * `table` - target table name
    /// * `p` - request parameters
    pub async fn delete_table(&self, dataset: &DatasetId, table: &TableId) -> Result<()> {
        let api = self.api.tables().delete(&self.project, &dataset, &table);
        let res = api.doit().await;
        match Bq::handle_error(res) {
            Ok(result) => {
                println!("{:?}", result);
                Ok(())
            }
            Err(e) => Err(anyhow::anyhow!("{}", e)),
        }
    }

    /// Call tables.list API
    ///
    /// This will return only table id(project id and dataset id) and timestamp for now.
    #[async_recursion]
    pub async fn list_tables(
        &'async_recursion self,
        dataset: &'async_recursion DatasetId,
        p: &'async_recursion BqListParam,
    ) -> Result<Vec<BqTable>> {
        let mut list_api = self.api.tables().list(&self.project, &dataset);
        if let Some(max_results) = p.max_results {
            list_api = list_api.max_results(max_results);
        }
        if let Some(token) = &p.page_token {
            list_api = list_api.page_token(&token);
        }
        list_api = list_api.param("fields",
            "tables/id, tables/tableReference, tables/creationTime, tables/expirationTime, nextPageToken, totalItems");
        let res = list_api.doit().await;
        //println!("{:?}", res);
        match Bq::handle_error(res) {
            Ok(result) => {
                let mut tables: Vec<BqTable> = match result.1.tables {
                    Some(ts) => ts
                        .par_iter()
                        .map(|t| {
                            let default = "".to_string();
                            let (dataset_id, table_id) = t
                                .table_reference
                                .as_ref()
                                .map(|tr| {
                                    let dataset_id = tr.dataset_id.as_ref().unwrap_or(&default);
                                    let table_id = tr.table_id.as_ref().unwrap_or(&default);
                                    (dataset_id, table_id)
                                })
                                .unwrap_or((&default, &default));
                            BqTable {
                                dataset: BqDataset::new(&self.project, &dataset_id),
                                table_id: table_id.to_string(),
                                schemas: None,
                                created_at: t.creation_time.map(|t| t as u64),
                                expired_at: t.expiration_time.map(|t| t as u64),
                            }
                        })
                        .collect(),
                    None => vec![],
                };
                if let Some(token) = result.1.next_page_token {
                    let mut param = p.clone();
                    param.page_token(&token);
                    let additionals = self.list_tables(dataset, &param).await?;
                    tables.extend(additionals);
                };

                Ok(tables)
            }
            Err(e) => Err(anyhow::anyhow!(format!("{}", e))),
        }
    }

    #[async_recursion]
    async fn get_query_results(
        &'async_recursion self,
        p: &'async_recursion BqGetQueryResultParam,
    ) -> Result<Vec<BqRow>> {
        let api = self
            .api
            .jobs()
            .get_query_results(&self.project, &p.job_id)
            .page_token(&p.page_token)
            .max_results(p._max_results);
        let resp = Bq::handle_error(api.doit().await);
        match resp {
            Ok(result) => {
                //println!("{:?}", result);
                let bq_rows: Vec<BqRow> =
                    if let (Some(schema), Some(rows)) = (result.1.schema, result.1.rows) {
                        let mut tmp_rows: Vec<BqRow> = self.to_rows(&schema, &rows);
                        if let Some(token) = &result.1.page_token {
                            let mut param = BqGetQueryResultParam::new(
                                &result
                                    .1
                                    .job_reference
                                    .map(|jr| jr.job_id.unwrap_or("".to_string()))
                                    .unwrap_or("".to_string()),
                                token,
                            );
                            param.max_results(p._max_results);
                            tmp_rows.extend(self.get_query_results(&param).await?);
                        }
                        tmp_rows
                    } else {
                        vec![]
                    };

                Ok(bq_rows)
            }
            Err(e) => Err(anyhow::anyhow!(format!("{}", e))),
        }
    }

    fn to_schemas(&self, schema: &TableSchema) -> Vec<BqTableSchema> {
        schema
            .fields
            .as_ref()
            .map(|fields| {
                fields
                    .iter()
                    .map(|f| BqTableSchema::from_table_field_schema(f))
                    .collect()
            })
            .unwrap_or(vec![])
    }

    fn to_rows(&self, schema: &TableSchema, rows: &Vec<TableRow>) -> Vec<BqRow> {
        schema
            .fields
            .as_ref()
            .map(|fields| {
                let schemas: Vec<BqTableSchema> = fields
                    .iter()
                    .map(|f| BqTableSchema::from_table_field_schema(f))
                    .collect();
                rows.par_iter()
                    .map(|row| {
                        let columns: Vec<BqColumn> = match &row.f {
                            Some(cs) => cs
                                .iter()
                                .enumerate()
                                .map(|(i, c)| BqColumn::new(c, &schemas[i]))
                                .collect(),
                            None => vec![],
                        };
                        BqRow::new(columns)
                    })
                    .collect()
            })
            .unwrap_or(vec![])
    }

    /// Execute query.
    ///
    /// If 'dry_run' parameter is set, result would be the result table schema.
    ///
    /// # Arguments
    ///
    /// * `p` - request parameters.
    #[async_recursion]
    pub async fn query(
        &'async_recursion self,
        p: &'async_recursion BqQueryParam,
    ) -> Result<QueryResult> {
        let req = p.to_query_request();
        let query_api = self.api.jobs().query(req, &self.project);
        let resp = Bq::handle_error(query_api.doit().await);
        match resp {
            Ok(result) => {
                //println!("{:?}", result);
                if p._dry_ryn {
                    let schemas = if let Some(schema) = result.1.schema {
                        self.to_schemas(&schema)
                    } else {
                        vec![]
                    };
                    Ok(QueryResult::Schema(schemas))
                } else {
                    // TODO: should return total rows for local memory
                    //let total_rows = result.1.total_rows.map(|n| n.parse().unwrap_or(-1)).unwrap_or(-1);
                    let bq_rows: Vec<BqRow> =
                        if let (Some(schema), Some(rows)) = (result.1.schema, result.1.rows) {
                            let mut tmp_rows: Vec<BqRow> = self.to_rows(&schema, &rows);
                            if let Some(token) = &result.1.page_token {
                                let mut param = BqGetQueryResultParam::new(
                                    &result
                                        .1
                                        .job_reference
                                        .map(|jr| jr.job_id.unwrap_or("".to_string()))
                                        .unwrap_or("".to_string()),
                                    token,
                                );
                                param.max_results(p._max_results);
                                let resp = self.get_query_results(&param).await;
                                match resp {
                                    Ok(result) => tmp_rows.extend(result),
                                    _ => println!("{:?}", resp),
                                }
                            }
                            tmp_rows
                        } else {
                            vec![]
                        };
                    Ok(QueryResult::Data(bq_rows))
                }
            }
            Err(e) => Err(anyhow::anyhow!(format!("{}", e))),
        }
    }

    fn handle_error<T>(result: GcpResult<T>) -> Result<T> {
        match result {
            Err(e) => match e {
                Error::HttpError(_)
                | Error::Io(_)
                | Error::MissingAPIKey
                | Error::MissingToken(_)
                | Error::Cancelled
                | Error::UploadSizeLimitExceeded(_, _)
                | Error::Failure(_)
                | Error::BadRequest(_)
                | Error::FieldClash(_)
                | Error::JsonDecodeError(_, _) => {
                    eprintln!("{}", e);
                    Err(anyhow::anyhow!("{}", e))
                }
            },
            Ok(res) => Ok(res),
        }
    }

    /// Call insert_all API.
    ///
    /// # Arguments
    ///
    /// * `data` - loading data
    /// * `p` - request parameters
    pub async fn insert_all<T: Serialize + BqSchemaBuilder>(
        self,
        data: Vec<T>,
        p: BqInsertAllParam,
    ) -> Result<()> {
        let mut create_param = BqCreateTableParam::new();
        create_param.schema::<T>();
        println!(
            "{:?}",
            self.create_table(&p.dataset, &p.table, create_param).await
        );

        let content: Vec<TableDataInsertAllRequestRows> = data
            .iter()
            .map(|d| {
                let jstring = serde_json::to_string(d).unwrap();
                let origin: HashMap<String, Value> = serde_json::from_str(&jstring).unwrap();
                let content: HashMap<String, JsonValue> =
                    origin.into_iter().map(|(k, v)| (k, JsonValue(v))).collect();
                let mut rows = TableDataInsertAllRequestRows::default();
                rows.json = Some(JsonObject(Some(content)));
                rows
            })
            .collect();
        let mut req = TableDataInsertAllRequest::default();
        req.ignore_unknown_values = Some(p.ignore_unknown_values);
        req.skip_invalid_rows = Some(p.skip_invalid_rows);
        req.rows = Some(content);

        self.call_insert_all(&p, &req, 0).await
    }

    /// Call insert_all API recursively.
    ///
    /// We have to wait until the table become available if the table was created right before
    /// calling this function.
    #[async_recursion]
    async fn call_insert_all(
        &self,
        p: &BqInsertAllParam,
        req: &TableDataInsertAllRequest,
        retry_count: u64,
    ) -> Result<()> {
        let mut insert_all =
            self.api
                .tabledata()
                .insert_all(req.clone(), &self.project, &p.dataset, &p.table);
        if let Some(trace_id) = p.trace_id.clone() {
            insert_all = insert_all.param("traceid", &trace_id);
        }

        let res = insert_all.doit().await;
        match res {
            Err(e) => match e {
                Error::BadRequest(_) => {
                    if 5 < retry_count {
                        eprintln!("{}", e);
                        Err(anyhow::anyhow!("{}", e))
                    } else {
                        let interval = 100 * retry_count.pow(2);
                        // eprintln!("{}, {}", e, interval);
                        thread::sleep(Duration::from_millis(interval));
                        self.call_insert_all(p, req, retry_count + 1).await
                    }
                }
                Error::HttpError(_)
                | Error::Io(_)
                | Error::MissingAPIKey
                | Error::MissingToken(_)
                | Error::Cancelled
                | Error::UploadSizeLimitExceeded(_, _)
                | Error::Failure(_)
                | Error::FieldClash(_)
                | Error::JsonDecodeError(_, _) => {
                    eprintln!("{}", e);
                    Err(anyhow::anyhow!("{}", e))
                }
            },
            Ok(_) => Ok(()),
        }
    }

    /// Call list_tabledata API.
    ///
    /// Notice: This will return whole table data.
    ///
    /// # Arguments
    ///
    /// * `table` - target table
    /// * `p` - request parameters
    #[async_recursion]
    pub async fn list_tabledata(
        &'async_recursion self,
        table: &'async_recursion BqTable,
        p: &'async_recursion BqListParam,
    ) -> Result<Vec<BqRow>> {
        let table_info = self.api.tables().get(
            &table.dataset.project,
            &table.dataset.dataset,
            &table.table_id,
        );
        let mut list_api = self.api.tabledata().list(
            &table.dataset.project,
            &table.dataset.dataset,
            &table.table_id,
        );
        if let Some(max_results) = p.max_results {
            list_api = list_api.max_results(max_results);
        }
        if let Some(token) = &p.page_token {
            list_api = list_api.page_token(&token);
        }
        let table_result_future = table_info.doit();
        let result_future = list_api.doit();
        let (table_result, result) = tokio::join!(table_result_future, result_future);
        //println!("{:?}", table_result);
        //println!("{:?}", result);
        let bq_rows: Vec<BqRow> = if let (Ok(tres), Ok(res)) =
            (Bq::handle_error(table_result), Bq::handle_error(result))
        {
            let empty: Vec<TableRow> = vec![];
            // TODO: should return total rows for local memory
            //let total_rows = res.1.total_rows.map(|n| n.parse().unwrap_or(-1)).unwrap_or(-1);
            let rows = res.1.rows.as_ref().unwrap_or(&empty);
            //println!("{:?}", res);
            let mut tmp_rows: Vec<BqRow> = tres
                .1
                .schema
                .as_ref()
                .map(|schema| self.to_rows(schema, rows))
                .unwrap_or(vec![]);
            if let Some(token) = &res.1.page_token {
                let mut param = p.clone();
                param.page_token(&token);
                tmp_rows.extend(self.list_tabledata(table, &param).await?);
            }
            tmp_rows
        } else {
            vec![]
        };

        Ok(bq_rows)
    }
}
