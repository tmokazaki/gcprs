use crate::auth;
use bigquery::api::{Content, QueryRequest, TableCell, TableFieldSchema, TableRow, TableSchema};
use bigquery::{hyper, hyper_rustls, Bigquery};
use chrono::prelude::*;
use google_bigquery2 as bigquery;

use anyhow;
use anyhow::Result;
use async_recursion::async_recursion;
use rayon::prelude::*;
use serde::ser::{Serialize as Serialize1, SerializeMap, SerializeSeq, Serializer};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::string;

type ProjectId = String;
type DatasetId = String;
type TableId = String;

pub struct Bq {
    api: Bigquery<hyper_rustls::HttpsConnector<hyper::client::connect::HttpConnector>>,
    project: ProjectId,
}

#[derive(Clone, Debug)]
pub struct BqListParam {
    _max_results: Option<u32>,
    _page_token: Option<String>,
}

impl BqListParam {
    pub fn new() -> BqListParam {
        BqListParam {
            _max_results: Default::default(),
            _page_token: Default::default(),
        }
    }

    pub fn max_results(&mut self, max_results: u32) -> &mut Self {
        self._max_results = Some(max_results);
        self
    }

    pub fn page_token(&mut self, page_token: &str) -> &mut Self {
        self._page_token = Some(page_token.to_string());
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
}

impl BqQueryParam {
    pub fn new(query: &String) -> Self {
        BqQueryParam {
            _query: query.to_owned(),
            _use_legacy_sql: false,
            _max_results: 1000,
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

    fn to_query_request(&self) -> QueryRequest {
        let mut req = QueryRequest::default();
        req.query = Some(self._query.clone());
        req.max_results = Some(self._max_results);
        req.use_legacy_sql = Some(self._use_legacy_sql);
        req
    }
}

#[derive(Clone, Debug)]
pub struct BqDataset {
    dataset: DatasetId,
    project: ProjectId,
}

impl BqDataset {
    pub fn new(project: &str, dataset: &str) -> Self {
        BqDataset {
            dataset: dataset.to_owned(),
            project: project.to_owned(),
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct BqTable {
    dataset: BqDataset,
    table_id: TableId,
    created_at: Option<u64>,
    expired_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BqTableSchema {
    name: Option<String>,
    #[serde(rename = "type")]
    type_: BqType,
    mode: BqMode,
    fields: Box<Vec<BqTableSchema>>,
    description: Option<String>,
}

impl BqTableSchema {
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
enum BqMode {
    REQUIRED,
    NULLABLE,
    REPEATED,
    UNKNOWN,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum BqType {
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
    _name_index: HashMap<String, i32>,
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
    name: Option<String>,
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
    fn new(cell: &TableCell, schema: &BqTableSchema) -> Self {
        let name = schema.name.clone();
        let value = match &schema.type_ {
            BqType::STRING => {
                if let Content::Value(s) = &cell.v {
                    BqValue::BqString(s.to_string())
                } else {
                    BqValue::BqNull
                }
            }
            BqType::INTEGER => {
                if let Content::Value(s) = &cell.v {
                    BqValue::BqInteger(s.parse::<i64>().unwrap_or(0))
                } else {
                    BqValue::BqNull
                }
            }
            BqType::FLOAT => {
                if let Content::Value(s) = &cell.v {
                    BqValue::BqFloat(s.parse().unwrap_or(0.0))
                } else {
                    BqValue::BqNull
                }
            }
            BqType::BOOLEAN => {
                if let Content::Value(s) = &cell.v {
                    BqValue::BqBool(s.parse().unwrap_or(false))
                } else {
                    BqValue::BqNull
                }
            }
            BqType::TIMESTAMP => {
                if let Content::Value(s) = &cell.v {
                    BqValue::BqTimestamp(DateTime::from_utc(
                        NaiveDateTime::from_timestamp(
                            s.parse::<f64>().map(|v| v as i64).unwrap_or(0),
                            0,
                        ),
                        Utc,
                    ))
                } else {
                    BqValue::BqNull
                }
            }
            BqType::RECORD => match schema.mode {
                BqMode::REPEATED => match &cell.v {
                    Content::Repeated(cells) => {
                        let columns = cells
                            .iter()
                            .map(|c| Box::new(BqColumn::new(c, &schema).value))
                            .collect();
                        BqValue::BqRepeated(columns)
                    }
                    Content::Struct(row) => {
                        let columns: Vec<BqColumn> = match &row.f {
                            Some(cs) => cs
                                .iter()
                                .enumerate()
                                .map(|(i, c)| BqColumn::new(c, &schema.fields[i]))
                                .collect(),
                            None => vec![],
                        };
                        BqValue::BqStruct(BqRow::new(columns))
                    }
                    _ => BqValue::BqNull,
                },
                _ => {
                    if let Content::Struct(row) = &cell.v {
                        let columns: Vec<BqColumn> = match &row.f {
                            Some(cs) => cs
                                .iter()
                                .enumerate()
                                .map(|(i, c)| BqColumn::new(c, &schema.fields[i]))
                                .collect(),
                            None => vec![],
                        };
                        BqValue::BqStruct(BqRow::new(columns))
                    } else {
                        BqValue::BqNull
                    }
                }
            },
            BqType::UNKNOWN => BqValue::BqNull,
        };
        BqColumn { name, value }
    }
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub enum BqValue {
    BqString(String),
    BqInteger(i64),
    BqFloat(f64),
    BqBool(bool),
    BqTimestamp(DateTime<Utc>),
    BqStruct(BqRow),
    BqRepeated(Vec<Box<BqValue>>),
    BqNull,
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
            created_at: Default::default(),
            expired_at: Default::default(),
        }
    }
}

impl Bq {
    pub fn new(auth: auth::GcpAuth, project: &str) -> Result<Bq> {
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
        })
    }

    #[async_recursion]
    pub async fn list_dataset(
        &'async_recursion self,
        p: &'async_recursion BqListParam,
    ) -> Result<Vec<BqDataset>> {
        let mut list_api = self.api.datasets().list(&self.project);
        if let Some(max_results) = p._max_results {
            list_api = list_api.max_results(max_results);
        }
        if let Some(token) = &p._page_token {
            list_api = list_api.page_token(&token);
        }
        list_api = list_api.param(
            "fields",
            "datasets/id, datasets/datasetReference, nextPageToken",
        );
        let result = list_api.doit().await?;
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

    pub async fn get_table_schema(
        &self,
        dataset: &DatasetId,
        table: &TableId,
    ) -> Result<Vec<BqTableSchema>> {
        let api = self.api.tables().get(&self.project, &dataset, table);
        let result = api.doit().await?;
        let schemas = if let Some(schema) = result.1.schema {
            self.to_schemas(&schema)
        } else {
            vec![]
        };
        Ok(schemas)
    }

    #[async_recursion]
    pub async fn list_tables(
        &'async_recursion self,
        ds: &'async_recursion BqDataset,
        p: &'async_recursion BqListParam,
    ) -> Result<Vec<BqTable>> {
        let mut list_api = self.api.tables().list(&ds.project, &ds.dataset);
        if let Some(max_results) = p._max_results {
            list_api = list_api.max_results(max_results);
        }
        if let Some(token) = &p._page_token {
            list_api = list_api.page_token(&token);
        }
        list_api = list_api.param("fields",
            "tables/id, tables/tableReference, tables/creationTime, tables/expirationTime, nextPageToken, totalItems");
        let result = list_api.doit().await?;
        //println!("{:?}", result);
        let mut tables: Vec<BqTable> = match result.1.tables {
            Some(ts) => ts
                .par_iter()
                .map(|t| {
                    t.table_reference.as_ref().map(|tr| {
                        let table_id = tr.table_id.as_ref().unwrap_or(&"".to_string()).to_string();
                        BqTable {
                            dataset: ds.clone(),
                            table_id,
                            created_at: None,
                            expired_at: None,
                        }
                    })
                })
                .filter_map(|v| v)
                .collect(),
            None => vec![],
        };
        if let Some(token) = result.1.next_page_token {
            let mut param = p.clone();
            param.page_token(&token);
            let additionals = self.list_tables(ds, &param).await?;
            tables.extend(additionals);
        };

        Ok(tables)
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
        let resp = api.doit().await;
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
            Err(e) => {
                // TODO: error handling
                println!("{}", e);
                Err(anyhow::anyhow!(format!("{}", e)))
            }
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

    #[async_recursion]
    pub async fn query(
        &'async_recursion self,
        p: &'async_recursion BqQueryParam,
    ) -> Result<Vec<BqRow>> {
        let req = p.to_query_request();
        let query_api = self.api.jobs().query(req, &self.project);
        let resp = query_api.doit().await;
        //println!("{:?}", resp);
        match resp {
            Ok(result) => {
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
                Ok(bq_rows)
            }
            Err(e) => {
                // TODO: error handling
                println!("{}", e);
                Err(anyhow::anyhow!(format!("{}", e)))
            }
        }
    }

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
        if let Some(max_results) = p._max_results {
            list_api = list_api.max_results(max_results);
        }
        if let Some(token) = &p._page_token {
            list_api = list_api.page_token(&token);
        }
        let table_result_future = table_info.doit();
        let result_future = list_api.doit();
        let (table_result, result) = tokio::join!(table_result_future, result_future);
        //println!("{:?}", table_result);
        //println!("{:?}", result);
        let bq_rows: Vec<BqRow> = if let (Ok(tres), Ok(res)) = (&table_result, &result) {
            let empty: Vec<TableRow> = vec![];
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
