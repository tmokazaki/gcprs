use crate::auth;
use gcs::{api::Object, Error, Storage};
use google_storage1 as gcs;
use hyper;
use hyper_rustls;
use mime;
use std::fs;
use std::io::Cursor;
use urlencoding;

use chrono::{DateTime, Utc};

use anyhow;
use anyhow::Result;
use async_recursion::async_recursion;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::io::{Read, Seek};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GcsObject {
    /// Bucket name
    pub bucket: String,

    /// Content type
    pub content_type: Option<String>,

    /// Name of the object
    pub name: Option<String>,

    /// Size of object
    pub size: Option<u64>,

    /// Link to the object downloading
    pub self_link: Option<String>,

    /// Created At
    pub created_at: Option<DateTime<Utc>>,

    /// Updated At
    pub updated_at: Option<DateTime<Utc>>,

    /// The content
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

impl GcsObject {
    pub fn new(bucket: String, name: String) -> Self {
        GcsObject {
            bucket,
            content_type: None,
            name: Some(name),
            size: None,
            self_link: None,
            created_at: None,
            updated_at: None,
            content: None,
        }
    }

    /// Get mime object
    ///
    fn get_mime(&self) -> Option<mime::Mime> {
        self.content_type.as_ref().map(|ct| {
            ct.parse::<mime::Mime>()
                .unwrap_or(mime::APPLICATION_OCTET_STREAM)
        })
    }

    pub fn mime(&mut self, mime: String) -> &mut Self {
        match mime.parse::<mime::Mime>() {
            Ok(_) => self.content_type = Some(mime),
            _ => self.content_type = Some(String::from("application/octet_stream")),
        };
        self
    }

    /// Get path of this object
    ///
    pub fn url(&self) -> String {
        format!(
            "gs://{}/{}",
            self.bucket,
            self.name.as_ref().unwrap_or(&"".to_string())
        )
    }

    pub fn from_object(bucket: &String, item: &Object) -> Self {
        let content_type = item.content_type.as_ref().map(|c| c.to_string());
        let self_link = item.self_link.as_ref().map(|c| c.to_string());
        let name = item.name.as_ref().map(|n| n.to_string());
        let size = item.size;
        let created_at: Option<DateTime<Utc>> = item.time_created;
        let updated_at: Option<DateTime<Utc>> = item.updated;
        GcsObject {
            bucket: bucket.to_string(),
            content_type,
            name,
            size,
            self_link,
            content: None,
            created_at,
            updated_at,
        }
    }

    pub fn to_object(&self) -> Object {
        let mut object = Object::default();
        object.name = self.name.as_ref().map(|n| n.to_string());
        object.size = self.size;
        object.content_type = self.content_type.as_ref().map(|c| c.to_string());
        object.self_link = self.self_link.as_ref().map(|l| l.to_string());
        object.time_created = self.created_at;
        object.updated = self.updated_at;
        object
    }
}

pub struct Gcs {
    api: Storage<hyper_rustls::HttpsConnector<hyper::client::connect::HttpConnector>>,
    bucket: String,
}

#[derive(Clone, Debug)]
pub struct GcsInsertParam {}

impl GcsInsertParam {
    pub fn new() -> Self {
        Self {}
    }
}

#[derive(Clone, Debug)]
pub struct GcsListParam {
    prefix: Option<String>,
    max_results: Option<u32>,
    delimiter: Option<String>,
    next_token: Option<String>,
    start_offset: Option<String>,
    end_offset: Option<String>,
}

impl GcsListParam {
    pub fn new() -> Self {
        GcsListParam {
            prefix: Default::default(),
            max_results: Default::default(),
            delimiter: Default::default(),
            next_token: Default::default(),
            start_offset: Default::default(),
            end_offset: Default::default(),
        }
    }

    pub fn prefix(&mut self, p: &str) -> &mut Self {
        // remove only the first slash
        self.prefix = if p.starts_with("/") {
            Some(p[1..].to_string())
        } else {
            Some(p.to_string())
        };
        self
    }

    pub fn max_results(&mut self, p: u32) -> &mut Self {
        self.max_results = Some(p);
        self
    }

    pub fn delimiter(&mut self, p: &str) -> &mut Self {
        self.delimiter = Some(p.to_string());
        self
    }

    pub fn next_token(&mut self, p: &str) -> &mut Self {
        self.next_token = Some(p.to_string());
        self
    }

    pub fn start_offset(&mut self, p: &str) -> &mut Self {
        self.start_offset = Some(p.to_string());
        self
    }

    pub fn end_offset(&mut self, p: &str) -> &mut Self {
        self.end_offset = Some(p.to_string());
        self
    }
}

impl Gcs {
    pub fn new(auth: auth::GcpAuth, bucket: String) -> Gcs {
        let client = hyper::Client::builder().build(
            hyper_rustls::HttpsConnectorBuilder::new()
                .with_native_roots()
                .https_only()
                .enable_http1()
                .enable_http2()
                .build(),
        );
        let api = Storage::new(client, auth.authenticator());
        Gcs { api, bucket }
    }

    #[async_recursion]
    pub async fn list_objects(
        &'async_recursion self,
        p: &'async_recursion GcsListParam,
    ) -> Result<Vec<GcsObject>> {
        let mut gcs = self.api.objects().list(&self.bucket);
        if let Some(mr) = p.max_results {
            gcs = gcs.max_results(mr);
        }
        if let Some(pf) = &p.prefix {
            gcs = gcs.prefix(&pf);
        }
        if let Some(de) = &p.delimiter {
            gcs = gcs.delimiter(&de);
        } else {
            // get necessary parameters only.
            // reference: https://cloud.google.com/storage/docs/json_api/v1/objects
            gcs = gcs.param("fields",
                "items/id,items/bucket,items/name,items/selfLink,items/size,items/contentType,items/timeCreated,items/updated,nextPageToken,prefixes");
        }
        if let Some(token) = &p.next_token {
            gcs = gcs.page_token(&token);
        }
        if let Some(so) = &p.start_offset {
            gcs = gcs.start_offset(&so);
        }
        if let Some(eo) = &p.end_offset {
            gcs = gcs.end_offset(&eo);
        }
        let result = gcs.doit().await?;
        let objects = match &p.delimiter {
            Some(_) => match result.1.prefixes {
                Some(prefixes) => prefixes
                    .par_iter()
                    .map(|item| GcsObject {
                        bucket: self.bucket.to_string(),
                        content_type: None,
                        name: Some(item.clone()),
                        size: None,
                        self_link: None,
                        content: None,
                        created_at: None,
                        updated_at: None,
                    })
                    .collect(),
                None => Vec::new(),
            },
            None => {
                let mut objects = match result.1.items {
                    Some(items) => items
                        .par_iter()
                        .map(|item| GcsObject::from_object(&self.bucket, item))
                        .collect(),
                    None => Vec::new(),
                };
                if let Some(token) = result.1.next_page_token {
                    let mut param = p.clone();
                    param.next_token(&token);
                    let additionals = self.list_objects(&param).await?;
                    objects.extend(additionals);
                };

                objects
            }
        };
        Ok(objects)
    }

    pub async fn get_object_metadata(&self, name: String) -> Result<GcsObject> {
        let content = self
            .api
            .objects()
            .get(&self.bucket, &urlencoding::encode(&name))
            .param("alt", "json")
            .doit()
            .await?;
        Ok(GcsObject::from_object(&self.bucket, &content.1))
    }

    /// Get object and store `GcsObject` instance
    ///
    /// # Arguments
    ///
    /// * `object` - to be stored object
    pub async fn get_object(&self, object: &mut GcsObject) -> Result<()> {
        match &object.name {
            Some(name) => {
                let content = self
                    .api
                    .objects()
                    .get(&self.bucket, &urlencoding::encode(&name))
                    .param("alt", "media")
                    .doit()
                    .await?;
                //println!("{:?}", content);
                let body = content.0.into_body();
                let bytes = hyper::body::to_bytes(body).await?;
                object.content = Some(String::from_utf8(bytes.to_vec())?);
                Ok(())
            }
            _ => Err(anyhow::anyhow!("there is no object name")),
        }
    }

    /// Get object stream. You need to store data by yourself.
    ///
    /// # Arguments
    ///
    /// * `name` - object name(full path)
    pub async fn get_object_stream(&self, name: String) -> Result<hyper::Response<hyper::Body>> {
        let resp = self
            .api
            .objects()
            .get(&self.bucket, &urlencoding::encode(&name))
            .param("alt", "media")
            .doit()
            .await;
        match resp {
            Ok((body, _)) => Ok(body),
            Err(e) => match e {
                Error::BadRequest(_)
                | Error::HttpError(_)
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
        }
    }

    /// Upload File to the bucket
    ///
    /// # Arguments
    ///
    /// * `object` - GcsObject instance. The object name is used to store bucket.
    /// * `file` - Name of the file.
    /// * `p` - Request parameter.
    pub async fn insert_file(
        &self,
        object: &GcsObject,
        file: String,
        p: Option<GcsInsertParam>,
    ) -> Result<GcsObject> {
        self.insert_object(object, fs::File::open(file)?, p).await
    }

    /// Upload String object to the bucket
    ///
    /// # Arguments
    ///
    /// * `object` - GcsObject instance. The object name is used to store bucket.
    /// * `str` - Data.
    /// * `p` - Request parameter.
    pub async fn insert_string(
        &self,
        object: &GcsObject,
        str: String,
        p: Option<GcsInsertParam>,
    ) -> Result<GcsObject> {
        self.insert_object(object, Cursor::new(str), p).await
    }

    /// Upload object stream to Bucket.
    ///
    /// # Arguments
    ///
    /// * `object` - GcsObject instance. The object name is used to store bucket.
    /// * `stream` - Data.
    /// * `p` - Request parameter.
    pub async fn insert_object<T: Seek + Read + Send>(
        &self,
        object: &GcsObject,
        stream: T,
        p: Option<GcsInsertParam>,
    ) -> Result<GcsObject> {
        let req = object.to_object();
        let insert = self.api.objects().insert(req, &self.bucket);
        let mime_type = if let Some(m) = object.get_mime() {
            m
        } else {
            mime::APPLICATION_OCTET_STREAM
        };
        let resp = insert.upload_resumable(stream, mime_type).await;
        match resp {
            Ok(content) => {
                let obj = GcsObject::from_object(&self.bucket, &content.1);
                Ok(obj)
            }
            Err(e) => match e {
                Error::BadRequest(_)
                | Error::HttpError(_)
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
        }
    }
}
