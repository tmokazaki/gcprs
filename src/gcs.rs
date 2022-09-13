use crate::auth;
use google_storage1 as gcs;
use gcs::Storage;
use hyper;
use hyper_rustls;
use urlencoding;

use chrono::{DateTime, Utc};

use anyhow;
use anyhow::Result;
use async_recursion::async_recursion;
use rayon::prelude::*;

#[derive(Debug, Clone)]
pub struct GcsObject {
    pub bucket: String,
    pub content_type: Option<String>,
    pub name: Option<String>,
    pub size: Option<u64>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
    pub content: Option<String>,
}

impl GcsObject {
    pub fn new(bucket: String, name: String) -> GcsObject {
        GcsObject {
            bucket: bucket,
            content_type: None,
            name: Some(name),
            size: None,
            created_at: None,
            updated_at: None,
            content: None,
        }
    }
}

pub struct Gcs {
    api: Storage,
    bucket: String,
}

#[derive(Clone, Debug)]
pub struct GcsListParam {
    _prefix: Option<String>,
    _max_results: Option<u32>,
    _delimiter: Option<String>,
    _next_token: Option<String>,
}

impl GcsListParam {
    pub fn new() -> Self {
        GcsListParam {
            _prefix: Default::default(),
            _max_results: Default::default(),
            _delimiter: Default::default(),
            _next_token: Default::default(),
        }
    }

    pub fn prefix(&mut self, p: &str) -> &mut Self {
        self._prefix = Some(p.to_string());
        self
    }

    pub fn max_results(&mut self, p: u32) -> &mut Self {
        self._max_results = Some(p);
        self
    }

    pub fn delimiter(&mut self, p: &str) -> &mut Self {
        self._delimiter = Some(p.to_string());
        self
    }

    pub fn next_token(&mut self, p: &str) -> &mut Self {
        self._next_token = Some(p.to_string());
        self
    }
}

impl Gcs {
    pub fn new(auth: auth::GcpAuth, bucket: String) -> Result<Gcs> {
        let client = hyper::Client::builder().build(
            hyper_rustls::HttpsConnectorBuilder::new()
                .with_native_roots()
                .https_only()
                .enable_http1()
                .enable_http2()
                .build(),
        );
        let hub = Storage::new(client, auth.authenticator());
        Ok(Gcs {
            api: hub,
            bucket: bucket,
        })
    }

    #[async_recursion]
    pub async fn list_objects(
        &'async_recursion self,
        p: &'async_recursion GcsListParam,
    ) -> Result<Vec<GcsObject>> {
        let mut gcs = self.api.objects().list(&self.bucket);
        if let Some(mr) = p._max_results {
            gcs = gcs.max_results(mr);
        };
        if let Some(pf) = &p._prefix {
            gcs = gcs.prefix(&pf);
        };
        if let Some(de) = &p._delimiter {
            gcs = gcs.delimiter(&de);
        } else {
            // get necessary parameters only.
            // reference: https://cloud.google.com/storage/docs/json_api/v1/objects
            gcs = gcs.param("fields",
                "items/id,items/bucket,items/name,items/size,items/contentType,items/timeCreated,items/updated,nextPageToken,prefixes");
        };
        if let Some(token) = &p._next_token {
            gcs = gcs.page_token(&token);
        };
        let result = gcs.doit().await?;
        let objects = match &p._delimiter {
            Some(_) => match result.1.prefixes {
                Some(prefixes) => prefixes
                    .par_iter()
                    .map(|item| GcsObject {
                        bucket: self.bucket.to_string(),
                        content_type: None,
                        name: Some(item.clone()),
                        size: None,
                        content: None,
                        created_at: None,
                        updated_at: None,
                    })
                    .collect(),
                None => Vec::new(),
            },
            None => {
                let mut objects = match result.1.items {
                    Some(items) => {
                        items
                            .par_iter()
                            .map(|item| {
                                //Object { acl: None, bucket: Some("blocks-gn-okazaki-optimization-job-store"), cache_control: None, component_count: None, content_disposition: None, content_encoding: None, content_language: None, content_type: Some("application/octet-stream"), crc32c: Some("/6VwpQ=="), custom_time: None, customer_encryption: None, etag: Some("CNj04MXmk/ICEAE="), event_based_hold: None, generation: Some("1627957570845272"), id: Some("blocks-gn-okazaki-optimization-job-store/binpacking/4675ee901e83a39b1aefb8265d5ece9a/request/1627957570845272"), kind: Some("storage#object"), kms_key_name: None, md5_hash: Some("YoXBMt9CkzvaosvA1Ey9HA=="), media_link: Some("https://storage.googleapis.com/download/storage/v1/b/blocks-gn-okazaki-optimization-job-store/o/binpacking%2F4675ee901e83a39b1aefb8265d5ece9a%2Frequest?generation=1627957570845272&alt=media"), metadata: None, metageneration: Some("1"), name: Some("binpacking/4675ee901e83a39b1aefb8265d5ece9a/request"), owner: None, retention_expiration_time: None, self_link: Some("https://www.googleapis.com/storage/v1/b/blocks-gn-okazaki-optimization-job-store/o/binpacking%2F4675ee901e83a39b1aefb8265d5ece9a%2Frequest"), size: Some("2107"), storage_class: Some("STANDARD"), temporary_hold: None, time_created: Some("2021-08-03T02:26:10.866Z"), time_deleted: None, time_storage_class_updated: Some("2021-08-03T02:26:10.866Z"), updated: Some("2021-08-03T02:26:10.866Z") }
                                let content_type =
                                    item.content_type.as_ref().map(|c| c.to_string());
                                let name = item.name.as_ref().map(|n| n.to_string());
                                let size = item
                                    .size
                                    .as_ref()
                                    .map(|s| match s.parse::<u64>() {
                                        Ok(n) => Some(n),
                                        _ => None,
                                    })
                                    .flatten();
                                let created_at: Option<DateTime<Utc>> = item
                                    .time_created
                                    .as_ref()
                                    .map(|t| match t.parse::<DateTime<Utc>>() {
                                        Ok(dt) => Some(dt),
                                        _ => None,
                                    })
                                    .flatten();
                                let updated_at: Option<DateTime<Utc>> = item
                                    .updated
                                    .as_ref()
                                    .map(|t| match t.parse::<DateTime<Utc>>() {
                                        Ok(dt) => Some(dt),
                                        _ => None,
                                    })
                                    .flatten();
                                GcsObject {
                                    bucket: self.bucket.to_string(),
                                    content_type,
                                    name,
                                    size,
                                    content: None,
                                    created_at,
                                    updated_at,
                                }
                            })
                            .collect()
                    }
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
                let bytes = hyper::body::to_bytes(content.0.into_body()).await?;
                object.content = Some(String::from_utf8(bytes.to_vec())?);
                Ok(())
            }
            _ => Err(anyhow::anyhow!("there is no object name")),
        }
    }
}
