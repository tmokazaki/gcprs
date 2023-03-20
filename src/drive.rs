use super::common::error::BadRequest;
use crate::auth;
use anyhow;
use anyhow::Result;
use async_recursion::async_recursion;
use chrono::{DateTime, Utc};
use drive::{
    api::{File, Scope},
    DriveHub, Error,
};
use google_drive3 as drive;
use hyper;
use hyper::body::HttpBody;
use hyper_rustls;
use mime_guess;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::io::prelude::*;

pub struct Drive {
    api: DriveHub<hyper_rustls::HttpsConnector<hyper::client::connect::HttpConnector>>,
}

pub trait Exportable {
    fn extension(&self) -> &'static str;
    fn valid(&self, origin: &String) -> bool;
    fn mime_type(&self) -> &'static str;
}

pub enum DocumentExportMimeType {
    Word,
    OpenDocument,
    RichText,
    PDF,
    PlainText,
    HTML,
    EPUB,
}

impl Exportable for DocumentExportMimeType {
    fn extension(&self) -> &'static str {
        match self {
            DocumentExportMimeType::Word => "docx",
            DocumentExportMimeType::OpenDocument => "odt",
            DocumentExportMimeType::RichText => "rtf",
            DocumentExportMimeType::PDF => "pdf",
            DocumentExportMimeType::PlainText => "txt",
            DocumentExportMimeType::HTML => "zip",
            DocumentExportMimeType::EPUB => "epub",
        }
    }

    fn valid(&self, origin: &String) -> bool {
        "application/vnd.google-apps.document".eq(origin)
    }

    fn mime_type(&self) -> &'static str {
        match self {
            DocumentExportMimeType::Word => {
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            }
            DocumentExportMimeType::OpenDocument => "application/vnd.oasis.opendocument.text",
            DocumentExportMimeType::RichText => "application/rtf",
            DocumentExportMimeType::PDF => "application/pdf",
            DocumentExportMimeType::PlainText => "text/plain",
            DocumentExportMimeType::HTML => "application/zip",
            DocumentExportMimeType::EPUB => "application/epub+zip",
        }
    }
}

pub enum SheetExportMimeType {
    Excel,
    OpenDocument,
    PDF,
    HTML,
    CSV,
    TSV,
}

impl Exportable for SheetExportMimeType {
    fn extension(&self) -> &'static str {
        match self {
            SheetExportMimeType::Excel => "xlsx",
            SheetExportMimeType::OpenDocument => "ods",
            SheetExportMimeType::PDF => "pdf",
            SheetExportMimeType::HTML => "zip",
            SheetExportMimeType::CSV => "csv",
            SheetExportMimeType::TSV => "tsv",
        }
    }
    fn valid(&self, origin: &String) -> bool {
        "application/vnd.google-apps.spreadsheet".eq(origin)
    }

    fn mime_type(&self) -> &'static str {
        match self {
            SheetExportMimeType::Excel => {
                "	application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            }
            SheetExportMimeType::OpenDocument => "application/x-vnd.oasis.opendocument.spreadsheet",
            SheetExportMimeType::PDF => "application/pdf",
            SheetExportMimeType::HTML => "application/zip",
            SheetExportMimeType::CSV => "text/csv",
            SheetExportMimeType::TSV => "text/tab-separated-values",
        }
    }
}

pub enum PresentationExportMimeType {
    PowerPoint,
    ODP,
    PDF,
    PlainText,
}

impl Exportable for PresentationExportMimeType {
    fn extension(&self) -> &'static str {
        match self {
            PresentationExportMimeType::PowerPoint => "pptx",
            PresentationExportMimeType::ODP => "odp",
            PresentationExportMimeType::PDF => "pdf",
            PresentationExportMimeType::PlainText => "txt",
        }
    }
    fn valid(&self, origin: &String) -> bool {
        "application/vnd.google-apps.presentation".eq(origin)
    }

    fn mime_type(&self) -> &'static str {
        match self {
            PresentationExportMimeType::PowerPoint => {
                "application/vnd.openxmlformats-officedocument.presentationml.presentation"
            }
            PresentationExportMimeType::ODP => "application/vnd.oasis.opendocument.presentation",
            PresentationExportMimeType::PDF => "application/pdf",
            PresentationExportMimeType::PlainText => "text/plain",
        }
    }
}

#[derive(Clone)]
pub struct DriveListParam {
    drive_id: Option<String>,
    next_token: Option<String>,
    query: Option<String>,
}

impl DriveListParam {
    pub fn new() -> Self {
        DriveListParam {
            drive_id: None,
            next_token: None,
            query: None,
        }
    }

    pub fn drive_id(&mut self, drive_id: String) -> &mut Self {
        self.drive_id = Some(drive_id);
        self
    }

    pub fn next_token(&mut self, token: &str) -> &mut Self {
        self.next_token = Some(token.to_string());
        self
    }

    /// Set query parameter
    pub fn query(&mut self, query: &str) -> &mut Self {
        self.query = Some(query.to_string());
        self
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DriveFile {
    /// Drive's file ID
    pub id: Option<String>,

    /// Name
    pub name: String,

    /// Mime Type
    pub mime_type: Option<String>,

    /// Created timestamp
    pub created_at: Option<DateTime<Utc>>,

    /// Updated timestamp
    pub modified_at: Option<DateTime<Utc>>,

    /// Size of the remote file
    pub size: i64,

    /// Folder IDs
    pub parents: Option<Vec<String>>,

    /// Link to open in the browser
    pub web_view_link: Option<String>,
}

impl DriveFile {
    fn from_file(f: &File) -> Self {
        //println!("{:?}", f);
        let id = f.id.to_owned();
        let name = f.name.to_owned().unwrap_or_else(|| String::from(""));
        let created_at = f.created_time;
        let modified_at = f.modified_time;
        let mime_type = f.mime_type.to_owned();
        let size = f.size.unwrap_or_else(|| 0);
        let parents = f.parents.as_ref().map(|v| v.clone());
        let web_view_link = f.web_view_link.to_owned();
        DriveFile {
            id,
            name,
            mime_type,
            created_at,
            modified_at,
            size,
            parents,
            web_view_link,
        }
    }

    fn is_folder(&self) -> bool {
        self.mime_type
            .as_ref()
            .map(|mt| mt.eq("application/vnd.google-apps.folder"))
            .unwrap_or_else(|| false)
    }

    fn to_file(&self) -> File {
        let mut file = File::default();
        file.name = Some(self.name.clone());
        file.mime_type = self.mime_type.to_owned();
        file
    }
}

impl Drive {
    pub fn new(auth: &auth::GcpAuth) -> Self {
        let client = hyper::Client::builder().build(
            hyper_rustls::HttpsConnectorBuilder::new()
                .with_native_roots()
                .https_only()
                .enable_http1()
                .enable_http2()
                .build(),
        );
        let api = DriveHub::new(client, auth.authenticator());
        Drive { api }
    }

    /// Upload a loacal file to Drive.
    ///
    /// # Arguments
    ///
    /// * `name`: upload target file path
    /// * `parents`: if you need to put the file under some folders, parents(folder's drive id) are
    /// necessary
    pub async fn create_file(&self, name: &str, parents: Option<Vec<String>>) -> Result<DriveFile> {
        let path = std::path::Path::new(&name);
        let file_name = path.file_name().unwrap().to_str();
        let mime = mime_guess::from_path(path).first_or_octet_stream();
        let infile = std::fs::File::open(&name)?;

        let mut file = File::default();
        file.name = Some(String::from(file_name.unwrap()));
        file.mime_type = Some(mime.to_string());
        file.parents = parents.to_owned();
        let res = self.api.files().create(file)
            .param(
                "fields",
                "id,name,createdTime,modifiedTime,size,mimeType,fileExtension,driveId,parents,webViewLink")
            .upload_resumable(infile, mime)
            .await;
        let result = match res {
            Ok(result) => result,
            Err(e) => match e {
                Error::BadRequest(badrequest) => {
                    if let Ok(br) = serde_json::from_value::<BadRequest>(badrequest.clone()) {
                        anyhow::bail!(br.request_error())
                    } else {
                        anyhow::bail!(badrequest)
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
                    anyhow::bail!(e)
                }
            },
        };
        //println!("{:?}", result);
        let created = DriveFile::from_file(&(result.1));
        Ok(created)
    }

    /// Update file in Drive.
    ///
    /// # Arguments
    ///
    /// * `f`: target file in Drive. This needs to have the file_id in drive
    /// * `content`: local content of the file to be uploaded.
    pub async fn update_file(&self, mut f: DriveFile, content: &str) -> Result<DriveFile> {
        let path = std::path::Path::new(&content);
        let file_name = path.file_name().unwrap().to_str();
        if let Some(filename) = file_name {
            f.name = String::from(filename);
        }
        let file = f.to_file();
        let mime = mime_guess::from_path(path).first_or_octet_stream();
        let infile = std::fs::File::open(&content)?;

        let update = self.api.files().update(file, f.id.as_ref().unwrap())
            .param(
                "fields",
                "id,name,createdTime,modifiedTime,size,mimeType,fileExtension,driveId,parents,webViewLink")
            .upload_resumable(infile, mime);
        let res = update.await;
        let result = match res {
            Ok(result) => result,
            Err(e) => match e {
                Error::BadRequest(badrequest) => {
                    if let Ok(br) = serde_json::from_value::<BadRequest>(badrequest.clone()) {
                        anyhow::bail!(br.request_error())
                    } else {
                        anyhow::bail!(badrequest)
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
                    anyhow::bail!(e)
                }
            },
        };
        let updated = DriveFile::from_file(&(result.1));
        Ok(updated)
    }

    /// Search file.
    ///
    /// Query drive file examples.
    /// - "name contains \"abc\""
    /// - "mimeType=\"application/vnd.google-apps.folder\""
    /// - "\"root\" in parents"
    ///
    /// # Arguments
    ///
    /// * `p`: request parameter
    #[async_recursion]
    pub async fn list_files(
        &'async_recursion self,
        p: &'async_recursion DriveListParam,
    ) -> Result<Vec<DriveFile>> {
        let mut list = self.api.files().list()
            .corpora("user")
            //.drive_id(&p.drive_id)
            //.include_items_from_all_drives(false)
            //.supports_all_drives(false)
            .param("fields", "nextPageToken, files(id,name,createdTime,modifiedTime,size,mimeType,fileExtension,driveId,parents,webViewLink)");
        if let Some(query) = &p.query {
            list = list.q(&format!("{} and trashed=false", query));
        } else {
            list = list.q("trashed=false");
        }
        if let Some(token) = &p.next_token {
            list = list.page_token(&token);
        }
        let res = list.doit().await;
        //println!("{:?}", res);
        let result = match res {
            Ok(result) => result,
            Err(e) => match e {
                Error::BadRequest(badrequest) => {
                    if let Ok(br) = serde_json::from_value::<BadRequest>(badrequest.clone()) {
                        anyhow::bail!(br.request_error())
                    } else {
                        anyhow::bail!(badrequest)
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
                    anyhow::bail!(e)
                }
            },
        };
        //println!("result: {:?}", result);
        let mut files = match result.1.files {
            Some(files) => files
                .par_iter()
                .map(|file| DriveFile::from_file(file))
                .collect(),
            None => Vec::new(),
        };
        if let Some(token) = result.1.next_page_token {
            let mut param = p.clone();
            param.next_token(&token);
            let additionals = self.list_files(&param).await?;
            files.extend(additionals);
        }

        Ok(files)
    }

    /// Get file metadata from Drive.
    /// metadata:
    /// - id
    /// - name
    /// - createdTime
    /// - modifiedTime
    /// - size
    /// - mimeType
    /// - fileExtension
    /// - driveId
    /// - parents
    /// - webViewLink
    ///
    /// # Arguments
    ///
    /// * `file_id`: target file's drive id
    pub async fn get_file_meta_by_id(&self, file_id: &str) -> Result<DriveFile> {
        let res = self
            .api
            .files()
            .get(file_id)
            .param(
                "fields",
                "id,name,createdTime,modifiedTime,size,mimeType,fileExtension,driveId,parents,webViewLink",
            )
            .add_scope(Scope::Readonly)
            .doit()
            .await?;
        //println!("{:?}", res);
        let file = DriveFile::from_file(&res.1);
        Ok(file)
    }

    /// Get(download) file from Drive. The target file may be downloaded and saved locally.
    ///
    /// # Arguments
    ///
    /// * `file_id`: target file's drive id
    pub async fn get_file_by_id(&self, file_id: &str) -> Result<DriveFile> {
        let file = self.get_file_meta_by_id(file_id).await?;
        self.get_file(file).await
    }

    /// Get(download) file from Drive. The target file may be downloaded and saved locally.
    ///
    /// # Arguments
    ///
    /// * `file`: target file object. Before calling, you need to list and get the file object.
    pub async fn get_file(&self, file: DriveFile) -> Result<DriveFile> {
        anyhow::ensure!(file.id.is_some(), "input file does not have id");

        let res = self
            .api
            .files()
            .get(file.id.as_ref().unwrap())
            .param("alt", "media")
            .add_scope(Scope::Readonly)
            .doit()
            .await?;
        //println!("{:?}", res);
        let mut body = res.0.into_body();
        let mut f = std::fs::File::create(&file.name).unwrap();
        while let Some(d) = body.data().await {
            f.write_all(&d?)?;
        }
        Ok(file)
    }

    /// Export file from Drive. The target file shall be downloaded and saved locally.
    ///
    /// # Arguments
    ///
    /// * `file_id`: target file's drive id
    /// * `mime_type`: export mime type
    pub async fn export_file_by_id(
        &self,
        file_id: &str,
        mime_type: impl Exportable,
    ) -> Result<DriveFile> {
        let file = self.get_file_meta_by_id(file_id).await?;
        self.export_file(file, mime_type).await
    }

    /// Export file from Drive. The target file shall be downloaded and saved locally.
    ///
    /// # Arguments
    ///
    /// * `file`: target file object. Before calling, you need to list and get the file object.
    /// * `mime_type`: export mime type
    pub async fn export_file(
        &self,
        file: DriveFile,
        mime_type: impl Exportable,
    ) -> Result<DriveFile> {
        anyhow::ensure!(
            mime_type.valid(file.mime_type.as_ref().unwrap_or(&String::from(""))),
            format!(
                "{:?} does not support to export {}",
                file.mime_type,
                mime_type.mime_type()
            )
        );

        let res = self
            .api
            .files()
            .export(file.id.as_ref().unwrap(), mime_type.mime_type())
            .param("alt", "media")
            .add_scope(Scope::Readonly)
            .doit()
            .await?;
        //println!("{:?}", res);
        let mut body = res.into_body();
        let mut f =
            std::fs::File::create(&format!("{}.{}", file.name, mime_type.extension())).unwrap();
        while let Some(d) = body.data().await {
            f.write_all(&d?)?;
        }
        Ok(file)
    }
}
