use cloud_run::{
    api::{
        GoogleCloudRunV2CloudSqlInstance, GoogleCloudRunV2Container, GoogleCloudRunV2EnvVar,
        GoogleCloudRunV2Job, GoogleCloudRunV2RevisionScaling, GoogleCloudRunV2RevisionTemplate,
        GoogleCloudRunV2Volume, GoogleCloudRunV2VolumeMount,
    },
    Error, Result as GcpResult,
};
use google_run2 as cloud_run;
use regex::Regex;

use anyhow;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct RunJobName {
    project: String,
    location: String,
    job_name: Option<String>,
}

impl RunJobName {
    pub fn new(project: &str, location: &str, job_name: Option<String>) -> Self {
        RunJobName {
            project: project.to_string(),
            location: location.to_string(),
            job_name,
        }
    }

    pub fn from_name(name: &str) -> Result<Self> {
        let re: Regex = Regex::new(
            r"projects/(?P<project>.+)/locations/(?P<location>.+)/jobs/(?P<job_name>.+)",
        )
        .unwrap();
        if let Some(caps) = re.captures(name) {
            let project = if let Some(project) = caps.name("project") {
                project.as_str().to_string()
            } else {
                String::from("")
            };
            let location = if let Some(location) = caps.name("location") {
                location.as_str().to_string()
            } else {
                String::from("")
            };
            let job_name = if let Some(job_name) = caps.name("job_name") {
                Some(job_name.as_str().to_string())
            } else {
                None
            };
            Ok(RunJobName {
                project,
                location,
                job_name,
            })
        } else {
            Err(anyhow::anyhow!("format error"))
        }
    }

    pub fn name(&self) -> String {
        format!(
            "projects/{}/locations/{}/jobs/{}",
            self.project,
            self.location,
            self.job_name.as_ref().unwrap()
        )
    }

    pub fn parent(&self) -> String {
        format!("projects/{}/locations/{}", self.project, self.location)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Job {
    name: RunJobName,
    labels: HashMap<String, String>,
    parallelism: Option<i32>,
    task_count: Option<i32>,
    containers: Vec<super::Container>,
    volumes: Vec<super::Volume>,
    timeout: i64,
    service_account: String,
    max_retries: i32,
}

impl Job {
    pub fn from_job(job: &GoogleCloudRunV2Job) -> Result<Self> {}
}
