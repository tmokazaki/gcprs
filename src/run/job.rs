use chrono::Duration;
use chrono::{DateTime, Utc};
use cloud_run::{
    api::{GoogleCloudRunV2ExecutionTemplate, GoogleCloudRunV2Job, GoogleCloudRunV2TaskTemplate},
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

    pub fn job_name(&self) -> Option<&String> {
        self.job_name.as_ref()
    }

    pub fn replace_name(&self, job_name: String) -> Self {
        RunJobName {
            project: self.project.clone(),
            location: self.location.clone(),
            job_name: Some(job_name),
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
    pub name: RunJobName,
    labels: HashMap<String, String>,
    parallelism: Option<i32>,
    task_count: Option<i32>,
    max_retries: i32,
    timeout: Option<i64>,
    service_account: String,
    containers: Vec<super::Container>,
    volumes: Vec<super::Volume>,
    update_time: Option<DateTime<Utc>>,
    create_time: Option<DateTime<Utc>>,
}

impl Job {
    pub fn to_job(&self) -> GoogleCloudRunV2Job {
        let mut job = GoogleCloudRunV2Job::default();
        job.labels = Some(self.labels.clone());
        let mut template = GoogleCloudRunV2ExecutionTemplate::default();
        let mut task_template = GoogleCloudRunV2TaskTemplate::default();
        task_template.service_account = Some(self.service_account.clone());
        task_template.timeout = self.timeout.map(|t| Duration::seconds(t));
        task_template.max_retries = Some(self.max_retries);
        task_template.containers = Some(self.containers.iter().map(|c| c.to_container()).collect());
        task_template.volumes = Some(self.volumes.iter().map(|v| v.to_volume()).collect());
        template.template = Some(task_template);
        template.parallelism = self.parallelism.clone();
        template.task_count = self.task_count.clone();
        job.template = Some(template);
        job
    }
    pub fn from_job(job: &GoogleCloudRunV2Job) -> Result<Self> {
        if let Some(template) = job.template.as_ref() {
            let name = job
                .name
                .as_ref()
                .map(|name| RunJobName::from_name(&name).unwrap())
                .unwrap();
            let labels = job
                .labels
                .as_ref()
                .map(|l| l.clone())
                .unwrap_or_else(|| HashMap::new());
            let parallelism = template.parallelism.clone();
            let task_count = template.task_count.clone();
            let create_time = job.create_time.clone();
            let update_time = job.update_time.clone();
            if let Some(task_template) = template.template.as_ref() {
                let timeout = task_template.timeout.map(|t| t.num_seconds());
                let max_retries = task_template.max_retries.unwrap();
                let service_account = task_template.service_account.as_ref().unwrap().to_string();
                let containers = if let Some(containers) = task_template.containers.as_ref() {
                    containers
                        .iter()
                        .map(|container| super::Container::from_container(container))
                        .collect()
                } else {
                    vec![]
                };
                let volumes = task_template
                    .volumes
                    .as_ref()
                    .map(|vec| vec.clone())
                    .unwrap_or(vec![])
                    .iter()
                    .map(|v| super::Volume::from_volume(&v))
                    .collect();
                Ok(Job {
                    name,
                    labels,
                    parallelism,
                    task_count,
                    max_retries,
                    timeout,
                    service_account,
                    containers,
                    volumes,
                    update_time,
                    create_time,
                })
            } else {
                Err(anyhow::anyhow!("task template does not exist"))
            }
        } else {
            Err(anyhow::anyhow!("template does not exist"))
        }
    }
}
