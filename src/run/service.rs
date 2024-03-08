use cloud_run::{
    api::{
        GoogleCloudRunV2CloudSqlInstance, GoogleCloudRunV2Container, GoogleCloudRunV2EnvVar,
        GoogleCloudRunV2RevisionScaling, GoogleCloudRunV2RevisionTemplate, GoogleCloudRunV2Service,
        GoogleCloudRunV2Volume, GoogleCloudRunV2VolumeMount,
    },
    Error, Result as GcpResult,
};
use google_run2 as cloud_run;
use regex::Regex;

use anyhow;
use anyhow::Result;
use chrono::Duration;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct RunServiceName {
    project: String,
    location: String,
    service_name: Option<String>,
}

impl RunServiceName {
    pub fn new(project: &str, location: &str, service_name: Option<String>) -> Self {
        RunServiceName {
            project: project.to_string(),
            location: location.to_string(),
            service_name,
        }
    }

    pub fn from_name(name: &str) -> Result<Self> {
        let re: Regex = Regex::new(
            r"projects/(?P<project>.+)/locations/(?P<location>.+)/services/(?P<service_name>.+)",
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
            let service_name = if let Some(service_name) = caps.name("service_name") {
                Some(service_name.as_str().to_string())
            } else {
                None
            };
            Ok(RunServiceName {
                project,
                location,
                service_name,
            })
        } else {
            Err(anyhow::anyhow!("format error"))
        }
    }

    pub fn name(&self) -> String {
        format!(
            "projects/{}/locations/{}/services/{}",
            self.project,
            self.location,
            self.service_name.as_ref().unwrap()
        )
    }

    pub fn parent(&self) -> String {
        format!("projects/{}/locations/{}", self.project, self.location)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Service {
    name: RunServiceName,
    max_instance_count: Option<i32>,
    min_instance_count: Option<i32>,
    timeout: Option<i64>,
    service_account: String,
    containers: Vec<super::Container>,
    volumes: Vec<super::Volume>,
    uri: String,
}

impl Service {
    fn to_service(&self) -> GoogleCloudRunV2Service {
        let mut service = GoogleCloudRunV2Service::default();
        let mut template = GoogleCloudRunV2RevisionTemplate::default();
        let mut scaling = GoogleCloudRunV2RevisionScaling::default();
        scaling.max_instance_count = self.max_instance_count.clone();
        scaling.min_instance_count = self.min_instance_count.clone();
        template.scaling = Some(scaling);
        template.timeout = self.timeout.map(|t| Duration::seconds(t));
        template.service_account = Some(self.service_account.clone());
        service
    }
    pub fn from_service(service: &GoogleCloudRunV2Service) -> Result<Self> {
        if let Some(template) = service.template.as_ref() {
            let name = service
                .name
                .as_ref()
                .map(|name| RunServiceName::from_name(&name).unwrap())
                .unwrap();
            let max_instance_count = template.scaling.as_ref().unwrap().max_instance_count;
            let min_instance_count = template.scaling.as_ref().unwrap().min_instance_count;
            let timeout = template.timeout.map(|t| t.num_seconds());
            let service_account = template.service_account.as_ref().unwrap().to_string();
            let containers = if let Some(containers) = template.containers.as_ref() {
                containers
                    .iter()
                    .map(|container| super::Container::from_container(container))
                    .collect()
            } else {
                vec![]
            };
            let volumes = template
                .volumes
                .as_ref()
                .map(|vec| vec.clone())
                .unwrap_or(vec![])
                .iter()
                .map(|v| super::Volume::from_volume(&v))
                .collect();
            let uri = service.uri.as_ref().unwrap().to_string();

            Ok(Service {
                name,
                max_instance_count,
                min_instance_count,
                timeout,
                service_account,
                containers,
                volumes,
                uri,
            })
        } else {
            Err(anyhow::anyhow!("template does not exist"))
        }
    }
}
