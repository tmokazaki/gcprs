pub mod service;
pub mod job;
use crate::auth;

use anyhow;
use anyhow::Result;
use google_run2 as cloud_run;
use cloud_run::{
    api::{
        GoogleCloudRunV2CloudSqlInstance, GoogleCloudRunV2Container, GoogleCloudRunV2EnvVar,
        GoogleCloudRunV2RevisionScaling,
        GoogleCloudRunV2Volume, GoogleCloudRunV2VolumeMount,
    },
    CloudRun as GcpCloudRun, Error, Result as GcpResult,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct VolumeMount {
    name: String,
    mount_path: String,
}

impl VolumeMount {
    pub fn from_volume_mount(vm: &GoogleCloudRunV2VolumeMount) -> Self {
        let name = vm.name.as_ref().map(|n| n.clone()).unwrap();
        let mount_path = vm.mount_path.as_ref().map(|p| p.clone()).unwrap();
        VolumeMount { name, mount_path }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Volume {
    name: String,
    cloud_sql_instance: Option<String>,
}

impl Volume {
    fn to_volume(&self) -> GoogleCloudRunV2Volume {
        let mut volume = GoogleCloudRunV2Volume::default();
        volume.name = Some(self.name.clone());
        if let Some(instance) = self.cloud_sql_instance.as_ref() {
            volume.cloud_sql_instance = Some(GoogleCloudRunV2CloudSqlInstance {
                instances: Some(vec![instance.clone()]),
            });
        }
        volume
    }
    fn from_volume(volume: &GoogleCloudRunV2Volume) -> Volume {
        let cloud_sql_instance = if let Some(csi) = volume.cloud_sql_instance.as_ref() {
            csi.instances.as_ref().map(|instances| instances[0].clone())
        } else {
            None
        };
        Volume {
            name: volume.name.as_ref().unwrap().clone(),
            cloud_sql_instance,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Container {
    image: String,
    args: Vec<String>,
    command: Vec<String>,
    env: HashMap<String, String>,
    resources: HashMap<String, String>,
    volume_mounts: Vec<VolumeMount>,
}

impl Container {
    fn to_container(&self) -> GoogleCloudRunV2Container {
        let mut container = GoogleCloudRunV2Container::default();
        container
    }
    fn from_container(container: &GoogleCloudRunV2Container) -> Self {
        let image = container.image.as_ref().unwrap().to_string();
        let args = if let Some(args) = container.args.as_ref() {
            args.clone()
        } else {
            vec![]
        };
        let command: Vec<String> = if let Some(command) = container.command.as_ref() {
            command.clone()
        } else {
            vec![]
        };
        let default_str = String::from("");
        let env = if let Some(envs) = container.env.as_ref() {
            let env_map = HashMap::from(
                envs.iter()
                    .map(|env| {
                        (
                            env.name
                                .as_ref()
                                .unwrap_or_else(|| &default_str)
                                .to_string(),
                            env.value
                                .as_ref()
                                .unwrap_or_else(|| &default_str)
                                .to_string(),
                        )
                    })
                    .collect::<HashMap<String, String>>(),
            );
            env_map
        } else {
            HashMap::new()
        };
        let resources = if let Some(resources) = container.resources.as_ref() {
            if let Some(limits) = resources.limits.as_ref() {
                limits.clone()
            } else {
                HashMap::new()
            }
        } else {
            HashMap::new()
        };
        let volume_mounts = if let Some(vms) = container.volume_mounts.as_ref() {
            vms.iter()
                .map(|vm| VolumeMount::from_volume_mount(vm))
                .collect()
        } else {
            vec![]
        };
        Container {
            image,
            args,
            command,
            env,
            resources,
            volume_mounts,
        }
    }
}
pub struct CloudRun {
    api: GcpCloudRun<auth::HttpsConnector>,
}

impl CloudRun {
    pub fn new(auth: &auth::GcpAuth) -> Result<CloudRun> {
        let client = auth::new_client();
        let hub = GcpCloudRun::new(client, auth.authenticator());
        Ok(CloudRun { api: hub })
    }

    fn handle_error<T>(result: GcpResult<T>) -> Result<T> {
        match result {
            Err(e) => match e {
                // The Error enum provides details about what exactly happened.
                // You can also just use its `Debug`, `Display` or `Error` traits
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
                    println!("{}", e);
                    Err(anyhow::anyhow!("{}", e))
                }
            },
            Ok(res) => Ok(res),
        }
    }

    pub async fn services_get(&self, service_name: &service::RunServiceName) -> Result<service::Service> {
        let resp = self
            .api
            .projects()
            .locations_services_get(&service_name.name())
            .doit()
            .await;
        println!("{:?}", resp);
        match resp {
            Err(e) => match e {
                Error::BadRequest(_) => {
                    eprintln!("{}", e);
                    Err(anyhow::anyhow!("{}", e))
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
            Ok(resp) => service::Service::from_service(&resp.1),
        }
    }

    pub async fn services_list(&self, service_name: &service::RunServiceName) -> Result<Vec<service::Service>> {
        let resp = self
            .api
            .projects()
            .locations_services_list(&service_name.parent())
            .doit()
            .await;
        println!("{:?}", resp);
        match resp {
            Err(e) => match e {
                Error::BadRequest(_) => {
                    eprintln!("{}", e);
                    Err(anyhow::anyhow!("{}", e))
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
            Ok(resp) => Ok(resp
                .1
                .services
                .as_ref()
                .map(|services| services.clone())
                .unwrap_or_else(|| vec![])
                .iter()
                .map(|service| service::Service::from_service(&service).unwrap())
                .collect()),
        }
    }
    pub async fn jobs_get(&self, service_name: &job::RunJobName) -> Result<()> {
        let resp = self
            .api
            .projects()
            .locations_jobs_get(&service_name.name())
            .doit()
            .await;
        println!("{:?}", resp);
        match resp {
            Err(e) => match e {
                Error::BadRequest(_) => {
                    eprintln!("{}", e);
                    Err(anyhow::anyhow!("{}", e))
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
            Ok(resp) => Ok(()),
        }
    }

    pub async fn jobs_list(&self, job_name: &job::RunJobName) -> Result<Vec<()>> {
        let resp = self
            .api
            .projects()
            .locations_jobs_list(&job_name.parent())
            .doit()
            .await;
        println!("{:?}", resp);
        match resp {
            Err(e) => match e {
                Error::BadRequest(_) => {
                    eprintln!("{}", e);
                    Err(anyhow::anyhow!("{}", e))
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
            Ok(resp) => Ok(vec![]),
        }
    }
}
