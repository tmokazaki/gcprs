pub mod execution;
pub mod job;
pub mod service;
use crate::auth;

use anyhow;
use anyhow::Result;
use cloud_run::{
    api::{
        GoogleCloudRunV2CloudSqlInstance, GoogleCloudRunV2Container, GoogleCloudRunV2EnvVar,
        GoogleCloudRunV2Execution, GoogleCloudRunV2Job, GoogleCloudRunV2ListExecutionsResponse,
        GoogleCloudRunV2ListJobsResponse, GoogleCloudRunV2ListServicesResponse,
        GoogleCloudRunV2ResourceRequirements, GoogleCloudRunV2RunJobRequest,
        GoogleCloudRunV2Service, GoogleCloudRunV2Volume, GoogleCloudRunV2VolumeMount,
        GoogleLongrunningOperation,
    },
    hyper, CloudRun as GcpCloudRun, Error, Result as GcpResult,
};
use google_run2 as cloud_run;
use http_body_util::combinators::BoxBody;
use hyper::body::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct VolumeMount {
    name: String,
    mount_path: String,
}

impl VolumeMount {
    fn to_volume_mount(&self) -> GoogleCloudRunV2VolumeMount {
        let mut volume_mount = GoogleCloudRunV2VolumeMount::default();
        volume_mount.name = Some(self.name.clone());
        volume_mount.mount_path = Some(self.mount_path.clone());
        volume_mount
    }
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
        container.image = Some(self.image.clone());
        container.args = Some(self.args.clone());
        container.command = Some(self.command.clone());
        if 0 < self.volume_mounts.len() {
            container.volume_mounts = Some(
                self.volume_mounts
                    .iter()
                    .map(|v| v.to_volume_mount())
                    .collect(),
            )
        }
        container.env = Some(
            self.env
                .iter()
                .map(|(k, v)| GoogleCloudRunV2EnvVar {
                    name: Some(k.clone()),
                    value: Some(v.clone()),
                    value_source: None,
                })
                .collect(),
        );
        let mut resources = GoogleCloudRunV2ResourceRequirements::default();
        resources.limits = Some(self.resources.clone());
        resources.startup_cpu_boost = Some(true);
        container.resources = Some(resources);
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

    fn handle_error<T, E>(result: GcpResult<T>, f: &dyn Fn(T) -> Result<E>) -> Result<E> {
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
            Ok(res) => f(res),
        }
    }

    fn response_to_service(
        resp: (
            hyper::Response<BoxBody<Bytes, hyper::Error>>,
            GoogleCloudRunV2Service,
        ),
    ) -> Result<service::Service> {
        service::Service::from_service(&resp.1)
    }

    pub async fn services_get(
        &self,
        service_name: &service::RunServiceName,
    ) -> Result<service::Service> {
        let resp = self
            .api
            .projects()
            .locations_services_get(&service_name.name())
            .doit()
            .await;
        println!("{:?}", resp);
        CloudRun::handle_error(resp, &CloudRun::response_to_service)
    }

    fn response_to_list_services(
        resp: (
            hyper::Response<BoxBody<Bytes, hyper::Error>>,
            GoogleCloudRunV2ListServicesResponse,
        ),
    ) -> Result<Vec<service::Service>> {
        Ok(resp
            .1
            .services
            .as_ref()
            .map(|services| {
                services
                    .iter()
                    .map(|service| service::Service::from_service(service).unwrap())
                    .collect()
            })
            .unwrap_or_else(|| vec![]))
    }

    pub async fn services_list(
        &self,
        service_name: &service::RunServiceName,
    ) -> Result<Vec<service::Service>> {
        let resp = self
            .api
            .projects()
            .locations_services_list(&service_name.parent())
            .doit()
            .await;
        println!("{:?}", resp);
        CloudRun::handle_error(resp, &CloudRun::response_to_list_services)
    }

    fn response_to_job(
        resp: (
            hyper::Response<BoxBody<Bytes, hyper::Error>>,
            GoogleCloudRunV2Job,
        ),
    ) -> Result<job::Job> {
        job::Job::from_job(&resp.1)
    }

    pub async fn jobs_get(&self, job_name: &job::RunJobName) -> Result<job::Job> {
        let resp = self
            .api
            .projects()
            .locations_jobs_get(&job_name.name())
            .doit()
            .await;
        println!("{:?}", resp);
        CloudRun::handle_error(resp, &CloudRun::response_to_job)
    }

    fn response_to_operation(
        resp: (
            hyper::Response<BoxBody<Bytes, hyper::Error>>,
            GoogleLongrunningOperation,
        ),
    ) -> Result<()> {
        println!("{:?}", &resp.1);
        Ok(())
    }

    fn response_operation_to_job(
        resp: (
            hyper::Response<BoxBody<Bytes, hyper::Error>>,
            GoogleLongrunningOperation,
        ),
    ) -> Result<job::Job> {
        let job_json = serde_json::to_string(&resp.1.metadata.unwrap()).unwrap();
        let j: GoogleCloudRunV2Job = serde_json::from_str(&job_json).unwrap();
        job::Job::from_job(&j)
    }

    pub async fn jobs_create(
        &self,
        job_name: &job::RunJobName,
        job: &job::Job,
    ) -> Result<job::Job> {
        let resp = self
            .api
            .projects()
            .locations_jobs_create(job.to_job(), &job_name.parent())
            .job_id(job_name.job_name().unwrap())
            .doit()
            .await;
        // println!("{:?}", resp);
        CloudRun::handle_error(resp, &CloudRun::response_operation_to_job)
    }
    pub async fn jobs_delete(&self, job_name: &job::RunJobName) -> Result<job::Job> {
        let resp = self
            .api
            .projects()
            .locations_jobs_delete(&job_name.name())
            .doit()
            .await;
        // println!("{:?}", resp);
        CloudRun::handle_error(resp, &CloudRun::response_operation_to_job)
    }
    pub async fn jobs_run(&self, job_name: &job::RunJobName) -> Result<job::Job> {
        // TODO: accept override parameters?
        let req = GoogleCloudRunV2RunJobRequest::default();
        let resp = self
            .api
            .projects()
            .locations_jobs_run(req, &job_name.name())
            .doit()
            .await;
        // println!("{:?}", resp);
        CloudRun::handle_error(resp, &CloudRun::response_operation_to_job)
    }

    fn response_to_list_jobs(
        resp: (
            hyper::Response<BoxBody<Bytes, hyper::Error>>,
            GoogleCloudRunV2ListJobsResponse,
        ),
    ) -> Result<Vec<job::Job>> {
        Ok(resp
            .1
            .jobs
            .as_ref()
            .map(|jobs| {
                jobs.iter()
                    .map(|job| job::Job::from_job(job).unwrap())
                    .collect()
            })
            .unwrap_or_else(|| vec![]))
    }

    pub async fn jobs_list(&self, job_name: &job::RunJobName) -> Result<Vec<job::Job>> {
        let resp = self
            .api
            .projects()
            .locations_jobs_list(&job_name.parent())
            .doit()
            .await;
        println!("{:?}", resp);
        CloudRun::handle_error(resp, &CloudRun::response_to_list_jobs)
    }

    fn response_to_execution(
        resp: (
            hyper::Response<BoxBody<Bytes, hyper::Error>>,
            GoogleCloudRunV2Execution,
        ),
    ) -> Result<execution::Execution> {
        execution::Execution::from_execution(&resp.1)
    }
    pub async fn executions_get(
        &self,
        execution_name: &execution::RunExecutionName,
    ) -> Result<execution::Execution> {
        let resp = self
            .api
            .projects()
            .locations_jobs_executions_get(&execution_name.name())
            .doit()
            .await;
        println!("{:?}", resp);
        CloudRun::handle_error(resp, &CloudRun::response_to_execution)
    }
    pub async fn executions_delete(
        &self,
        execution_name: &execution::RunExecutionName,
    ) -> Result<()> {
        let resp = self
            .api
            .projects()
            .locations_jobs_executions_delete(&execution_name.name())
            .doit()
            .await;
        println!("{:?}", resp);
        CloudRun::handle_error(resp, &CloudRun::response_to_operation)
    }

    fn response_to_list_executions(
        resp: (
            hyper::Response<BoxBody<Bytes, hyper::Error>>,
            GoogleCloudRunV2ListExecutionsResponse,
        ),
    ) -> Result<Vec<execution::Execution>> {
        Ok(resp
            .1
            .executions
            .as_ref()
            .map(|executions| {
                executions
                    .iter()
                    .map(|exe| execution::Execution::from_execution(exe).unwrap())
                    .collect()
            })
            .unwrap_or_else(|| vec![]))
    }

    pub async fn executions_list(
        &self,
        execution_name: &execution::RunExecutionName,
    ) -> Result<Vec<execution::Execution>> {
        let resp = self
            .api
            .projects()
            .locations_jobs_executions_list(&execution_name.parent())
            .doit()
            .await;
        println!("{:?}", resp);
        CloudRun::handle_error(resp, &CloudRun::response_to_list_executions)
    }
}
