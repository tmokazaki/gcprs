use chrono::{DateTime, Utc};
use cloud_run::{
    api::{GoogleCloudRunV2Condition, GoogleCloudRunV2Execution},
    Error, Result as GcpResult,
};
use google_run2 as cloud_run;
use regex::Regex;

use anyhow;
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct RunExecutionName {
    project: String,
    location: String,
    job_name: String,
    execution_name: Option<String>,
}

impl RunExecutionName {
    pub fn new(
        project: &str,
        location: &str,
        job_name: &str,
        execution_name: Option<String>,
    ) -> Self {
        RunExecutionName {
            project: project.to_string(),
            location: location.to_string(),
            job_name: job_name.to_string(),
            execution_name,
        }
    }

    pub fn replace_name(&self, execution_name: String) -> Self {
        RunExecutionName {
            project: self.project.clone(),
            location: self.location.clone(),
            job_name: self.job_name.clone(),
            execution_name: Some(execution_name),
        }
    }

    pub fn from_name(name: &str) -> Result<Self> {
        let re: Regex = Regex::new(
            r"projects/(?P<project>.+)/locations/(?P<location>.+)/jobs/(?P<job_name>.+)/executions/(?P<execution_name>.+)",
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
                job_name.as_str().to_string()
            } else {
                String::from("")
            };
            let execution_name = if let Some(execution_name) = caps.name("execution_name") {
                Some(execution_name.as_str().to_string())
            } else {
                None
            };
            Ok(RunExecutionName {
                project,
                location,
                job_name,
                execution_name,
            })
        } else {
            Err(anyhow::anyhow!("format error"))
        }
    }

    pub fn name(&self) -> String {
        format!(
            "projects/{}/locations/{}/jobs/{}/executions/{}",
            self.project,
            self.location,
            self.job_name,
            self.execution_name.as_ref().unwrap()
        )
    }

    pub fn parent(&self) -> String {
        format!(
            "projects/{}/locations/{}/jobs/{}",
            self.project, self.location, self.job_name
        )
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ConditionType {
    Started,
    ContainerReady,
    ResourcesAvailable,
    Completed,
    Retry,
    Undefined,
}

impl ConditionType {
    fn from_type(t: &str) -> Self {
        match t {
            "Started" => ConditionType::Started,
            "ContainerReady" => ConditionType::ContainerReady,
            "ResourcesAvailable" => ConditionType::ResourcesAvailable,
            "Completed" => ConditionType::Completed,
            "Retry" => ConditionType::Retry,
            _ => ConditionType::Undefined,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ConditionState {
    Unspecified,
    Pending,
    Reconciling,
    Failed,
    Succeeded,
}

impl ConditionState {
    fn from_state(t: &str) -> Self {
        match t {
            "STATE_UNSPECIFIED" => ConditionState::Unspecified,
            "CONDITION_PENDING" => ConditionState::Pending,
            "CONDITION_RECONCILING" => ConditionState::Reconciling,
            "CONDITION_FAILED" => ConditionState::Failed,
            "CONDITION_SUCCEEDED" => ConditionState::Succeeded,
            _ => ConditionState::Unspecified,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Condition {
    type_: ConditionType,
    state: ConditionState,
    last_transition_time: Option<DateTime<Utc>>,
}

impl Condition {
    fn from_condition(c: &GoogleCloudRunV2Condition) -> Self {
        let type_ = c
            .type_
            .as_ref()
            .map(|t| ConditionType::from_type(&t))
            .unwrap();
        let state = c
            .state
            .as_ref()
            .map(|s| ConditionState::from_state(&s))
            .unwrap();
        let last_transition_time = c.last_transition_time.clone();
        Condition {
            type_,
            state,
            last_transition_time,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Execution {
    pub name: RunExecutionName,
    generation: i64,
    containers: Vec<super::Container>,
    start_time: Option<DateTime<Utc>>,
    update_time: Option<DateTime<Utc>>,
    completion_time: Option<DateTime<Utc>>,
    timeout: Option<i64>,
    service_account: String,
    conditions: Vec<Condition>,
}

impl Execution {
    pub fn from_execution(exe: &GoogleCloudRunV2Execution) -> Result<Self> {
        if let Some(template) = exe.template.as_ref() {
            let name = exe
                .name
                .as_ref()
                .map(|name| RunExecutionName::from_name(&name).unwrap())
                .unwrap();
            let generation = exe.generation.unwrap();
            let start_time = exe.start_time.clone();
            let update_time = exe.update_time.clone();
            let completion_time = exe.completion_time.clone();
            let containers = if let Some(containers) = template.containers.as_ref() {
                containers
                    .iter()
                    .map(|container| super::Container::from_container(container))
                    .collect()
            } else {
                vec![]
            };
            let timeout = template.timeout.map(|t| t.num_seconds());
            let service_account = template.service_account.as_ref().unwrap().to_string();
            let conditions = if let Some(conditions) = exe.conditions.as_ref() {
                conditions
                    .iter()
                    .map(|c| Condition::from_condition(c))
                    .collect()
            } else {
                vec![]
            };
            Ok(Execution {
                name,
                generation,
                containers,
                start_time,
                update_time,
                completion_time,
                timeout,
                service_account,
                conditions,
            })
        } else {
            Err(anyhow::anyhow!("template does not exist"))
        }
    }
}
