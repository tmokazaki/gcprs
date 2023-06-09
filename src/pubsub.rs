use crate::auth;
use google_pubsub1 as pubsub;
use pubsub::{
    api::{AcknowledgeRequest, PublishRequest, PubsubMessage, PullRequest},
    Error, Pubsub, Result as GcpResult,
};

use anyhow;
use anyhow::Result;
use async_recursion::async_recursion;
use std::thread;
use std::time::Duration;

pub struct PubSub {
    api: Pubsub<auth::HttpsConnector>,
}

#[derive(Clone, Debug)]
pub struct PublishParam {
    project: String,
    topic: String,
}

impl PublishParam {
    pub fn new(project: &str, topic: &str) -> Self {
        PublishParam {
            project: project.to_string(),
            topic: topic.to_string(),
        }
    }

    fn topic_name(&self) -> String {
        format!("projects/{}/topics/{}", self.project, self.topic)
    }
}

pub struct SubscriptionParam {
    project: String,
    subscription: String,
    max_messages: i32,
}

impl SubscriptionParam {
    pub fn new(project: &str, subscription: &str) -> Self {
        SubscriptionParam {
            project: project.to_string(),
            subscription: subscription.to_string(),
            max_messages: 1,
        }
    }

    pub fn max_messages(&mut self, v: i32) -> &mut Self {
        self.max_messages = v;
        self
    }

    fn subscription_name(&self) -> String {
        format!(
            "projects/{}/subscriptions/{}",
            self.project, self.subscription
        )
    }
}

impl PubSub {
    pub fn new(auth: &auth::GcpAuth) -> Result<PubSub> {
        let client = auth::new_client();
        let hub = Pubsub::new(client, auth.authenticator());
        Ok(PubSub { api: hub })
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

    #[async_recursion]
    async fn call_publish(
        &self,
        req: PublishRequest,
        topic: &str,
        retry_count: u64,
    ) -> Result<Vec<String>> {
        let res = self
            .api
            .projects()
            .topics_publish(req.clone(), topic)
            .doit()
            .await;
        println!("{:?}", res);
        match res {
            Err(e) => match e {
                Error::BadRequest(_) => {
                    if 5 < retry_count {
                        eprintln!("{}", e);
                        Err(anyhow::anyhow!("{}", e))
                    } else {
                        let interval = 100 * retry_count.pow(2);
                        // eprintln!("{}, {}", e, interval);
                        thread::sleep(Duration::from_millis(interval));
                        self.call_publish(req, topic, retry_count + 1).await
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
                    Err(anyhow::anyhow!("{}", e))
                }
            },
            Ok(resp) => Ok(resp.1.message_ids.unwrap_or_default()),
        }
    }

    /// publish message to topic
    ///
    pub async fn publish(&self, p: &PublishParam, data: Vec<u8>) -> Result<Vec<String>> {
        let mut message = PubsubMessage::default();
        message.data = Some(data);
        let mut req = PublishRequest::default();
        req.messages = Some(vec![message]);
        self.call_publish(req, &p.topic_name(), 0).await
    }

    async fn send_acknowledge(&self, subscription_name: &str, ack_ids: Vec<String>) -> bool {
        let mut req = AcknowledgeRequest::default();
        req.ack_ids = Some(ack_ids);

        let ack_res = self
            .api
            .projects()
            .subscriptions_acknowledge(req, subscription_name)
            .doit()
            .await;
        match ack_res {
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
                    false
                }
            },
            Ok(_) => true,
        }
    }

    /// Pull message from subscription
    ///
    pub async fn pull_subscription<T>(
        &self,
        p: SubscriptionParam,
        message_handler: fn(&Vec<u8>) -> Result<T>,
    ) -> Result<Vec<T>> {
        let mut req = PullRequest::default();
        req.max_messages = Some(p.max_messages);
        let res = self
            .api
            .projects()
            .subscriptions_pull(req, &p.subscription_name())
            .doit()
            .await;
        println!("{:?}", res);
        match res {
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
            Ok(resp) => {
                if let Some(receives) = resp.1.received_messages {
                    let mut handled_results = vec![];
                    for received in receives {
                        let message = received
                            .message
                            .as_ref()
                            .map(|pm| pm.data.as_ref())
                            .flatten();
                        if let Some(data) = message {
                            let handled = message_handler(data);
                            if handled.is_ok() {
                                // send acknowledge
                                if let Some(id) = received.ack_id.as_ref() {
                                    if self
                                        .send_acknowledge(&p.subscription_name(), vec![id.clone()])
                                        .await
                                    {
                                        handled_results.push(handled.unwrap());
                                    } else {
                                        eprintln!(
                                            "handling message failure. message: {:?}",
                                            received
                                        );
                                    }
                                } else {
                                    eprintln!("no ack_id in message. message: {:?}", received);
                                }
                            } else {
                            }
                        } else if let Some(id) = received.ack_id {
                            // message is empty but has ack_id. send acknowledge but ignore the
                            // result
                            self.send_acknowledge(&p.subscription_name(), vec![id])
                                .await;
                        }
                    }
                    Ok(handled_results)
                } else {
                    Ok(vec![])
                }
            }
        }
    }
}
