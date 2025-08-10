/*
    Copyright 2025 MydriaTech AB

    Licensed under the Apache License 2.0 with Free world makers exception
    1.0.0 (the "License"); you may not use this file except in compliance with
    the License. You should have obtained a copy of the License with the source
    or binary distribution in file named

        LICENSE-Apache-2.0-with-FWM-Exception-1.0.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

//! Client abstraction for only dealing with processing of event documents.

mod event_processor;
mod event_source;
mod web_socket_pool;

pub use self::event_processor::EventProcessor;
pub use self::event_source::EventSource;
pub use self::web_socket_pool::SubscriberCommand;
pub use self::web_socket_pool::SubscriberResponse;
use self::web_socket_pool::WebSocketPool;
use crate::RestApiClient;
use std::sync::Arc;

/// Abstraction for client that is only dealing with event messages.
pub struct EventClient {
    rest_api_client: RestApiClient,
    web_socket_pool_subscribe: Arc<WebSocketPool>,
    web_socket_pool_ack: Arc<WebSocketPool>,
    web_socket_pool_publish: Arc<WebSocketPool>,
    event_processor: Arc<dyn EventProcessor>,
}

#[async_trait::async_trait]
impl EventSource for EventClient {
    async fn event_by_topic_and_event_id(&self, topic_id: &str, event_id: &str) -> Option<String> {
        self.rest_api_client
            .event_by_topic_and_event_id(topic_id, event_id)
            .await
    }

    async fn event_ids_by_indexed_column(
        &self,
        topic_id: &str,
        index_name: &str,
        index_key: &str,
    ) -> Vec<String> {
        self.rest_api_client
            .event_ids_by_topic_and_index(topic_id, index_name, index_key)
            .await
    }
}

impl EventClient {
    /// Package name reported by Cargo at build time.
    const CARGO_PKG_NAME: &str = env!("CARGO_PKG_NAME");
    /// Package version reported by Cargo at build time.
    const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

    /// Interval between ping at the client to keep-alive the connection.
    pub const PING_INTERVAL_MICROS: u64 = WebSocketPool::PING_INTERVAL_MICROS;

    /// Connect a new instance.
    ///
    /// This will spawn off background jobs for consuming events and deliver
    /// them to the provided [EventProcessor] implementation.
    ///
    /// `concurrency` is the number of available cores and at least `1`.
    pub async fn connect(
        event_service_base_url: &str,
        consume_from_topic_id: &str,
        publish_to_topic_id: &str,
        event_processor: Box<Arc<dyn EventProcessor>>,
        concurrency: usize,
    ) -> Arc<Self> {
        let max_pool_size_multiplier = std::cmp::max(1, concurrency);
        let rest_api_client = RestApiClient::new(
            event_service_base_url,
            Self::CARGO_PKG_NAME,
            Self::CARGO_PKG_VERSION,
            max_pool_size_multiplier,
        )
        .await;
        let web_socket_pool_subscribe = WebSocketPool::new(
            &format!("{event_service_base_url}/topics/{consume_from_topic_id}/subscribe"),
            max_pool_size_multiplier * 16,
            1,
        )
        .await;
        let web_socket_pool_ack = WebSocketPool::new(
            &format!("{event_service_base_url}/topics/{consume_from_topic_id}/confirm"),
            max_pool_size_multiplier,
            1,
        )
        .await;
        let web_socket_pool_publish = WebSocketPool::new(
            &format!("{event_service_base_url}/topics/{publish_to_topic_id}/events"),
            max_pool_size_multiplier,
            1,
        )
        .await;
        Arc::new(Self {
            rest_api_client,
            //event_client_config,
            web_socket_pool_subscribe,
            web_socket_pool_ack,
            web_socket_pool_publish,
            event_processor: Arc::clone(&event_processor),
        })
        .init(
            max_pool_size_multiplier * 16 * 4,
            publish_to_topic_id,
            consume_from_topic_id,
        )
        .await
    }

    /// Initialize background tasks.
    async fn init(
        self: Arc<Self>,
        task_count: usize,
        publish_to_topic_id: &str,
        subscribed_topic_id: &str,
    ) -> Arc<Self> {
        self.rest_api_client
            .register_topic(publish_to_topic_id, None)
            .await;
        // Start N concurrent tasks polling for new messages
        for i in 0..task_count {
            let self_clone = Arc::clone(&self);
            let subscribed_topic_id = subscribed_topic_id.to_owned();
            tokio::spawn(async move { self_clone.handle_messages(i, &subscribed_topic_id).await });
        }
        self.event_processor
            .post_subscribed_hook(subscribed_topic_id);
        self
    }

    async fn handle_messages(self: &Arc<Self>, _index: usize, subscribed_topic_id: &str) {
        while let Some(SubscriberResponse::Next {
            encoded_unique_time,
            event_document,
            correlation_token,
            delivery_instance_id,
        }) = self.web_socket_pool_subscribe.next().await
        {
            if log::log_enabled!(log::Level::Trace) {
                log::trace!("event_document: {event_document:?}");
            }
            self.confirm_delivery_ws(encoded_unique_time, delivery_instance_id)
                .await;
            if log::log_enabled!(log::Level::Trace) {
                log::trace!("Confirmed: {event_document}");
            }
            let subscribed_topic_id = subscribed_topic_id.to_owned();
            let event_processor = Arc::clone(&self.event_processor);
            let event_source = Arc::clone(self) as Arc<dyn EventSource>;
            let result_document = tokio::task::spawn(async move {
                event_processor
                    .process_message(
                        subscribed_topic_id,
                        event_document.to_owned(),
                        event_source.as_ref(),
                    )
                    .await
            })
            .await
            .unwrap();
            if let Some(result_document) = result_document {
                if log::log_enabled!(log::Level::Trace) {
                    log::trace!("Sending: {result_document}");
                }
                // Use default priority
                self.publish_document_ws(None, &result_document, Some(correlation_token))
                    .await;
            } else if log::log_enabled!(log::Level::Debug) {
                log::debug!("Failed to process event.");
            }
        }
        if log::log_enabled!(log::Level::Debug) {
            log::debug!("Will not handle additional messages.");
        }
    }

    /// Confirm that the even was recieved.
    async fn confirm_delivery_ws(&self, encoded_unique_time: u64, delivery_instance_id: u16) {
        Arc::clone(&self.web_socket_pool_ack)
            .send(
                &SubscriberCommand::AckDelivery {
                    encoded_unique_time,
                    delivery_instance_id,
                },
                false,
            )
            .await;
    }

    /// Publish the result of the processing.
    ///
    /// The correlation token from the consumed message is transparently
    /// forwarded.
    async fn publish_document_ws(
        &self,
        priority: Option<u8>,
        document: &str,
        correlation_token: Option<String>,
    ) {
        Arc::clone(&self.web_socket_pool_publish)
            .send(
                &SubscriberCommand::Publish {
                    priority,
                    event_document: document.to_owned(),
                    correlation_token,
                    descriptor_version: None,
                },
                false,
            )
            .await;
    }
}
