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

//! Ephemeral in-memory implementation of [EventFacade].

use crate::InMemoryDatabaseProvider;
use crate::inmemdb_provider::inmem_topic::InMemTopic;
use fragtale_dbp::dbp::facades::EventFacade;
use fragtale_dbp::mb::TopicEvent;
use fragtale_dbp::mb::UniqueTime;
use fragtale_dbp::mb::consumers::EventDeliveryGist;
use std::sync::Arc;

/// Ephemeral in-memory implementation of [EventFacade].
pub struct InMemEventFacade {
    inmem_provider: Arc<InMemoryDatabaseProvider>,
}

impl InMemEventFacade {
    /// Return a new instance.
    pub fn new(inmem_provider: &Arc<InMemoryDatabaseProvider>) -> Self {
        Self {
            inmem_provider: Arc::clone(inmem_provider),
        }
    }
}

#[async_trait::async_trait]
impl EventFacade for InMemEventFacade {
    async fn event_by_id(&self, topic_id: &str, event_id: &str) -> Option<EventDeliveryGist> {
        self.inmem_provider
            .topics
            .get_or_insert_with(topic_id.to_owned(), InMemTopic::default)
            .value()
            .event_by_id_and_unique_time(event_id, None)
            .map(|event| {
                EventDeliveryGist::new(
                    event.unique_time,
                    event.document.to_owned(),
                    event.protection_ref.to_owned(),
                    event.correlation_token.to_owned(),
                )
            })
    }

    async fn event_by_id_and_unique_time(
        &self,
        topic_id: &str,
        event_id: &str,
        unique_time: UniqueTime,
    ) -> Option<EventDeliveryGist> {
        self.inmem_provider
            .topics
            .get_or_insert_with(topic_id.to_owned(), InMemTopic::default)
            .value()
            .event_by_id_and_unique_time(event_id, Some(unique_time))
            .map(|event| {
                EventDeliveryGist::new(
                    event.unique_time,
                    event.document.to_owned(),
                    event.protection_ref.to_owned(),
                    event.correlation_token.to_owned(),
                )
            })
    }

    async fn event_ids_by_index(
        &self,
        topic_id: &str,
        index_column: &str,
        index_key: &str,
    ) -> Vec<String> {
        self.inmem_provider
            .topics
            .get_or_insert_with(topic_id.to_owned(), InMemTopic::default)
            .value()
            .event_ids_by_index(index_column, index_key)
    }

    async fn event_document_by_correlation_token(
        &self,
        topic_id: &str,
        correlation_token: &str,
    ) -> Option<EventDeliveryGist> {
        self.inmem_provider
            .topics
            .get_or_insert_with(topic_id.to_owned(), InMemTopic::default)
            .value()
            .event_unique_time_by_corrolation
            .get(correlation_token)
            .map(|entry| entry.value().to_owned())
            .and_then(|(event_id, unique_time)| {
                self.inmem_provider
                    .topics
                    .get_or_insert_with(topic_id.to_owned(), InMemTopic::default)
                    .value()
                    .event_by_id_and_unique_time(&event_id, Some(unique_time))
                    .map(|event| {
                        EventDeliveryGist::new(
                            event.unique_time,
                            event.document.to_owned(),
                            event.protection_ref.to_owned(),
                            event.correlation_token.to_owned(),
                        )
                    })
            })
    }

    async fn event_persist(&self, topic_id: &str, topic_event: TopicEvent) -> String {
        self.inmem_provider
            .topics
            .get_or_insert_with(topic_id.to_owned(), InMemTopic::default)
            .value()
            .event_persist(topic_event)
    }
}
