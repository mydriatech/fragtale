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

//! Database facade for operation related to events.

use crate::mb::TopicEvent;
use crate::mb::UniqueTime;
use crate::mb::consumers::EventDeliveryGist;

/// Database facade for operation related to events.
#[async_trait::async_trait]
pub trait EventFacade: Send + Sync {
    /// Get core information of an event by the event identifier.
    ///
    /// If multiple events with the same identifier exists (e.g. due to the
    /// same document being published multiple time), the latest one will be
    /// returned.
    async fn event_by_id(&self, topic_id: &str, event_id: &str) -> Option<EventDeliveryGist>;

    /// Get core information of an event by the event identifier and unique
    /// time.
    async fn event_by_id_and_unique_time(
        &self,
        topic_id: &str,
        event_id: &str,
        unique_time: UniqueTime,
    ) -> Option<EventDeliveryGist>;

    /// Get all event identifiers exactly matching the `ìndex_key` of the
    /// `index_column`.
    ///
    /// Ordered by newest event_id first.
    async fn event_ids_by_index(
        &self,
        topic_id: &str,
        index_column: &str,
        index_key: &str,
    ) -> Vec<String>;

    /// Get event's document by the provided correlation token.
    async fn event_document_by_correlation_token(
        &self,
        topic_id: &str,
        correlation_token: &str,
    ) -> Option<EventDeliveryGist>;

    /// Persist an event.
    async fn event_persist(&self, topic_id: &str, topic_event: TopicEvent) -> String;
}
