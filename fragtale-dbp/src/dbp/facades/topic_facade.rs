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

//! Database facade for operation related to topics and event descriptor.

use crate::mb::MessageBrokerError;

/// Database facade for operation related to topics and event descriptor.
#[async_trait::async_trait]
pub trait TopicFacade: Send + Sync {
    /// Ensure that the topic is setup in the database and perform sanity checks
    /// on name conformance.
    async fn ensure_topic_setup(&self, topic_id: &str) -> Result<(), MessageBrokerError>;

    /// Get all topics (ascending) and an indicator if there might be more
    /// results than what was returned.
    async fn get_topic_ids(&self, from: &Option<String>) -> (Vec<String>, bool);

    /// Return true if the EventDescriptor did not already exist
    async fn event_descriptor_persists(
        &self,
        topic_id: &str,
        version: u64,
        version_min: Option<u64>,
        schema_id: &Option<String>,
        event_descriptor: &str,
    ) -> bool;

    /// Get a list of topic identifiers that have descriptors
    async fn event_descriptors_by_topic_id(
        &self,
        topic_id: &str,
        min_descriptor_version: Option<u64>,
    ) -> Vec<String>;

    /// Setup indexes or similar backend functionality to enable queries of
    /// extracted document values.
    async fn extraction_setup_searchable(
        &self,
        topic_id: &str,
        name_and_type_slice: &[(String, String)],
    );
}
