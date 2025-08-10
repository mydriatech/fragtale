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

/// Query interface for the event source.
#[async_trait::async_trait]
pub trait EventSource: Send + Sync {
    /// Get an event document by its identifier.
    async fn event_by_topic_and_event_id(&self, topic_id: &str, event_id: &str) -> Option<String>;

    /// Get all event identifiers where `index_name` exactly has `index_key`
    /// entries.
    ///
    /// Ordered by latest event identifier first.
    async fn event_ids_by_indexed_column(
        &self,
        topic_id: &str,
        index_name: &str,
        index_key: &str,
    ) -> Vec<String>;
}
