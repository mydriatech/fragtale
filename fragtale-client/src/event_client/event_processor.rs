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

//! Processor of event documents.

use super::EventSource;

/// Process event documents.
#[async_trait::async_trait]
pub trait EventProcessor: Send + Sync + 'static {
    /// Handle incoming event.
    ///
    /// If a document is returned it will be published to the result topic.
    async fn process_message(
        &self,
        topic_id: String,
        event_document: String,
        event_source: &dyn EventSource,
    ) -> Option<String>;

    /// Invoked when subsciption connections has started.
    fn post_subscribed_hook(&self, topic_id: &str) {
        let _ = topic_id;
    }
}
