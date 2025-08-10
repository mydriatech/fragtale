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

//! Ephemeral in-memory implementation of [DatabaseProvider].

mod inmem_facades;
mod inmem_topic;

use self::inmem_facades::InMemProviderFacades;
use self::inmem_topic::InMemConsumer;
use self::inmem_topic::InMemTopic;
use crossbeam_skiplist::SkipMap;
use fragtale_dbp::dbp::DatabaseProvider;
use std::sync::Arc;

/// Ephemeral in-memory implementation of [DatabaseProvider].
pub struct InMemoryDatabaseProvider {
    topics: SkipMap<String, InMemTopic>,
    topic_descriptors: SkipMap<String, SkipMap<u64, String>>,
}

impl InMemoryDatabaseProvider {
    /// Return a new instance.
    pub async fn new() -> Arc<Self> {
        if log::log_enabled!(log::Level::Trace) {
            log::trace!("Using in-mem db provider.");
        }
        Arc::new(Self {
            topics: SkipMap::default(),
            topic_descriptors: SkipMap::default(),
        })
    }

    /// Get [DatabaseProvider] instance.
    pub fn as_database_provider(self: &Arc<Self>) -> DatabaseProvider {
        DatabaseProvider::new(Arc::new(InMemProviderFacades::new(self)))
    }

    /// Get consumer for a topic.
    fn consumer_by_id(&self, topic_id: &str, consumer_id: &str) -> Arc<InMemConsumer> {
        Arc::clone(
            self.topics
                .get_or_insert_with(topic_id.to_owned(), InMemTopic::default)
                .value()
                .consumers
                .get_or_insert_with(consumer_id.to_owned(), Arc::default)
                .value(),
        )
    }
}
