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

//! Ephemeral in-memory implementation of [TopicFacade].

use crate::InMemoryDatabaseProvider;
use crossbeam_skiplist::SkipMap;
use fragtale_dbp::dbp::facades::TopicFacade;
use fragtale_dbp::mb::MessageBrokerError;
use std::sync::Arc;

/// Ephemeral in-memory implementation of [TopicFacade].
pub struct InMemTopicFacade {
    inmem_provider: Arc<InMemoryDatabaseProvider>,
}

impl InMemTopicFacade {
    /// Return a new instance.
    pub fn new(inmem_provider: &Arc<InMemoryDatabaseProvider>) -> Self {
        Self {
            inmem_provider: Arc::clone(inmem_provider),
        }
    }
}

#[async_trait::async_trait]
impl TopicFacade for InMemTopicFacade {
    async fn ensure_topic_setup(&self, _topic_id: &str) -> Result<(), MessageBrokerError> {
        // NOOP
        Ok(())
    }

    async fn get_topic_ids(&self, from: &Option<String>) -> (Vec<String>, bool) {
        if from.is_some() {
            log::debug!("Getting batches with 'from' is not implemented from the in-mem provider.");
        }
        let res = self
            .inmem_provider
            .topics
            .iter()
            .map(|entry| entry.key().to_owned())
            .collect();
        (res, false)
    }

    /// Return true if the EventDescriptor did not already exist
    async fn event_descriptor_persists(
        &self,
        topic_id: &str,
        version: u64,
        _version_min: Option<u64>,
        _schema_id: &Option<String>,
        event_descriptor: &str,
    ) -> bool {
        let eds = self
            .inmem_provider
            .topic_descriptors
            .get_or_insert_with(topic_id.to_owned(), SkipMap::default);
        if eds.value().contains_key(&version) {
            false
        } else {
            eds.value().insert(version, event_descriptor.to_owned());
            true
        }
    }

    /// Get a list of topic identifiers that have descriptors
    async fn event_descriptors_by_topic_id(
        &self,
        topic_id: &str,
        min_descriptor_version: Option<u64>,
    ) -> Vec<String> {
        let eds = self
            .inmem_provider
            .topic_descriptors
            .get_or_insert_with(topic_id.to_owned(), SkipMap::default);
        eds.value()
            .iter()
            .filter_map(|entry| {
                if min_descriptor_version.is_none_or(|min| *entry.key() > min) {
                    Some(entry.value().to_owned())
                } else {
                    None
                }
            })
            .collect()
    }

    async fn extraction_setup_searchable(
        &self,
        _topic_id: &str,
        _name_and_type_slice: &[(String, String)],
    ) {
        // In-mem impl sets things up lazily / when used
    }
}
