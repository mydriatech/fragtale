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

//! Ephemeral in-memory implementation of [EventTrackingFacade].

use crate::InMemoryDatabaseProvider;
use crate::inmemdb_provider::inmem_topic::InMemTopic;
use fragtale_dbp::dbp::facades::EventTrackingFacade;
use fragtale_dbp::mb::ObjectCount;
use fragtale_dbp::mb::ObjectCountType;
use fragtale_dbp::mb::correlation::CorrelationResultListener;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

/// Ephemeral in-memory implementation of [EventTrackingFacade].
pub struct InMemEventTrackingFacade {
    inmem_provider: Arc<InMemoryDatabaseProvider>,
}

impl InMemEventTrackingFacade {
    /// Return a new instance.
    pub fn new(inmem_provider: &Arc<InMemoryDatabaseProvider>) -> Self {
        Self {
            inmem_provider: Arc::clone(inmem_provider),
        }
    }
}

#[async_trait::async_trait]
impl EventTrackingFacade for InMemEventTrackingFacade {
    /// Persist the local count of the [ObjectCountType]
    async fn object_count_insert(
        &self,
        topic_id: &str,
        object_count_type: &ObjectCountType,
        _instance_id: u16,
        value: u64,
    ) {
        self.inmem_provider
            .topics
            .get_or_insert_with(topic_id.to_owned(), InMemTopic::default)
            .value()
            .object_count
            .get_or_insert_with(object_count_type.name().to_owned(), AtomicU64::default)
            .value()
            .store(value, std::sync::atomic::Ordering::Relaxed);
    }

    /// Get the recent count of the [ObjectCountType]
    async fn object_count_by_topic_and_type(
        &self,
        topic_id: &str,
        object_count_type: &ObjectCountType,
    ) -> Vec<ObjectCount> {
        let count = self
            .inmem_provider
            .topics
            .get_or_insert_with(topic_id.to_owned(), InMemTopic::default)
            .value()
            .object_count
            .get_or_insert_with(object_count_type.name().to_owned(), AtomicU64::default)
            .value()
            .load(std::sync::atomic::Ordering::Relaxed);
        // Single node in mem
        vec![ObjectCount::new(0, i64::try_from(count).unwrap())]
    }

    async fn track_new_events_in_topic(
        &self,
        topic_id: &str,
        correlation_hotlist: Box<Arc<dyn CorrelationResultListener>>,
        hotlist_duration_micros: u64,
    ) -> bool {
        self.inmem_provider
            .topics
            .get_or_insert_with(topic_id.to_owned(), InMemTopic::default)
            .value()
            .track_new_events(
                topic_id,
                correlation_hotlist.as_ref().as_ref(),
                hotlist_duration_micros,
            )
    }
}
