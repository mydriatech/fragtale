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

//! Cassandra implementation of [EventFacade].

use crate::CassandraProvider;
use crate::cassandra_provider::entity::EventEntity;
use crate::cassandra_provider::entity::EventIdByUniqueTimeEntity;
use crate::cassandra_provider::entity::UniqueTimeBucketByShelfEntity;
use crossbeam_skiplist::SkipMap;
use fragtale_dbp::dbp::facades::EventFacade;
use fragtale_dbp::mb::TopicEvent;
use fragtale_dbp::mb::UniqueTime;
use fragtale_dbp::mb::consumers::EventDeliveryGist;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

/// Cassandra implementation of [EventFacade].
pub struct CassandraEventFacade {
    cassandra_provider: Arc<CassandraProvider>,
    per_topic_persisted_bucket: SkipMap<String, AtomicU64>,
}

impl CassandraEventFacade {
    /// Return a new instance.
    pub fn new(cassandra_provider: &Arc<CassandraProvider>) -> Self {
        Self {
            cassandra_provider: Arc::clone(cassandra_provider),
            per_topic_persisted_bucket: SkipMap::default(),
        }
    }
}

#[async_trait::async_trait]
impl EventFacade for CassandraEventFacade {
    async fn event_by_id(&self, topic_id: &str, event_id: &str) -> Option<EventDeliveryGist> {
        EventEntity::select_by_event_id(&self.cassandra_provider, topic_id, event_id, 1)
            .await
            .into_iter()
            .map(EventEntity::into_event_delivery_gist)
            .next()
    }

    async fn event_by_id_and_unique_time(
        &self,
        topic_id: &str,
        event_id: &str,
        unique_time: UniqueTime,
    ) -> Option<EventDeliveryGist> {
        EventEntity::select_by_event_id_and_unique_time(
            &self.cassandra_provider,
            topic_id,
            event_id,
            unique_time,
        )
        .await
        .map(EventEntity::into_event_delivery_gist)
    }

    async fn event_ids_by_index(
        &self,
        topic_id: &str,
        index_column: &str,
        index_key: &str,
    ) -> Vec<String> {
        // Assume 512 bits event_id as hex + u64 -> 128 + 8 bytes = 136 bytes
        // 128MiB would be a hard upper limit ~> 986_895 results.
        // Use something like 524288 (~68 MiB) to stay clear of this limit.
        // This would still be pretty bad index design, but this is not the
        // right place to enfore suhc limit..
        let mut ret = EventEntity::select_ids_and_unique_time_by_index(
            &self.cassandra_provider,
            topic_id,
            index_column,
            index_key,
            524_288,
        )
        .await;
        // Newest event first
        ret.sort_unstable_by_key(|(_event_id, unique_time)| *unique_time);
        ret.reverse();
        ret.into_iter()
            .map(|(event_id, _unique_time)| event_id)
            .collect()
    }

    async fn event_document_by_correlation_token(
        &self,
        topic_id: &str,
        correlation_token: &str,
    ) -> Option<EventDeliveryGist> {
        EventEntity::select_by_correlation_token(
            &self.cassandra_provider,
            topic_id,
            correlation_token,
        )
        .await
        .map(EventEntity::into_event_delivery_gist)
    }

    async fn event_persist(&self, topic_id: &str, topic_event: TopicEvent) -> String {
        EventEntity::from(&topic_event)
            .insert(
                &self.cassandra_provider,
                topic_id,
                topic_event.get_additional_columns().to_owned(),
            )
            .await;
        EventIdByUniqueTimeEntity::from(&topic_event)
            .insert(&self.cassandra_provider, topic_id)
            .await;
        let unique_time = topic_event.get_unique_time();
        // Avoid this query/insert if its already known to be there
        // (This is full of glitches, but lightweight and prevents several db ops.)
        let persisted_bucket_entry = self
            .per_topic_persisted_bucket
            .get_or_insert_with(topic_id.to_owned(), AtomicU64::default);
        let persisted_bucket = persisted_bucket_entry.value();
        let old_value = persisted_bucket.load(Ordering::Relaxed);
        if log::log_enabled!(log::Level::Trace) {
            log::trace!(
                "persisted_bucket - old_value: {}, unique_time.as_bucket: {}",
                old_value,
                unique_time.get_bucket()
            );
        }
        if old_value != unique_time.get_bucket() {
            // Be optimistic
            persisted_bucket.store(unique_time.get_bucket(), Ordering::Relaxed);
            UniqueTimeBucketByShelfEntity::new(unique_time)
                .insert(&self.cassandra_provider, topic_id)
                .await;
            if log::log_enabled!(log::Level::Trace) {
                log::trace!("persisted_bucket.store {}", unique_time.get_bucket());
            }
        }
        topic_event.get_correlation_token().to_owned()
    }
}
