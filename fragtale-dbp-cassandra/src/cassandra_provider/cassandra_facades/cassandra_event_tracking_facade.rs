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

//! Cassandra implementation of [EventTrackingFacade].

use super::CassandraProviderFacades;
use crate::CassandraProvider;
use crate::cassandra_provider::entity::EventIdByUniqueTimeEntity;
use crate::cassandra_provider::entity::ObjectCountEntity;
use crate::cassandra_provider::entity::UniqueTimeBucketByShelfEntity;
use fragtale_dbp::dbp::facades::EventTrackingFacade;
use fragtale_dbp::mb::ObjectCount;
use fragtale_dbp::mb::ObjectCountType;
use fragtale_dbp::mb::UniqueTime;
use fragtale_dbp::mb::correlation::CorrelationResultListener;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::sync::Semaphore;

/// Cassandra implementation of [EventTrackingFacade].
pub struct CassandraEventTrackingFacade {
    cassandra_provider: Arc<CassandraProvider>,
}

impl CassandraEventTrackingFacade {
    /// Return a new instance.
    pub fn new(cassandra_provider: &Arc<CassandraProvider>) -> Self {
        Self {
            cassandra_provider: Arc::clone(cassandra_provider),
        }
    }

    /// Notify `correlation_hotlist` about new events that matches in the
    /// bucket.
    async fn track_new_events_in_topic_bucket(
        cassandra_provider: &CassandraProvider,
        correlation_hotlist: &Arc<dyn CorrelationResultListener>,
        topic_id: &str,
        bucket: u64,
        unique_time_start: UniqueTime,
        any_change: &AtomicBool,
    ) {
        let mut unique_time_low_exclusive =
            UniqueTime::min_encoded_for_micros(unique_time_start.get_time_micros());
        // While the ts is still within the bucket
        while unique_time_low_exclusive <= UniqueTime::max_encoded_in_bucket(bucket) {
            let max_results = 128;
            // Get next batch of potential events
            let event_id_bute_vec = EventIdByUniqueTimeEntity::select_by_unique_time(
                cassandra_provider,
                topic_id,
                bucket,
                unique_time_low_exclusive,
                max_results,
            )
            .await;
            if log::log_enabled!(log::Level::Trace) {
                log::trace!(
                    "topic_id '{topic_id}' bucket {bucket} with {unique_time_low_exclusive} has {} results",
                    event_id_bute_vec.len()
                );
            }
            // if there are no more results in this bucket
            let event_id_bute_vec_len = event_id_bute_vec.len();
            if event_id_bute_vec_len == 0 {
                if log::log_enabled!(log::Level::Trace) {
                    log::trace!(
                        "topic_id '{topic_id}' bucket {bucket} hos no more results. -> Break!"
                    );
                }
                break;
            }
            for event_id_bute in event_id_bute_vec {
                unique_time_low_exclusive = event_id_bute.get_unique_time().as_encoded();
                if correlation_hotlist
                    .notify_hotlist_entry(topic_id, event_id_bute.get_correlation_token())
                {
                    any_change.store(true, Ordering::Relaxed);
                }
            }
            if event_id_bute_vec_len < max_results {
                if log::log_enabled!(log::Level::Trace) {
                    log::trace!(
                        "topic_id '{topic_id}' bucket {bucket} hos no more results. -> Break!",
                    );
                }
                break;
            }
        }
    }
}

#[async_trait::async_trait]
impl EventTrackingFacade for CassandraEventTrackingFacade {
    async fn object_count_insert(
        &self,
        topic_id: &str,
        object_count_type: &ObjectCountType,
        instance_id: u16,
        value: u64,
    ) {
        let now_micros = fragtale_client::time::get_timestamp_micros();
        ObjectCountEntity::new(object_count_type, now_micros, instance_id, value)
            .insert(&self.cassandra_provider, topic_id)
            .await;
    }

    async fn object_count_by_topic_and_type(
        &self,
        topic_id: &str,
        object_count_type: &ObjectCountType,
    ) -> Vec<ObjectCount> {
        let now_micros = fragtale_client::time::get_timestamp_micros();
        ObjectCountEntity::select_by_topic_id_and_object_type(
            &self.cassandra_provider,
            topic_id,
            now_micros,
            object_count_type,
        )
        .await
        .iter()
        .map(ObjectCount::from)
        .collect::<Vec<_>>()
    }

    async fn track_new_events_in_topic(
        &self,
        topic_id: &str,
        correlation_hotlist: Box<Arc<dyn CorrelationResultListener>>,
        hotlist_duration_micros: u64,
    ) -> bool {
        let any_change = Arc::new(AtomicBool::new(false));
        // Query "events" for corrolation_tokens from last X seconds
        let now_ts_micros = fragtale_client::time::get_timestamp_micros();
        let now_shelf = CassandraProviderFacades::get_shelf_from_timestamp_u16(now_ts_micros);
        let now_bucket = CassandraProviderFacades::get_bucket_from_timestamp_u64(now_ts_micros);
        // Get delivery attempts baseline
        let unique_time_start = UniqueTime::from(UniqueTime::min_encoded_for_micros(
            now_ts_micros - hotlist_duration_micros,
        ));
        // Count ongoing tasks and await results when there are too many to limit this some
        let concurrency_semaphore = Arc::new(Semaphore::new(16));
        // Get attempt baseline shelf and bucket
        let start_shelf = unique_time_start.get_shelf();
        let start_bucket = unique_time_start.get_bucket();
        for shelf in start_shelf..=now_shelf {
            let mut last_bucket = start_bucket - 1;
            let max_results = 16;
            while last_bucket < now_bucket {
                let buckets = UniqueTimeBucketByShelfEntity::select_next_by_shelf_and_bucket(
                    &self.cassandra_provider,
                    topic_id,
                    shelf,
                    last_bucket,
                    max_results,
                )
                .await
                .iter()
                .map(UniqueTimeBucketByShelfEntity::get_bucket)
                .collect::<Vec<_>>();
                let buckets_len = buckets.len();
                if buckets_len == 0 {
                    break;
                }
                last_bucket = *buckets.get(buckets_len - 1).unwrap();
                for bucket in buckets {
                    let any_change = Arc::clone(&any_change);
                    let topic_id = topic_id.to_owned();
                    let cassandra_provider = Arc::clone(&self.cassandra_provider);
                    let correlation_hotlist = Arc::clone(&correlation_hotlist);
                    concurrency_semaphore
                        .acquire()
                        .await
                        .inspect_err(|e| log::debug!("Failed to aquire sempahore: {e}"))
                        .ok();
                    let concurrency_semaphore = Arc::clone(&concurrency_semaphore);
                    tokio::spawn(async move {
                        Self::track_new_events_in_topic_bucket(
                            &cassandra_provider,
                            &correlation_hotlist,
                            &topic_id,
                            bucket,
                            unique_time_start,
                            &any_change,
                        )
                        .await;
                        concurrency_semaphore.add_permits(1);
                    });
                }
                if buckets_len < max_results {
                    break;
                }
            }
        }
        any_change.load(Ordering::Relaxed)
    }
}
