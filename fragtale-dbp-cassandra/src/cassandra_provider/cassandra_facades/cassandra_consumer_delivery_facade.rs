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

//! Cassandra implementation of [ConsumerDeliveryFacade].

use super::CassandraProviderFacades;
use crate::CassandraProvider;
use crate::cassandra_provider::entity::ConsumerEntity;
use crate::cassandra_provider::entity::DeliveryIntentEntity;
use crate::cassandra_provider::entity::EventIdByUniqueTimeEntity;
use crate::cassandra_provider::entity::UniqueTimeBucketByShelfEntity;
use fragtale_dbp::dbp::facades::ConsumerDeliveryFacade;
use fragtale_dbp::mb::MessageBrokerError;
use fragtale_dbp::mb::MessageBrokerErrorKind;
use fragtale_dbp::mb::UniqueTime;
use fragtale_dbp::mb::consumers::DeliveryIntentTemplate;
use fragtale_dbp::mb::consumers::DeliveryIntentTemplateInsertable;
use std::collections::HashSet;
use std::sync::Arc;

/// Cassandra implementation of [ConsumerDeliveryFacade].
pub struct CassandraConsumerDeliveryFacade {
    cassandra_provider: Arc<CassandraProvider>,
}

impl CassandraConsumerDeliveryFacade {
    /// Allowed characters for consumer identifiers.
    const ALLOWED_CONSUMER_ID_CHARS: &str = "abcdefghijklmnopqrstuvwxyz0123456789_-:;";

    /// Return a new instance.
    pub fn new(cassandra_provider: &Arc<CassandraProvider>) -> Self {
        Self {
            cassandra_provider: Arc::clone(cassandra_provider),
        }
    }

    /// Assert that `consumer_id` uses allowed characters and is of the right
    /// lenght.
    fn assert_consumer_id_well_formed(consumer_id: &str) -> Result<(), MessageBrokerError> {
        if consumer_id
            .chars()
            .any(|c| !Self::ALLOWED_CONSUMER_ID_CHARS.contains(c))
        {
            Err(
                MessageBrokerErrorKind::MalformedIdentifier.error_with_msg(format!(
                    "Invalid chars in consumer id '{consumer_id}'. Only '{}' are allowed.",
                    Self::ALLOWED_CONSUMER_ID_CHARS
                )),
            )?;
        }
        if consumer_id.is_empty() || consumer_id.len() > 255 {
            Err(
                MessageBrokerErrorKind::MalformedIdentifier.error_with_msg(format!(
                    "Invalid length of consumer id '{consumer_id}'. Must be of length 1-255."
                )),
            )?;
        }
        Ok(())
    }

    /// Insert fresh entires into `consumer_delivery_cache` in the order they
    /// are discovered in the specified "bucket".
    async fn populate_delivery_cache_with_fresh_in_bucket(
        cassandra_provider: &CassandraProvider,
        topic_id: &str,
        consumer_id: &str,
        unique_time_attempted: UniqueTime,
        bucket: u64,
        consumer_delivery_cache: Arc<dyn DeliveryIntentTemplateInsertable>,
        //res: &SkipMap<UniqueTime, DeliveryIntentTemplate>,
    ) -> (u64, bool, UniqueTime, bool) {
        let mut any_new_found = false;
        let mut all_attempted = true;
        let mut last_attempted_ts = unique_time_attempted.as_encoded();
        let mut unique_time_low_exclusive =
            UniqueTime::min_encoded_for_micros(unique_time_attempted.get_time_micros());
        // While the ts is still within the bucket
        while unique_time_low_exclusive <= UniqueTime::max_encoded_in_bucket(bucket) {
            // Get next batch of potential events to deliver
            let max_results = 128;
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
                    "bucket {bucket} with unique_time_low_exclusive {unique_time_low_exclusive} has {} results",
                    event_id_bute_vec.len()
                );
            }
            // if there are no more results in this bucket
            let event_id_bpts_vec_len = event_id_bute_vec.len();
            if event_id_bpts_vec_len == 0 {
                break;
            }
            // Get existing delivery intents in the same range
            let delivery_intent_hs = if let Some(last_event_id_unique_time) = event_id_bute_vec
                .last()
                .map(EventIdByUniqueTimeEntity::get_unique_time)
            {
                DeliveryIntentEntity::select_by_unique_time(
                    cassandra_provider,
                    topic_id,
                    consumer_id,
                    bucket,
                    unique_time_low_exclusive,
                    last_event_id_unique_time.as_encoded(),
                    max_results * 2,
                )
                .await
            } else {
                vec![]
            }
            .iter()
            .filter(|die| !die.get_retracted())
            .map(DeliveryIntentEntity::get_unique_time)
            .collect::<HashSet<_>>();
            for event_id_bute in event_id_bute_vec {
                let event_unique_time = event_id_bute.get_unique_time();
                unique_time_low_exclusive = event_unique_time.as_encoded();
                if delivery_intent_hs.contains(&event_unique_time) {
                    // Don't bother adding this to the queue if there is an intent already
                    // It's better that one thread is doing this than every consumer
                    if all_attempted {
                        last_attempted_ts = event_unique_time.as_encoded();
                        if log::log_enabled!(log::Level::Trace) {
                            log::trace!(
                                "all_attempted {all_attempted}, last_attempted_ts: {last_attempted_ts}"
                            )
                        }
                    }
                    continue;
                }
                all_attempted = false;
                // Since the event's UniqueTime is used as map key, it wont really matter if we add
                // multiple entires with deliveryintents originating from different instances.
                // (there will still only be one entry unless it is pulled quickly)
                consumer_delivery_cache.insert(DeliveryIntentTemplate::new(
                    event_unique_time,
                    event_id_bute.get_event_id().to_owned(),
                    event_id_bute.get_descriptor_version(),
                    None,
                ));
                any_new_found = true;
            }
            if event_id_bpts_vec_len < max_results {
                break;
            }
        }
        (
            bucket,
            all_attempted,
            UniqueTime::from(last_attempted_ts),
            any_new_found,
        )
    }
}

#[async_trait::async_trait]
impl ConsumerDeliveryFacade for CassandraConsumerDeliveryFacade {
    async fn ensure_consumer_setup(
        &self,
        topic_id: &str,
        consumer_id: &str,
        baseline_ts: Option<u64>,
        encoded_descriptor_version: Option<u64>,
    ) -> Result<(), MessageBrokerError> {
        Self::assert_consumer_id_well_formed(consumer_id)?;
        // Does the consumer already exists, but is just not cached in this instance?
        let consumer_entity =
            ConsumerEntity::select_by_consumer_id(&self.cassandra_provider, topic_id, consumer_id)
                .await;
        let now_ts = fragtale_client::time::get_timestamp_micros();
        if let Some(consumer_entity) = consumer_entity {
            // Is an update required?
            if consumer_entity.get_last_update_ts() < now_ts + 10_000_000 {
                // Round of the value to bette reflect actual granularity
                let last_update_ts = now_ts - now_ts % 10_000_000;
                ConsumerEntity::update_last_update_ts(
                    &self.cassandra_provider,
                    topic_id,
                    consumer_id,
                    last_update_ts,
                )
                .await;
            }
            if consumer_entity
                .get_latest_descriptor_version()
                .unwrap_or_default()
                < encoded_descriptor_version.unwrap_or_default()
            {
                ConsumerEntity::update_latest_descriptor_version(
                    &self.cassandra_provider,
                    topic_id,
                    consumer_id,
                    encoded_descriptor_version,
                )
                .await;
            }
        } else {
            // Insert
            ConsumerEntity::new(
                consumer_id.to_owned(),
                now_ts,
                baseline_ts,
                encoded_descriptor_version,
            )
            .insert_if_not_exists(&self.cassandra_provider, topic_id)
            .await;
        }
        Ok(())
    }

    async fn consumer_get_attempted_by_id(
        &self,
        topic_id: &str,
        consumer_id: &str,
    ) -> Option<UniqueTime> {
        ConsumerEntity::select_by_consumer_id(&self.cassandra_provider, topic_id, consumer_id)
            .await
            .as_ref()
            .map(ConsumerEntity::get_unique_time_attempted)
    }

    async fn consumer_get_done_by_id(
        &self,
        topic_id: &str,
        consumer_id: &str,
    ) -> Option<UniqueTime> {
        ConsumerEntity::select_by_consumer_id(&self.cassandra_provider, topic_id, consumer_id)
            .await
            .as_ref()
            .map(ConsumerEntity::get_unique_time_done)
    }

    async fn consumer_set_attempted_by_id(
        &self,
        topic_id: &str,
        consumer_id: &str,
        attempted: UniqueTime,
    ) -> bool {
        ConsumerEntity::update_unique_time_attempted(
            &self.cassandra_provider,
            topic_id,
            consumer_id,
            attempted,
        )
        .await
    }

    async fn consumer_set_done_by_id(
        &self,
        topic_id: &str,
        consumer_id: &str,
        done: UniqueTime,
    ) -> bool {
        ConsumerEntity::update_unique_time_done(
            &self.cassandra_provider,
            topic_id,
            consumer_id,
            done,
        )
        .await
    }

    async fn delivery_intent_mark_done(
        &self,
        topic_id: &str,
        consumer_id: &str,
        unique_time: UniqueTime,
        delivery_instance_id: u16,
    ) {
        DeliveryIntentEntity::update_on_done(
            &self.cassandra_provider,
            topic_id,
            consumer_id,
            unique_time,
            delivery_instance_id,
        )
        .await;
    }

    async fn delivery_intent_insert_done(
        &self,
        topic_id: &str,
        consumer_id: &str,
        event_id: &str,
        event_unique_time: UniqueTime,
        instance_id_local: u16,
        descriptor_version: &Option<u64>,
        intent_ts_micros: u64,
    ) {
        DeliveryIntentEntity::new_delivered(
            consumer_id,
            event_unique_time,
            instance_id_local,
            intent_ts_micros,
            event_id,
            descriptor_version,
        )
        .insert(&self.cassandra_provider, topic_id)
        .await
    }

    async fn delivery_intent_reserve(
        &self,
        topic_id: &str,
        consumer_id: &str,
        event_id: &str,
        event_unique_time: UniqueTime,
        instance_id_local: u16,
        descriptor_version: &Option<u64>,
        intent_ts_micros: u64,
        freshness_duration_micros: u64,
        failed_intent_ts_micros: Option<u64>,
    ) -> bool {
        let mut reserved = false;
        let timeout_ts = intent_ts_micros - freshness_duration_micros;
        // Check if this has already been taken care of by another instance
        let dies = DeliveryIntentEntity::select_by_unique_time_only_vec(
            &self.cassandra_provider,
            topic_id,
            consumer_id,
            event_unique_time,
        )
        .await;
        if dies.iter().any(|die|
            // Another node has taken care of this
            die.get_done() ||
            // Another node is about to take care of this
            (!die.get_retracted() && die.get_intent_ts() > timeout_ts))
        {
            return false;
        };
        if let Some(_failed_intent_ts) = failed_intent_ts_micros {
            // Retry
            for die in dies {
                if die.get_delivering_instance_id() == instance_id_local {
                    // Update intent_ts and withdraw retraction
                    DeliveryIntentEntity::update_retracted_and_intent_ts(
                        &self.cassandra_provider,
                        topic_id,
                        consumer_id,
                        event_unique_time,
                        instance_id_local,
                        true,
                        intent_ts_micros,
                    )
                    .await;
                    break;
                }
            }
        } else {
            // Persist a non-retracted delivery intent
            DeliveryIntentEntity::new(
                consumer_id,
                event_unique_time,
                instance_id_local,
                intent_ts_micros,
                event_id,
                descriptor_version,
            )
            .insert(&self.cassandra_provider, topic_id)
            .await;
        }
        // Get all non-retracted delivery intents for this event
        let mut dies = DeliveryIntentEntity::select_by_unique_time_only_vec(
            &self.cassandra_provider,
            topic_id,
            consumer_id,
            event_unique_time,
        )
        .await
        .into_iter()
        // Ignore non-retracted from timed out delivery intents
        .filter(|die| die.get_intent_ts() > timeout_ts)
        .collect::<Vec<_>>();
        // Assumption: If two entires are wirtten at the same time, both writers will see each others writes.
        // Order by WRITETIME (retracted), intent_ts, instance_id
        dies.sort_unstable_by_key(DeliveryIntentEntity::get_delivering_instance_id);
        dies.sort_by_key(DeliveryIntentEntity::get_intent_ts);
        dies.sort_by_key(DeliveryIntentEntity::get_retracted_write_time);
        let first = dies.first().unwrap();
        // We have a winner
        if first.get_delivering_instance_id() == instance_id_local {
            // This instance should deliver the event!
            reserved = true;
        } else {
            // Retract our intent to deliver
            DeliveryIntentEntity::update_retracted(
                &self.cassandra_provider,
                topic_id,
                consumer_id,
                event_unique_time,
                instance_id_local,
                true,
            )
            .await;
        }
        reserved
    }

    // NOTE: `consumer_delivery_cache` will not be strictly populated in the
    // order events were published.

    async fn populate_delivery_cache_with_fresh(
        &self,
        topic_id: &str,
        consumer_id: &str,
        consumer_delivery_cache: Box<Arc<dyn DeliveryIntentTemplateInsertable>>,
        attempted_low_exclusive: UniqueTime,
    ) -> (u64, bool) {
        let mut any_new_found = false;
        let now_ts_micros = fragtale_client::time::get_timestamp_micros();
        let now_shelf = CassandraProviderFacades::get_shelf_from_timestamp_u16(now_ts_micros);
        let now_bucket = CassandraProviderFacades::get_bucket_from_timestamp_u64(now_ts_micros);
        // Get attempt baseline shelf and bucket
        let attempt_shelf = attempted_low_exclusive.get_shelf();
        let attempt_bucket = attempted_low_exclusive.get_bucket();
        let mut all_attempted = true;
        let mut last_attempted_ts = attempted_low_exclusive.as_encoded();
        if log::log_enabled!(log::Level::Trace) {
            log::trace!("attempt_shelf: {attempt_shelf}, now_shelf: {now_shelf}");
        }
        for shelf in attempt_shelf..=now_shelf {
            let mut last_bucket = attempt_bucket - 1;
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
                let mut tasks = Vec::new();
                for bucket in buckets {
                    let cassandra_provider = Arc::clone(&self.cassandra_provider);
                    let topic_id = topic_id.to_owned();
                    let consumer_id = consumer_id.to_owned();
                    let consumer_delivery_cache = Arc::clone(&consumer_delivery_cache);
                    let task = tokio::spawn(async move {
                        Self::populate_delivery_cache_with_fresh_in_bucket(
                            &cassandra_provider,
                            &topic_id,
                            &consumer_id,
                            attempted_low_exclusive,
                            bucket,
                            consumer_delivery_cache,
                        )
                        .await
                    });
                    tasks.push(task);
                }
                // Await these in the order they were created (bucket order)
                for task in tasks {
                    let (
                        _bucket,
                        all_attempted_in_bucket,
                        last_attempted_ts_res,
                        any_new_found_in_bucket,
                    ) = task.await.unwrap();
                    if all_attempted {
                        last_attempted_ts = last_attempted_ts_res.as_encoded();
                    } else if !all_attempted_in_bucket {
                        all_attempted = false;
                    }
                    any_new_found |= any_new_found_in_bucket;
                }
                if buckets_len < max_results {
                    break;
                }
            }
        }
        (last_attempted_ts, any_new_found)
    }

    async fn populate_delivery_cache_with_retries(
        &self,
        topic_id: &str,
        consumer_id: &str,
        consumer_delivery_cache: Box<Arc<dyn DeliveryIntentTemplateInsertable>>,
        done_low_exclusive: UniqueTime,
        freshness_duration_micros: u64,
        clock_skew_tolerance_micros: u64,
    ) -> u64 {
        let mut done_count = 0;
        let mut total_count = 0;
        let timeout_ts = fragtale_client::time::get_timestamp_micros() - freshness_duration_micros;
        let timeout_shelf = CassandraProviderFacades::get_shelf_from_timestamp_u16(timeout_ts);
        // Get attempt baseline shelf and bucket
        let done_shelf = done_low_exclusive.get_shelf();
        let done_bucket = done_low_exclusive.get_bucket();
        let mut all_done = true;
        let mut last_done_ts = done_low_exclusive;
        if log::log_enabled!(log::Level::Trace) {
            log::trace!("Checking shelf {done_shelf}..={timeout_shelf} for redelivery");
        }
        for shelf in done_shelf..=timeout_shelf {
            let mut bucket = Some(done_bucket);
            // Keep going while there are more buckets
            while bucket.is_some() {
                tokio::task::yield_now().await;
                if log::log_enabled!(log::Level::Trace) {
                    log::trace!(
                        "Checking shelf {shelf} bucket {} for redelivery",
                        bucket.unwrap()
                    );
                }
                let mut unique_time_low_exclusive = done_low_exclusive.as_encoded();
                // While the ts is still within the bucket
                while unique_time_low_exclusive
                    <= std::cmp::min(
                        //CassandraClient::get_max_timestamp_in_bucket(bucket.unwrap()),
                        UniqueTime::max_encoded_in_bucket(bucket.unwrap()),
                        UniqueTime::min_encoded_for_micros(timeout_ts),
                    )
                {
                    tokio::task::yield_now().await;
                    let delivery_intent_vec = DeliveryIntentEntity::select_by_unique_time(
                        &self.cassandra_provider,
                        topic_id,
                        consumer_id,
                        bucket.unwrap(),
                        unique_time_low_exclusive,
                        timeout_ts,
                        1000,
                    )
                    .await;
                    if log::log_enabled!(log::Level::Trace) {
                        log::trace!(
                            "shelf {shelf} bucket {} with {unique_time_low_exclusive} has {} results",
                            bucket.unwrap(),
                            delivery_intent_vec.len(),
                        );
                    }
                    // if there are no more results in this bucket
                    if delivery_intent_vec.is_empty() {
                        if all_done {
                            last_done_ts = UniqueTime::from(UniqueTime::max_encoded_in_bucket(
                                bucket.unwrap(),
                            ));
                        }
                        break;
                    }
                    total_count += delivery_intent_vec.len();
                    for delivery_intent in delivery_intent_vec {
                        unique_time_low_exclusive = delivery_intent.get_unique_time().as_encoded();
                        // Track if all events are done (or if we have to retry deliveries again later)
                        if delivery_intent.get_done() {
                            if all_done {
                                last_done_ts = delivery_intent.get_unique_time();
                            }
                            done_count += 1;
                            continue;
                        }
                        all_done = false;
                        if delivery_intent.get_intent_ts() >= timeout_ts {
                            done_count += 1;
                            continue;
                        }
                        consumer_delivery_cache.insert(DeliveryIntentTemplate::new(
                            delivery_intent.get_unique_time(),
                            delivery_intent.get_event_id().to_owned(),
                            delivery_intent.get_descriptor_version(),
                            Some(delivery_intent.get_intent_ts()),
                        ));
                        if consumer_delivery_cache.is_full() {
                            if done_count > 0 || total_count > 0 {
                                log::info!("done_count: {done_count}, total_count: {total_count}");
                            }
                            return std::cmp::min(
                                last_done_ts.as_encoded(),
                                UniqueTime::min_encoded_for_micros(
                                    timeout_ts - clock_skew_tolerance_micros,
                                ),
                            );
                        }
                    }
                }
                // Get next bucket in shelf
                bucket = UniqueTimeBucketByShelfEntity::select_next_by_shelf_and_bucket(
                    &self.cassandra_provider,
                    topic_id,
                    shelf,
                    bucket.unwrap(),
                    1,
                )
                .await
                .first()
                .map(UniqueTimeBucketByShelfEntity::get_bucket);
            }
        }
        if done_count > 0 || total_count > 0 {
            log::info!("done_count: {done_count}, total_count: {total_count}");
        }
        std::cmp::min(
            last_done_ts.as_encoded(),
            UniqueTime::min_encoded_for_micros(timeout_ts - clock_skew_tolerance_micros),
        )
    }
}
