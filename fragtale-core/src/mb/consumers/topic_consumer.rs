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

//! Track events to deliver to a connected consumer.

mod consumer_delivery_cache;

use self::consumer_delivery_cache::ConsumerDeliveryCache;
use crate::mb::object_count_tracker::ObjectCountTracker;
use fragtale_client::mb::event_descriptor::DescriptorVersion;
use fragtale_dbp::dbp::DatabaseProvider;
use fragtale_dbp::dbp::facades::DatabaseProviderFacades;
use fragtale_dbp::mb::ObjectCountType;
use fragtale_dbp::mb::UniqueTime;
use fragtale_dbp::mb::consumers::DeliveryIntentTemplateInsertable;
use fragtale_dbp::mb::consumers::EventDeliveryGist;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::time::Duration;
use tokio::time::sleep;

/// Track and cache new events to enable speedy event delivery when the consumer
/// requests it.
pub struct TopicConsumer {
    topic_id: String,
    consumer_id: String,
    instance_id: u16,
    dbp: Arc<DatabaseProvider>,
    object_count_tracker: Arc<ObjectCountTracker>,
    consumer_delivery_cache: Arc<ConsumerDeliveryCache>,
    last_reservation_attempt_micros: AtomicU64,
    maintain_fresh_has_run: AtomicBool,
    maintain_other_has_run: AtomicBool,
}
impl TopicConsumer {
    /// Return a new instance.
    pub fn new(
        dbp: &Arc<DatabaseProvider>,
        object_count_tracker: &Arc<ObjectCountTracker>,
        topic_id: &str,
        consumer_id: &str,
        instance_id: u16,
    ) -> Arc<Self> {
        Arc::new(Self {
            topic_id: topic_id.to_owned(),
            consumer_id: consumer_id.to_owned(),
            instance_id,
            dbp: Arc::clone(dbp),
            object_count_tracker: Arc::clone(object_count_tracker),
            consumer_delivery_cache: ConsumerDeliveryCache::new(),
            last_reservation_attempt_micros: AtomicU64::new(0),
            maintain_fresh_has_run: AtomicBool::new(false),
            maintain_other_has_run: AtomicBool::new(false),
        })
        .init()
    }

    /// Initialize
    fn init(self: Arc<Self>) -> Arc<Self> {
        let self_clone = Arc::clone(&self);
        tokio::spawn(async move { self_clone.maintain_delivery_cache_with_fresh().await });
        let self_clone = Arc::clone(&self);
        tokio::spawn(async move { self_clone.maintain_delivery_cache_other().await });
        self
    }

    /// The duration of which an element is considered "fresh".
    ///
    /// Fresh events will be polled for often, while the older ones will be
    /// delivered without stress.
    ///
    /// If it takes longer to retrieve new events from the database than this
    /// duration, some newly publihsed events will be handled as "old".
    const FRESHNESS_DURATION_MICROS: u64 = 1_500_000 * 2;
    const CLOCK_SKEW_TOLERANCE_MICROS: u64 = 100_000;

    /// Reserve a new event to deliver of an acceptable version.
    pub async fn reserve_delivery_intent(
        &self,
        descriptor_version: Option<DescriptorVersion>,
    ) -> Option<EventDeliveryGist> {
        while !self.maintain_fresh_has_run.load(Ordering::Relaxed)
            || !self.maintain_other_has_run.load(Ordering::Relaxed)
        {
            // Sleep until this happens for the first time
            sleep(Duration::from_millis(128)).await;
            log::debug!("Waiting for fresh and old cache population to run.");
        }
        self.last_reservation_attempt_micros.store(
            fragtale_client::time::get_timestamp_micros(),
            Ordering::Relaxed,
        );
        // Pull oldest entry from delivery cache until we are able to reserve a DeliveryIntent
        while let Some(dit) = self
            .consumer_delivery_cache
            .get_next_delivery_intent_template()
        {
            if log::log_enabled!(log::Level::Trace) {
                log::trace!("Pulled item from consumer_delivery_cache!");
            }
            // Check if this event is of an acceptable version to the consumer
            if let Some(descriptor_version) = &descriptor_version
                && let Some(event_descriptor_semver) = dit.get_descriptor_version()
                && *event_descriptor_semver > descriptor_version.as_encoded()
            {
                // Find another event with a compatible version
                continue;
            }
            let intent_ts = fragtale_client::time::get_timestamp_micros();
            if log::log_enabled!(log::Level::Trace) {
                let duration_since_publishing = intent_ts - dit.get_unique_time().get_time_micros();
                if duration_since_publishing > 2_000_000 {
                    log::trace!(
                        "Event spent {duration_since_publishing} micros at rest before being considered."
                    );
                }
            }
            let reserved = self
                .dbp
                .consumer_delivery_facade()
                .delivery_intent_reserve(
                    &self.topic_id,
                    &self.consumer_id,
                    dit.get_event_id(),
                    dit.get_unique_time(),
                    self.instance_id,
                    dit.get_descriptor_version(),
                    intent_ts,
                    Self::FRESHNESS_DURATION_MICROS,
                    *dit.get_failed_intent_ts(),
                )
                .await;
            if reserved {
                self.object_count_tracker.inc(
                    &self.topic_id.to_owned(),
                    &ObjectCountType::ReservedDeliveryIntents,
                );
                return self
                    .dbp
                    .event_facade()
                    .event_by_id_and_unique_time(
                        &self.topic_id,
                        dit.get_event_id(),
                        dit.get_unique_time(),
                    )
                    .await;
            } else if log::log_enabled!(log::Level::Trace) {
                log::trace!(
                    "Failed to reserve DeliveryIntent for '{}' on '{}'.",
                    self.consumer_id,
                    self.topic_id,
                );
            }
        }
        None
    }

    /// Populate delivery cache with information about newly arrived events.
    ///
    /// This ensures that the delivery cache for the consumer has sufficient
    /// entries to pull from when delivery is possible/requested.
    async fn maintain_delivery_cache_with_fresh(&self) {
        // Load enough "next" events to keep a descent queue to pull from
        loop {
            // Refresh ConsumerEntity info
            if let Some(unique_time_attempted) = self
                .dbp
                .consumer_delivery_facade()
                .consumer_get_attempted_by_id(&self.topic_id, &self.consumer_id)
                .await
            {
                let now = fragtale_client::time::get_timestamp_micros();
                // Priority 1: Get fresh events delivered
                let cdc_clone = Arc::clone(&self.consumer_delivery_cache);
                let diti: Box<Arc<dyn DeliveryIntentTemplateInsertable>> = Box::new(cdc_clone);
                let (last_attempted_ts, any_new_found) = self
                    .dbp
                    .consumer_delivery_facade()
                    .populate_delivery_cache_with_fresh(
                        &self.topic_id,
                        &self.consumer_id,
                        diti,
                        unique_time_attempted,
                    )
                    .await;
                let last_attempted_ts =
                    std::cmp::min(
                        last_attempted_ts,
                        UniqueTime::min_encoded_for_micros(now - Self::FRESHNESS_DURATION_MICROS),
                    ) - UniqueTime::min_encoded_for_micros(Self::CLOCK_SKEW_TOLERANCE_MICROS);
                // Update ConsumerEntity info if we have newer done
                if last_attempted_ts > unique_time_attempted.as_encoded() {
                    let applied = self
                        .dbp
                        .consumer_delivery_facade()
                        .consumer_set_attempted_by_id(
                            &self.topic_id,
                            &self.consumer_id,
                            UniqueTime::from(last_attempted_ts),
                        )
                        .await;
                    if applied && log::log_enabled!(log::Level::Trace) {
                        log::trace!("Updated done baseline!");
                    }
                }
                self.maintain_fresh_has_run.store(true, Ordering::Relaxed);
                if log::log_enabled!(log::Level::Trace) {
                    log::trace!(
                        "After getting fresh, the cache now has {} items.",
                        self.consumer_delivery_cache.len()
                    );
                }
                let duration = fragtale_client::time::get_timestamp_micros() - now;
                if duration > Self::FRESHNESS_DURATION_MICROS {
                    log::warn!(
                        "Getting fresh events took longer ({duration} micros) than the max fresh duration ({} micros). Some events will be handled as old directly after publishing.",
                        Self::FRESHNESS_DURATION_MICROS
                    );
                }
                let last_reservation_attempt_micros = self
                    .last_reservation_attempt_micros
                    .load(std::sync::atomic::Ordering::Relaxed);
                if last_reservation_attempt_micros < now - Self::FRESHNESS_DURATION_MICROS {
                    if log::log_enabled!(log::Level::Debug) && last_reservation_attempt_micros > 0 {
                        log::debug!(
                            "Consumer '{}' has not been polling topic '{}' for some time now..",
                            self.consumer_id,
                            self.topic_id
                        );
                    }
                    // No client has been polling this for some time now..
                    while self
                        .last_reservation_attempt_micros
                        .load(std::sync::atomic::Ordering::Relaxed)
                        == last_reservation_attempt_micros
                    {
                        // Sleep until this happens
                        sleep(Duration::from_millis(128)).await
                    }
                } else if !any_new_found {
                    if log::log_enabled!(log::Level::Trace) {
                        log::trace!(
                            "Consumer '{}' wants some, but we didn't find anything new in the latest check on topic '{}'..",
                            self.consumer_id,
                            self.topic_id
                        );
                    }
                    // Clients wants some, but we didn't find anything new in the latest check
                    self.object_count_tracker
                        .await_change(&self.topic_id, &ObjectCountType::Events, 10_000_000)
                        .await;
                    //sleep(Duration::from_millis(32)).await;
                } else {
                    // Hot topic!
                    tokio::task::yield_now().await;
                }
            } else {
                log::info!("Consumer {} has disappeared.", self.consumer_id);
                sleep(Duration::from_millis(5000)).await;
            }
        }
    }

    /// Populate delivery cache with information about events where delivery has
    /// failed or was inconclusive.
    ///
    /// This ensures that the delivery cache for the consumer has sufficient
    /// entries to pull from when delivery is possible/requested.
    async fn maintain_delivery_cache_other(self: &Arc<Self>) {
        // Load enough "next" events to keep a descent queue to pull from
        let mut glitch_count = 0;
        let mut counter = 0u64;
        loop {
            let now = fragtale_client::time::get_timestamp_micros();
            // Refresh ConsumerEntity info
            if let Some(unique_time_done) = self
                .dbp
                .consumer_delivery_facade()
                .consumer_get_done_by_id(&self.topic_id, &self.consumer_id)
                .await
            {
                // Priority 2: Retry failed deliveries from time to time
                let start_ts = now;
                let cdc_clone = Arc::clone(&self.consumer_delivery_cache);
                let diti: Box<Arc<dyn DeliveryIntentTemplateInsertable>> = Box::new(cdc_clone);
                let last_done_ts = self
                    .dbp
                    .consumer_delivery_facade()
                    .populate_delivery_cache_with_retries(
                        &self.topic_id,
                        &self.consumer_id,
                        diti,
                        unique_time_done,
                        Self::FRESHNESS_DURATION_MICROS,
                        Self::CLOCK_SKEW_TOLERANCE_MICROS,
                    )
                    .await
                    - UniqueTime::min_encoded_for_micros(Self::CLOCK_SKEW_TOLERANCE_MICROS);
                // Update ConsumerEntity info if we have newer done
                if last_done_ts > unique_time_done.as_encoded() {
                    let applied = self
                        .dbp
                        .consumer_delivery_facade()
                        .consumer_set_done_by_id(
                            &self.topic_id,
                            &self.consumer_id,
                            UniqueTime::from(last_done_ts),
                        )
                        .await;
                    if applied && log::log_enabled!(log::Level::Trace) {
                        log::trace!(
                            "'{}' has processed all events up to {last_done_ts} epoch microseconds.",
                            self.consumer_id
                        );
                    }
                }
                self.maintain_other_has_run.store(true, Ordering::Relaxed);
                if log::log_enabled!(log::Level::Trace) {
                    log::trace!(
                        "After getting others, the cache now has {} items.",
                        self.consumer_delivery_cache.len()
                    );
                }
                if log::log_enabled!(log::Level::Debug) {
                    let duration = fragtale_client::time::get_timestamp_micros() - start_ts;
                    if duration > 1_000_000 {
                        log::debug!("Getting failed deliveries took {duration} micros.");
                    }
                }
                // Step through and update baseline from time to time even when the system is mostly idle
                // (since entires might expire this is pretty far from bullet proof, but gets the job done)
                for i in 0..48 {
                    let reserved_before = self
                        .object_count_tracker
                        .get_total_object_count(
                            &self.topic_id,
                            &ObjectCountType::ReservedDeliveryIntents,
                        )
                        .await;
                    //sleep(Duration::from_micros(Self::FRESHNESS_DURATION_MICROS)).await;
                    sleep(Duration::from_micros(Self::FRESHNESS_DURATION_MICROS)).await;
                    let done_after = self
                        .object_count_tracker
                        .get_total_object_count(
                            &self.topic_id,
                            &ObjectCountType::DoneDeliveryIntents,
                        )
                        .await;
                    // If not all events have been processed properly, go investigate..
                    if reserved_before > done_after + glitch_count {
                        glitch_count = reserved_before - done_after;
                        if log::log_enabled!(log::Level::Debug) {
                            log::debug!(
                                "It seems like not all fresh events were processed. reserved_before: {reserved_before}, done_after: {done_after}, glitch_count: {glitch_count}"
                            );
                        }
                        break;
                    }
                    if i == 47 && reserved_before < done_after + glitch_count {
                        // Reset if things have sorted itself out (e.g. ttl kill of bad counts)
                        glitch_count = 0;
                    }
                }
            } else {
                log::warn!("Consumer {} has disappeared.", self.consumer_id);
                sleep(Duration::from_millis(5000)).await;
            }
            if log::log_enabled!(log::Level::Trace) && counter % 32 == 0 {
                log::trace!(
                    "consumer_delivery_cache.len: {} (recent: {})",
                    self.consumer_delivery_cache.len(),
                    self.consumer_delivery_cache.len_recent(),
                );
            }
            counter += 1;
        }
    }
}
