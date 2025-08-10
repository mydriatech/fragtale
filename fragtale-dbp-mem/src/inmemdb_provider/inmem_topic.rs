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

//! Ephemeral in-memory representation of a topic.

mod inmem_consumer;
mod inmem_event;

pub use self::inmem_consumer::*;
pub use self::inmem_event::*;
use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::SkipSet;
use fragtale_dbp::mb::ExtractedValue;
use fragtale_dbp::mb::TopicEvent;
use fragtale_dbp::mb::UniqueTime;
use fragtale_dbp::mb::consumers::DeliveryIntentTemplate;
use fragtale_dbp::mb::consumers::DeliveryIntentTemplateInsertable;
use fragtale_dbp::mb::correlation::CorrelationResultListener;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

/// Ephemeral in-memory representation of a topic.
#[derive(Default)]
pub struct InMemTopic {
    pub consumers: SkipMap<String, Arc<InMemConsumer>>,
    pub events: SkipMap<UniqueTime, Arc<InMemEvent>>,
    pub event_unique_time_by_id: SkipMap<String, Arc<SkipSet<UniqueTime>>>,
    pub event_unique_time_by_corrolation: SkipMap<String, (String, UniqueTime)>,
    pub object_count: SkipMap<String, AtomicU64>,
    pub indices: SkipMap<String, SkipMap<String, SkipSet<(String, UniqueTime)>>>,
}

impl InMemTopic {
    /// Retrieve a specific event by event identifier and [UniqueTime].
    pub fn event_by_id_and_unique_time(
        &self,
        event_id: &str,
        unique_time: Option<UniqueTime>,
    ) -> Option<Arc<InMemEvent>> {
        if let Some(unique_time) = unique_time {
            self.events.get(&unique_time)
        } else {
            self.event_unique_time_by_id
                .get(event_id)
                .map(|entry| Arc::clone(entry.value()))
                .and_then(|unique_times| unique_times.back().map(|entry| *entry.value()))
                .and_then(|unique_time| self.events.get(&unique_time))
        }
        .map(|event_entry| Arc::clone(event_entry.value()))
    }

    /// Retrieve event identifiers from an index..
    pub fn event_ids_by_index(&self, index_column: &str, index_key: &str) -> Vec<String> {
        let mut ret = self
            .indices
            .get_or_insert_with(index_column.to_owned(), SkipMap::default)
            .value()
            .get_or_insert_with(index_key.to_owned(), SkipSet::default)
            .value()
            .iter()
            .map(|entry| entry.value().to_owned())
            .collect::<Vec<(String, UniqueTime)>>();
        // Newest event first
        ret.sort_unstable_by_key(|(_event_id, unique_time)| *unique_time);
        ret.reverse();
        ret.into_iter()
            .map(|(event_id, _unique_time)| event_id)
            .collect()
    }

    /// Persist the event.
    pub fn event_persist(&self, topic_event: TopicEvent) -> String {
        self.events.insert(
            topic_event.get_unique_time(),
            Arc::new(InMemEvent {
                event_id: topic_event.get_event_id().to_owned(),
                unique_time: topic_event.get_unique_time(),
                document: topic_event.get_document().to_owned(),
                protection_ref: topic_event.get_protection_ref().to_owned(),
                correlation_token: topic_event.get_correlation_token().to_owned(),
                descriptor_version: topic_event.get_descriptor_version(),
            }),
        );
        Arc::clone(
            self.event_unique_time_by_id
                .get_or_insert_with(topic_event.get_event_id().to_owned(), || {
                    Arc::new(SkipSet::default())
                })
                .value(),
        )
        .insert(topic_event.get_unique_time());
        self.event_unique_time_by_corrolation.insert(
            topic_event.get_correlation_token().to_owned(),
            (
                topic_event.get_event_id().to_owned(),
                topic_event.get_unique_time(),
            ),
        );
        // Indexed columns...
        for (index_column, value) in topic_event.get_additional_columns() {
            let index_key = match value {
                ExtractedValue::Text(value) => value,
                ExtractedValue::BigInt(value) => &value.to_string(),
            };
            self.indices
                .get_or_insert_with(index_column.to_owned(), SkipMap::default)
                .value()
                .get_or_insert_with(index_key.to_owned(), SkipSet::default)
                .value()
                .insert((
                    topic_event.get_event_id().to_owned(),
                    topic_event.get_unique_time(),
                ));
        }
        topic_event.get_correlation_token().to_owned()
    }

    /// Add new events to the delivery cache of the consumer.
    pub fn populate_delivery_cache_with_fresh(
        &self,
        consumer_id: &str,
        consumer_delivery_cache: &dyn DeliveryIntentTemplateInsertable,
        attempted_low_exclusive: UniqueTime,
    ) -> (u64, bool) {
        let consumer = Arc::clone(
            self.consumers
                .get_or_insert_with(
                    consumer_id.to_owned(),
                    || Arc::new(InMemConsumer::default()),
                )
                .value(),
        );
        let mut next = self.events.front();
        if let Some(first_entry) = self.events.get(&attempted_low_exclusive) {
            next = first_entry.next();
        }
        let mut last_attempted_ts = attempted_low_exclusive.as_encoded();
        let mut any_new_found = false;
        while let Some(event_entry) = next {
            // Skip intents marked as done
            let no_done =
                consumer
                    .delivery_intents
                    .get(event_entry.key())
                    .is_none_or(|dis_entry| {
                        !dis_entry
                            .value()
                            .iter()
                            .any(|dis_entry| dis_entry.value().is_done())
                    });
            if no_done && let Some(event) = self.events.get(event_entry.key()) {
                let event = Arc::clone(event.value());
                consumer_delivery_cache.insert(DeliveryIntentTemplate::new(
                    event.unique_time,
                    event.event_id.to_owned(),
                    event.descriptor_version,
                    None,
                ));
                last_attempted_ts = event.unique_time.as_encoded();
                any_new_found = true;
            }
            if consumer_delivery_cache.is_full() {
                break;
            }
            next = event_entry.next();
        }
        (last_attempted_ts, any_new_found)
    }

    /// Add failed deliveries to the delivery cache of the consumer.
    pub fn populate_delivery_cache_with_retries(
        &self,
        consumer_id: &str,
        consumer_delivery_cache: &dyn DeliveryIntentTemplateInsertable,
        done_low_exclusive: UniqueTime,
        freshness_duration_micros: u64,
    ) -> u64 {
        let consumer = Arc::clone(
            self.consumers
                .get_or_insert_with(
                    consumer_id.to_owned(),
                    || Arc::new(InMemConsumer::default()),
                )
                .value(),
        );
        let mut next = self.events.front();
        if let Some(first_entry) = self.events.get(&done_low_exclusive) {
            next = first_entry.next();
        }
        let mut all_done = true;
        let mut confirmed_done_ts = done_low_exclusive.as_encoded();
        let timeout_ts = fragtale_client::time::get_timestamp_micros() - freshness_duration_micros;
        while let Some(event_entry) = next {
            if consumer_delivery_cache.is_full() || event_entry.key().as_encoded() >= timeout_ts {
                break;
            }
            let no_done =
                consumer
                    .delivery_intents
                    .get(event_entry.key())
                    .is_none_or(|dis_entry| {
                        !dis_entry.value().iter().any(|dis_entry| {
                            dis_entry.value().is_done()
                                || dis_entry.value().get_intent_ts_micros() > timeout_ts
                        })
                    });
            if no_done {
                if let Some(event) = self.events.get(event_entry.key()) {
                    let event = Arc::clone(event.value());
                    consumer_delivery_cache.insert(DeliveryIntentTemplate::new(
                        event.unique_time,
                        event.event_id.to_owned(),
                        event.descriptor_version,
                        None,
                    ));
                    all_done = false;
                }
            } else if all_done {
                confirmed_done_ts = event_entry.key().as_encoded();
            }
            next = event_entry.next();
        }
        confirmed_done_ts
    }

    /// Notify `correlation_hotlist` about new events.
    pub fn track_new_events(
        &self,
        topic_id: &str,
        correlation_hotlist: &dyn CorrelationResultListener,
        hotlist_duration_micros: u64,
    ) -> bool {
        let mut any_change = false;
        let ts_start = fragtale_client::time::get_timestamp_micros() - hotlist_duration_micros;
        let mut next = if let Some(entry) = self
            .events
            .iter()
            .rev()
            .find(|entry| entry.key().as_encoded() < ts_start)
        {
            entry.next()
        } else {
            // No entries at all or no older than ts_start
            self.events.front()
        };
        while let Some(entry) = &next {
            any_change |= correlation_hotlist
                .notify_hotlist_entry(topic_id, &entry.value().correlation_token);
            next = entry.next();
        }
        any_change
    }
}
