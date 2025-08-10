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

//! Ephemeral in-memory implementation a consumer.

mod inmem_delivery_intent;

pub use self::inmem_delivery_intent::InMemDeliveryIntent;
use crossbeam_skiplist::SkipMap;
use fragtale_dbp::mb::UniqueTime;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;

/// Ephemeral in-memory implementation a consumer.
#[derive(Debug, Default)]
pub struct InMemConsumer {
    attempted: AtomicU64,
    done: AtomicU64,
    pub delivery_intents: SkipMap<UniqueTime, SkipMap<u64, Arc<InMemDeliveryIntent>>>,
}

impl InMemConsumer {
    const MICROS_SINCE_EPOCH_20240101: u64 = 1_702_944_000_000_000;

    /// Return time of the event that was last attempted for delivery.
    pub fn get_attempted(&self) -> Option<UniqueTime> {
        let mut value = self.attempted.load(Relaxed);
        if value == 0 {
            value = Self::MICROS_SINCE_EPOCH_20240101;
            self.attempted.store(value, Relaxed);
        }
        Some(UniqueTime::new(value, 0))
    }

    /// Set the time of the event that was last attempted intent for delivery.
    pub fn set_attempted(&self, value: UniqueTime) {
        self.attempted.store(value.as_encoded(), Relaxed);
    }

    /// Return time of the event that was confirmed to be delivered.
    ///
    /// (All events before this point must have been delivered.)
    pub fn get_done(&self) -> Option<UniqueTime> {
        let mut value = self.done.load(Relaxed);
        if value == 0 {
            value = Self::MICROS_SINCE_EPOCH_20240101;
            self.done.store(value, Relaxed);
        }
        Some(UniqueTime::new(value, 0))
    }

    /// Set the time of the event that was confirmed to be delivered.
    ///
    /// (All events before this point must have been delivered.)
    pub fn set_done(&self, value: UniqueTime) {
        self.done.store(value.as_encoded(), Relaxed);
    }

    /// Retrieve delivery intent by [UniqueTime].
    pub fn delivery_intent_by_unique_time(
        &self,
        unique_time: &UniqueTime,
    ) -> Option<Arc<InMemDeliveryIntent>> {
        self.delivery_intents
            .get_or_insert_with(unique_time.to_owned(), SkipMap::default)
            .value()
            .back()
            .map(|entry| Arc::clone(entry.value()))
    }

    /// Reserve a delivery intent.
    pub fn delivery_intent_reserve(&self, unique_time: &UniqueTime, intent_ts_micros: u64) {
        self.delivery_intents
            .get_or_insert_with(unique_time.to_owned(), SkipMap::default)
            .value()
            .get_or_insert_with(intent_ts_micros, || {
                Arc::new(InMemDeliveryIntent::new(intent_ts_micros))
            });
    }
}
