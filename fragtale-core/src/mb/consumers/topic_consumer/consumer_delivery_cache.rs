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

//! Cache of events to delivery to a connected consumer.

use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::SkipSet;
use fragtale_dbp::mb::UniqueTime;
use fragtale_dbp::mb::consumers::DeliveryIntentTemplate;
use fragtale_dbp::mb::consumers::DeliveryIntentTemplateInsertable;
use std::sync::Arc;

/** A cache of events that should be delivered to a connected consumer.

This cache also tracks recently pulled events, to prevent a race condition where
the same event might be added again.
*/
#[derive(Default)]
pub struct ConsumerDeliveryCache {
    events: SkipMap<UniqueTime, DeliveryIntentTemplate>,
    recently_pulled: SkipSet<UniqueTime>,
}

impl ConsumerDeliveryCache {
    const MAX_CACHE_SIZE: usize = 1024;

    /// Return a new instance.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Return a guesstimate of the number of events pending delivery in cache.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Return a guesstimate of the number of events recently pulled for
    /// delivery.
    pub fn len_recent(&self) -> usize {
        self.recently_pulled.len()
    }

    /// Return the next event to delivery ordered by UniqueTime.
    pub fn get_next_delivery_intent_template(&self) -> Option<DeliveryIntentTemplate> {
        // Pull from list until a DeliveryIntent has been successfully reserved
        self.events.pop_front().map(|entry| {
            let delivery_intent_template = entry.value().clone();
            // Best effort to prevent some unnessary reservation attemps (small race condition here)
            self.recently_pulled
                .insert(delivery_intent_template.get_unique_time());
            delivery_intent_template
        })
    }
}

impl DeliveryIntentTemplateInsertable for ConsumerDeliveryCache {
    fn insert(&self, delivery_intent_template: DeliveryIntentTemplate) {
        // Remove entry if it already existed to delay re-insert
        if self
            .recently_pulled
            .remove(&delivery_intent_template.get_unique_time())
            .is_none()
        {
            self.events.insert(
                delivery_intent_template.get_unique_time(),
                delivery_intent_template,
            );
        }
    }

    fn is_full(&self) -> bool {
        // Guesstimate
        self.events.len() > Self::MAX_CACHE_SIZE
    }
}
