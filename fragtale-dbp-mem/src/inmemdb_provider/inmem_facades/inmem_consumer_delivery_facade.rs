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

//! Ephemeral in-memory implementation of [ConsumerDeliveryFacade].

use crate::InMemoryDatabaseProvider;
use crate::inmemdb_provider::inmem_topic::InMemTopic;
use fragtale_dbp::dbp::facades::ConsumerDeliveryFacade;
use fragtale_dbp::mb::MessageBrokerError;
use fragtale_dbp::mb::UniqueTime;
use fragtale_dbp::mb::consumers::DeliveryIntentTemplateInsertable;
use std::sync::Arc;

/// Ephemeral in-memory specific database code
pub struct InMemConsumerDeliveryFacade {
    inmem_provider: Arc<InMemoryDatabaseProvider>,
}

impl InMemConsumerDeliveryFacade {
    /// Return a new instance.
    pub fn new(inmem_provider: &Arc<InMemoryDatabaseProvider>) -> Self {
        Self {
            inmem_provider: Arc::clone(inmem_provider),
        }
    }
}

#[async_trait::async_trait]
impl ConsumerDeliveryFacade for InMemConsumerDeliveryFacade {
    async fn ensure_consumer_setup(
        &self,
        _topic_id: &str,
        _consumer_id: &str,
        _baseline_ts: Option<u64>,
        _encoded_descriptor_version: Option<u64>,
    ) -> Result<(), MessageBrokerError> {
        // NOOP
        Ok(())
    }

    async fn consumer_get_attempted_by_id(
        &self,
        topic_id: &str,
        consumer_id: &str,
    ) -> Option<UniqueTime> {
        self.inmem_provider
            .consumer_by_id(topic_id, consumer_id)
            .get_attempted()
    }

    async fn consumer_get_done_by_id(
        &self,
        topic_id: &str,
        consumer_id: &str,
    ) -> Option<UniqueTime> {
        self.inmem_provider
            .consumer_by_id(topic_id, consumer_id)
            .get_done()
    }

    async fn consumer_set_attempted_by_id(
        &self,
        topic_id: &str,
        consumer_id: &str,
        attempted: UniqueTime,
    ) -> bool {
        self.inmem_provider
            .consumer_by_id(topic_id, consumer_id)
            .set_attempted(attempted);
        true
    }

    async fn consumer_set_done_by_id(
        &self,
        topic_id: &str,
        consumer_id: &str,
        done: UniqueTime,
    ) -> bool {
        self.inmem_provider
            .consumer_by_id(topic_id, consumer_id)
            .set_done(done);
        true
    }

    async fn delivery_intent_mark_done(
        &self,
        topic_id: &str,
        consumer_id: &str,
        unique_time: UniqueTime,
        _delivery_instance_id: u16,
    ) {
        if let Some(delivery_intent) = self
            .inmem_provider
            .consumer_by_id(topic_id, consumer_id)
            .delivery_intent_by_unique_time(&unique_time)
        {
            delivery_intent.set_done(true)
        }
    }

    async fn delivery_intent_insert_done(
        &self,
        _topic_id: &str,
        _consumer_id: &str,
        _event_id: &str,
        _event_unique_time: UniqueTime,
        _instance_id_local: u16,
        _descriptor_version: &Option<u64>,
        _intent_ts_micros: u64,
    ) {
        // NOOP: No reason to keep an audit trail for an ephemeral db...
    }

    #[allow(clippy::too_many_arguments)]
    async fn delivery_intent_reserve(
        &self,
        topic_id: &str,
        consumer_id: &str,
        _event_id: &str,
        event_unique_time: UniqueTime,
        _instance_id_local: u16,
        _descriptor_version: &Option<u64>,
        intent_ts_micros: u64,
        _freshness_duration_micros: u64,
        _failed_intent_ts_micros: Option<u64>,
    ) -> bool {
        self.inmem_provider
            .consumer_by_id(topic_id, consumer_id)
            .delivery_intent_reserve(&event_unique_time, intent_ts_micros);
        true
    }

    async fn populate_delivery_cache_with_fresh(
        &self,
        topic_id: &str,
        consumer_id: &str,
        consumer_delivery_cache: Box<Arc<dyn DeliveryIntentTemplateInsertable>>,
        attempted_low_exclusive: UniqueTime,
    ) -> (u64, bool) {
        let (last_attempted_ts, any_new_found) = self
            .inmem_provider
            .topics
            .get_or_insert_with(topic_id.to_owned(), InMemTopic::default)
            .value()
            .populate_delivery_cache_with_fresh(
                consumer_id,
                consumer_delivery_cache.as_ref().as_ref(),
                attempted_low_exclusive,
            );
        (last_attempted_ts, any_new_found)
    }

    async fn populate_delivery_cache_with_retries(
        &self,
        topic_id: &str,
        consumer_id: &str,
        consumer_delivery_cache: Box<Arc<dyn DeliveryIntentTemplateInsertable>>,
        done_low_exclusive: UniqueTime,
        freshness_duration_micros: u64,
        _clock_skew_tolerance_micros: u64,
    ) -> u64 {
        self.inmem_provider
            .topics
            .get_or_insert_with(topic_id.to_owned(), InMemTopic::default)
            .value()
            .populate_delivery_cache_with_retries(
                consumer_id,
                consumer_delivery_cache.as_ref().as_ref(),
                done_low_exclusive,
                freshness_duration_micros,
            )
    }
}
