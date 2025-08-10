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

//! Database facade for operation related to delivery of events to consumers.

use crate::mb::MessageBrokerError;
use crate::mb::UniqueTime;
use crate::mb::consumers::DeliveryIntentTemplateInsertable;
use std::sync::Arc;

/// Database facade for operation related to delivery of events to consumers.
#[async_trait::async_trait]
pub trait ConsumerDeliveryFacade: Send + Sync {
    /// Ensure that the consumer is setup and updated in the database
    async fn ensure_consumer_setup(
        &self,
        topic_id: &str,
        consumer_id: &str,
        baseline_ts: Option<u64>,
        encoded_descriptor_version: Option<u64>,
    ) -> Result<(), MessageBrokerError>;

    /// Get latest [UniqueTime] that is confirmed to be attempted for delivery
    async fn consumer_get_attempted_by_id(
        &self,
        topic_id: &str,
        consumer_id: &str,
    ) -> Option<UniqueTime>;

    /// Get latest [UniqueTime] that is confirmed to be delivered for delivery
    async fn consumer_get_done_by_id(
        &self,
        topic_id: &str,
        consumer_id: &str,
    ) -> Option<UniqueTime>;

    /**
    Set latest [UniqueTime] that is confirmed to be attempted for delivery.

    Return `true` if the change was applied.
    */
    async fn consumer_set_attempted_by_id(
        &self,
        topic_id: &str,
        consumer_id: &str,
        attempted: UniqueTime,
    ) -> bool;

    /**
    Set latest [UniqueTime] that is confirmed to be delivered for delivery.

    Return `true` if the change was applied.
    */
    async fn consumer_set_done_by_id(
        &self,
        topic_id: &str,
        consumer_id: &str,
        done: UniqueTime,
    ) -> bool;

    /// Mark a delivery to never be considered again (due to success or fail)
    async fn delivery_intent_mark_done(
        &self,
        topic_id: &str,
        consumer_id: &str,
        unique_time: UniqueTime,
        delivery_instance_id: u16,
    );

    /**
    Insert a delivery intent as an audit record tying the consumer_id to the
    retrieval of an event.

    The event is expected to be delivering by other means, like in a response to
    a query.
    */
    #[allow(clippy::too_many_arguments)]
    async fn delivery_intent_insert_done(
        &self,
        topic_id: &str,
        consumer_id: &str,
        event_id: &str,
        event_unique_time: UniqueTime,
        instance_id_local: u16,
        descriptor_version: &Option<u64>,
        intent_ts_micros: u64,
    );

    /**
    Attempt to reserve an intent to deliver an event from this instance.

    Return `true` if the attempt was successful.
    */
    #[allow(clippy::too_many_arguments)]
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
    ) -> bool;

    /// Populate [DeliveryIntentTemplateInsertable] implementation with fresh
    /// intents to deliver events.
    async fn populate_delivery_cache_with_fresh(
        &self,
        topic_id: &str,
        consumer_id: &str,
        consumer_delivery_cache: Box<Arc<dyn DeliveryIntentTemplateInsertable>>,
        attempted_low_exclusive: UniqueTime,
    ) -> (u64, bool);

    /// Populate [DeliveryIntentTemplateInsertable] implementation with failed
    /// intents to deliver events for retry.
    async fn populate_delivery_cache_with_retries(
        &self,
        topic_id: &str,
        consumer_id: &str,
        consumer_delivery_cache: Box<Arc<dyn DeliveryIntentTemplateInsertable>>,
        done_low_exclusive: UniqueTime,
        freshness_duration_micros: u64,
        clock_skew_tolerance_micros: u64,
    ) -> u64;
}
