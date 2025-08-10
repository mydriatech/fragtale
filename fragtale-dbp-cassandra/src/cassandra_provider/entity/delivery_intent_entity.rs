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

//! Delivery intent entity and persistence.

use super::FromSignedOrDefault;
use super::FromUnsignedOrDefault;
use crate::CassandraProvider;
use crate::CassandraResultMapper;
use fragtale_dbp::mb::UniqueTime;

/// Delivery intent entity and persistence.
#[derive(
    Clone, Debug, cdrs_tokio::IntoCdrsValue, cdrs_tokio::TryFromRow, cdrs_tokio::TryFromUdt,
)]
pub struct DeliveryIntentEntity {
    /// Unique identifier per consumer group.
    consumer_id: String,
    /// Bucket part of [UniqueTime] of the event to deliver.
    unique_time_bucket: i64,
    /// [UniqueTime] of the event to deliver.
    unique_time: i64,
    /// Instance identifier claim of the instance that created this intent to
    /// deliver.
    ///
    /// Note that this is not the same as the instance_id encoded into the
    /// [UniqueTime] which identifies the instance that recieved the event.
    delivering_instance_id: i16,
    /// Time of intent to delivery in epoch micros.
    intent_ts: i64,
    /// The identifier of the event to deliver.
    event_id: String,
    /// Marks this intent as retracted.
    ///
    /// When multiple instances writes an intent to deliver the same event, the
    /// nodes can withdraw their intent by setting this flag until a single
    /// instance can guarantee that the event is only sent once.
    retracted: bool,
    /// Marks this intent to deliver as completed and it should not be
    /// considered again.
    ///
    /// This flag does not distinguish between successful or unrecoverably
    /// failed deliveries.
    done: bool,
    /// Optional event descriptor version.
    descriptor_version: Option<i64>,
    /// Database time in microseconds of when the `retracted` column was last
    /// written to.
    retracted_write_time: i64,
}

impl DeliveryIntentEntity {
    pub(crate) const CQL_TABLE_NAME: &'static str = "delivery_intent";

    const CQL_TEMPLATE_CREATE_TABLE: &'static str = "
        CREATE TABLE IF NOT EXISTS delivery_intent (
            consumer_id             text,
            unique_time_bucket      bigint,
            unique_time             bigint,
            delivering_instance_id  smallint,
            intent_ts               bigint,
            event_id                text,
            retracted               boolean,
            done                    boolean,
            descriptor_version      bigint,
            PRIMARY KEY ((consumer_id, unique_time_bucket), unique_time, delivering_instance_id)
        ) WITH CLUSTERING ORDER BY (unique_time ASC);
        ";

    /// QDI1. Create intent of delivery
    const CQL_TEMPLATE_INSERT: &'static str = "
        INSERT INTO {{ keyspace }}.delivery_intent
        (consumer_id, unique_time_bucket, unique_time, delivering_instance_id, intent_ts, event_id, retracted, done, descriptor_version)
        VALUES (?,?,?,?,?,?,?,?,?)
        ";

    /// QDI2. Find intents by UniqueTime
    const CQL_TEMPLATE_SELECT_BY_UNIQUE_TIME: &'static str = "
        SELECT consumer_id, unique_time_bucket, unique_time, delivering_instance_id, intent_ts, event_id, retracted, done, descriptor_version, WRITETIME (retracted) AS retracted_write_time
        FROM delivery_intent
        WHERE consumer_id = ? AND unique_time_bucket = ? AND unique_time > ? AND unique_time <= ?
        LIMIT {{ limit }}
        ";

    /// QDIx. Find intents by exact UniqueTime
    const CQL_TEMPLATE_SELECT_BY_UNIQUE_TIME_EXACT: &'static str = "
        SELECT consumer_id, unique_time_bucket, unique_time, delivering_instance_id, intent_ts, event_id, retracted, done, descriptor_version, WRITETIME (retracted) AS retracted_write_time
        FROM delivery_intent
        WHERE consumer_id= ? AND unique_time_bucket = ? AND unique_time = ?
        LIMIT 1024
        ";

    const CQL_TEMPLATE_UPDATE_RETRACTED: &'static str = "
        UPDATE delivery_intent
        SET retracted = ?
        WHERE consumer_id= ? AND unique_time_bucket = ? AND unique_time = ? AND delivering_instance_id = ?
        ";

    const CQL_TEMPLATE_UPDATE_RETRACTED_AND_TS: &'static str = "
        UPDATE delivery_intent
        SET retracted = ?, intent_ts = ?
        WHERE consumer_id=? AND unique_time_bucket = ? AND unique_time = ? AND delivering_instance_id = ?
        ";

    /// QDE3. Update intent of delivery for event that had an expired intent
    const CQL_TEMPLATE_UPDATE_ON_RETRY: &'static str = "
        UPDATE delivery_intent
        SET intent_ts = ?
        WHERE consumer_id = ? AND unique_time_bucket = ? AND unique_time = ? AND delivering_instance_id = ?
        IF intent_ts = ? AND done = false
        ";

    /// QDE4. Update intent on completion of delivery (ignoring intent_ts)
    const CQL_TEMPLATE_UPDATE_ON_DONE: &'static str = "
        UPDATE delivery_intent
        SET done=true
        WHERE consumer_id = ? AND unique_time_bucket = ? AND unique_time = ? AND delivering_instance_id = ?
        ";

    /// Create a new instance.
    ///
    /// By default, the [DeliveryIntentEntity] is not done nor retracted.
    pub fn new(
        consumer_id: &str,
        unique_time: UniqueTime,
        delivering_instance_id: u16,
        intent_ts: u64,
        event_id: &str,
        descriptor_version: &Option<u64>,
    ) -> Self {
        Self {
            consumer_id: consumer_id.to_owned(),
            unique_time_bucket: unique_time.get_bucket_i64(),
            unique_time: unique_time.as_encoded_i64(),
            delivering_instance_id: i16::from_unsigned(delivering_instance_id),
            intent_ts: i64::from_unsigned(intent_ts),
            event_id: event_id.to_owned(),
            retracted: false,
            done: false,
            descriptor_version: descriptor_version.map(i64::from_unsigned),
            retracted_write_time: 0,
        }
    }

    /// Create a new instance that will be delivered by other means.
    ///
    /// The entry will be marked as done from the start to avoid additinal
    /// processing.
    ///
    /// This acts as an audit trail to ensure that all retrieved events are
    /// coupled to a consumer_id.
    pub fn new_delivered(
        consumer_id: &str,
        unique_time: UniqueTime,
        delivering_instance_id: u16,
        intent_ts: u64,
        event_id: &str,
        descriptor_version: &Option<u64>,
    ) -> Self {
        Self {
            consumer_id: consumer_id.to_owned(),
            unique_time_bucket: unique_time.get_bucket_i64(),
            unique_time: unique_time.as_encoded_i64(),
            delivering_instance_id: i16::from_unsigned(delivering_instance_id),
            intent_ts: i64::from_unsigned(intent_ts),
            event_id: event_id.to_owned(),
            retracted: false,
            done: true,
            descriptor_version: descriptor_version.map(i64::from_unsigned),
            retracted_write_time: 0,
        }
    }

    /// The [UniqueTime] of the event to deliver.
    pub fn get_unique_time(&self) -> UniqueTime {
        UniqueTime::from(self.unique_time)
    }

    /// Return the instance identifier claim of the instance that created this
    /// intent to deliver.
    ///
    /// Note that this is not the same as the instance_id encoded into the
    /// [UniqueTime] which identifies the instance that recieved the event.
    pub fn get_delivering_instance_id(&self) -> u16 {
        u16::from_signed(self.delivering_instance_id)
    }

    /// Time of intent to delivery in epoch micros.
    pub fn get_intent_ts(&self) -> u64 {
        u64::from_signed(self.intent_ts)
    }

    /// Return the event identifier.
    pub fn get_event_id(&self) -> &str {
        &self.event_id
    }

    /// Return `true` if this intent as retracted.
    ///
    /// When multiple instances writes an intent to deliver the same event, the
    /// nodes can withdraw their intent by setting this flag until a single
    /// instance can guarantee that the event is only sent once.
    pub fn get_retracted(&self) -> bool {
        self.retracted
    }

    /// Returns `true` if this intent to deliver as completed and it should not
    /// be considered again.
    ///
    /// This flag does not distinguish between successful or unrecoverably
    /// failed deliveries.
    pub fn get_done(&self) -> bool {
        self.done
    }

    /// Optional event descriptor version.
    pub fn get_descriptor_version(&self) -> Option<u64> {
        self.descriptor_version.map(u64::from_signed)
    }

    /// Database time in microseconds of when the `retracted` column was last
    /// written to.
    pub fn get_retracted_write_time(&self) -> u64 {
        u64::from_signed(self.retracted_write_time)
    }

    /// Create table and indices.
    pub async fn create_table_and_indices(db: &CassandraProvider, topic_id: &str) {
        let keyspace = &db.get_keyspace_from_topic(topic_id);
        db.create_table(
            keyspace,
            Self::CQL_TABLE_NAME,
            Self::CQL_TEMPLATE_CREATE_TABLE,
        )
        .await;
    }

    /// Insert entity (unconditional).
    pub async fn insert(&self, db: &CassandraProvider, topic_id: &str) {
        db.query_with_keyspace_and_values(
            Self::CQL_TEMPLATE_INSERT,
            &db.get_keyspace_from_topic(topic_id),
            cdrs_tokio::query_values!(
                self.consumer_id.to_owned(),
                self.unique_time_bucket,
                self.unique_time,
                self.delivering_instance_id,
                self.intent_ts,
                self.event_id.to_owned(),
                self.retracted,
                self.done,
                self.descriptor_version
            ),
        )
        .await
        .map(CassandraResultMapper::into_applied)
        .unwrap_or_else(|| {
            log::debug!("Failed insert: {self:?}");
            false
        });
    }

    /// Return entities from a `bucket` in the range
    /// `[unique_time_low_exclusive+1..=unique_time_high_inclusive]`
    /// ordered by `unique_time`.
    ///
    /// `unique_time_high_inclusive` might not be reached if there are more
    /// results than `max_results` in the range.
    pub async fn select_by_unique_time(
        db: &CassandraProvider,
        topic_id: &str,
        consumer_id: &str,
        bucket: u64,
        unique_time_low_exclusive: u64,
        unique_time_high_inclusive: u64,
        max_results: usize,
    ) -> Vec<Self> {
        let keyspace = &db.get_keyspace_from_topic(topic_id);
        let values = cdrs_tokio::query_values!(
            consumer_id.to_owned(),
            bucket,
            i64::from_unsigned(unique_time_low_exclusive),
            i64::from_unsigned(unique_time_high_inclusive)
        );
        db.query_with_keyspace_and_values(
            &Self::CQL_TEMPLATE_SELECT_BY_UNIQUE_TIME.replacen(
                "{{ limit }}",
                &max_results.to_string(),
                1,
            ),
            keyspace,
            values,
        )
        .await
        .map(CassandraResultMapper::into_entities)
        .map(|entities| {
            entities
                .into_iter()
                .filter(|die: &Self| !die.get_retracted())
                .collect()
        })
        .unwrap_or_default()
    }

    /// Return all entities for a unique_time.
    ///
    /// Multiple instances might have attempted the delivery for the same event.
    pub async fn select_by_unique_time_only_vec(
        db: &CassandraProvider,
        topic_id: &str,
        consumer_id: &str,
        unique_time: UniqueTime,
    ) -> Vec<Self> {
        let keyspace = &db.get_keyspace_from_topic(topic_id);
        let values = cdrs_tokio::query_values!(
            consumer_id.to_owned(),
            unique_time.get_bucket_i64(),
            unique_time.as_encoded_i64()
        );
        db.query_with_keyspace_and_values(
            Self::CQL_TEMPLATE_SELECT_BY_UNIQUE_TIME_EXACT,
            keyspace,
            values,
        )
        .await
        .map(CassandraResultMapper::into_entities)
        .map(|vec| {
            vec.into_iter()
                .filter(|die: &Self| !die.get_retracted())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
    }

    /// Set the retracted flag for delivery intent.
    pub async fn update_retracted(
        db: &CassandraProvider,
        topic_id: &str,
        consumer_id: &str,
        unique_time: UniqueTime,
        delivering_instance_id: u16,
        retracted: bool,
    ) -> bool {
        db.query_with_keyspace_and_values(
            Self::CQL_TEMPLATE_UPDATE_RETRACTED,
            &db.get_keyspace_from_topic(topic_id),
            cdrs_tokio::query_values!(
                retracted,
                consumer_id.to_owned(),
                unique_time.get_bucket_i64(),
                unique_time.as_encoded_i64(),
                i16::from_unsigned(delivering_instance_id)
            ),
        )
        .await
        .map(CassandraResultMapper::into_applied)
        .unwrap_or(false)
    }

    /// Update retracted and time of intent.
    pub async fn update_retracted_and_intent_ts(
        db: &CassandraProvider,
        topic_id: &str,
        consumer_id: &str,
        unique_time: UniqueTime,
        delivering_instance_id: u16,
        retracted: bool,
        intent_ts: u64,
    ) -> bool {
        db.query_with_keyspace_and_values(
            Self::CQL_TEMPLATE_UPDATE_RETRACTED_AND_TS,
            &db.get_keyspace_from_topic(topic_id),
            cdrs_tokio::query_values!(
                retracted,
                i64::from_unsigned(intent_ts),
                consumer_id.to_owned(),
                unique_time.get_bucket_i64(),
                unique_time.as_encoded_i64(),
                i16::from_unsigned(delivering_instance_id)
            ),
        )
        .await
        .map(CassandraResultMapper::into_applied)
        .unwrap_or(false)
    }

    /// Update time of intent, unless it is done.
    pub async fn update_on_retry(
        db: &CassandraProvider,
        topic_id: &str,
        consumer_id: &str,
        unique_time: UniqueTime,
        delivering_instance_id: u16,
        intent_ts_new: u64,
        intent_ts_old: u64,
    ) -> bool {
        db.query_with_keyspace_and_values(
            Self::CQL_TEMPLATE_UPDATE_ON_RETRY,
            &db.get_keyspace_from_topic(topic_id),
            cdrs_tokio::query_values!(
                i64::from_unsigned(intent_ts_new),
                consumer_id.to_owned(),
                unique_time.get_bucket_i64(),
                unique_time.as_encoded_i64(),
                i16::from_unsigned(delivering_instance_id),
                i64::from_unsigned(intent_ts_old)
            ),
        )
        .await
        .map(CassandraResultMapper::into_applied)
        .unwrap_or(false)
    }

    /// Mark delivery intent as completed.
    pub async fn update_on_done(
        db: &CassandraProvider,
        topic_id: &str,
        consumer_id: &str,
        unique_time: UniqueTime,
        delivering_instance_id: u16,
    ) -> bool {
        db.query_with_keyspace_and_values(
            Self::CQL_TEMPLATE_UPDATE_ON_DONE,
            &db.get_keyspace_from_topic(topic_id),
            cdrs_tokio::query_values!(
                consumer_id.to_owned(),
                unique_time.get_bucket_i64(),
                unique_time.as_encoded_i64(),
                i16::from_unsigned(delivering_instance_id)
            ),
        )
        .await
        .map(CassandraResultMapper::into_applied)
        .unwrap_or(false)
    }
}
