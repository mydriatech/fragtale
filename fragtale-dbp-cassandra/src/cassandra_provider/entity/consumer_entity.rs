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

//! Consumer entity and persistence.

use super::FromSignedOrDefault;
use super::FromUnsignedOrDefault;
use crate::CassandraProvider;
use crate::CassandraResultMapper;
use fragtale_dbp::mb::UniqueTime;

/// Consumer entity tracks when a consumer last connected and which events
/// that has been delivered and attempted for delivery.
#[derive(
    Clone, Debug, cdrs_tokio::IntoCdrsValue, cdrs_tokio::TryFromRow, cdrs_tokio::TryFromUdt,
)]
pub struct ConsumerEntity {
    /// Unique identifier per consumer group
    consumer_id: String,
    /// Last time a client from the consumer group connected
    last_update_ts: i64,
    /// Latest event desciptor version a client from the consumer group has
    /// reported as supported.
    latest_descriptor_version: Option<i64>,
    /// Baseline priority timestamp where sending the event was attemped
    ///
    /// Stored as signed encoded UniqueTime.
    unique_time_attempted: i64,
    /// Baseline priority timestamp where confirmation or similar of the event has happened
    ///
    /// Stored as signed encoded UniqueTime.
    unique_time_done: i64,
}

impl ConsumerEntity {
    pub(crate) const CQL_TABLE_NAME: &'static str = "consumer";

    const CQL_TEMPLATE_CREATE_TABLE: &'static str = "
        CREATE TABLE IF NOT EXISTS consumer (
            consumer_id                 text,
            last_update_ts              bigint,
            latest_descriptor_version   bigint,
            unique_time_attempted       bigint,
            unique_time_done            bigint,
            PRIMARY KEY (consumer_id)
        );
        ";

    /// QC1: Insert new consumer with baseline (0 baselines means full history)
    const CQL_TEMPLATE_INSERT_IF_NOT_EXISTS: &'static str = "
        INSERT INTO consumer
        (consumer_id, last_update_ts, latest_descriptor_version, unique_time_attempted, unique_time_done)
        VALUES (?,?,?,?,?)
        IF NOT EXISTS
        ";

    /// QC2: Upsert consumer when connecting
    const CQL_TEMPLATE_UPDATE_LAST_SEEN: &'static str = "
        UPDATE consumer
        SET last_update_ts=?
        WHERE consumer_id=?
        ;";

    /// QC3: Upsert supported event descriptor version by consumer
    const CQL_TEMPLATE_UPDATE_LATEST_VERSION: &'static str = "
        UPDATE consumer
        SET latest_descriptor_version=?
        WHERE consumer_id=?
        ;";

    /// QC4. Get full entity
    const CQL_TEMPLATE_SELECT: &'static str = "
        SELECT consumer_id, last_update_ts, latest_descriptor_version, unique_time_attempted, unique_time_done
        FROM consumer
        WHERE consumer_id=?
        ";

    /// QC5. Update consumer's attempted baseline
    const CQL_TEMPLATE_UPDATE_ATTEMPTED: &'static str = "
        UPDATE consumer
        SET unique_time_attempted=?
        WHERE consumer_id=?
        ";

    /// QC6. Update consumer's done baseline
    const CQL_TEMPLATE_UPDATE_DONE: &'static str = "
        UPDATE consumer
        SET unique_time_done=?
        WHERE consumer_id=?
        ";

    const MICROS_SINCE_EPOCH_20240101: u64 = 1_702_944_000_000_000;

    /**
       Initialize a new consumer.

       A `baseline_ts` of `Some(0)` implies a full replay of history.
       An undefined baseline will use the `last_update_ts` which implies 'from this point on'.
    */
    pub fn new(
        consumer_id: String,
        last_update_ts: u64,
        baseline_ts: Option<u64>,
        latest_descriptor_version: Option<u64>,
    ) -> Self {
        let baseline_ts = baseline_ts.unwrap_or(last_update_ts);
        // We can do better than this, but don't go looking for events before this software ever existed.
        let baseline_ts = UniqueTime::min_encoded_for_micros(std::cmp::max(
            Self::MICROS_SINCE_EPOCH_20240101,
            baseline_ts,
        ));
        let baseline_ts_i64 = i64::from_unsigned(baseline_ts);
        Self {
            consumer_id,
            last_update_ts: i64::from_unsigned(last_update_ts),
            latest_descriptor_version: latest_descriptor_version.map(i64::from_unsigned),
            unique_time_attempted: baseline_ts_i64,
            unique_time_done: baseline_ts_i64,
        }
    }

    /// Return the consumer identifier.
    pub fn get_consumer_id(&self) -> &str {
        &self.consumer_id
    }

    /// Last time a client from the consumer group connected
    pub fn get_last_update_ts(&self) -> u64 {
        u64::from_signed(self.last_update_ts)
    }

    /// Last time a client from the consumer group connected
    pub fn get_latest_descriptor_version(&self) -> Option<u64> {
        self.latest_descriptor_version.map(u64::from_signed)
    }

    /// Get [UniqueTime] baseline for event delivery attempts.
    pub fn get_unique_time_attempted(&self) -> UniqueTime {
        UniqueTime::from(self.unique_time_attempted)
    }

    /// Get [UniqueTime] baseline for event deliveries that has completed.
    pub fn get_unique_time_done(&self) -> UniqueTime {
        UniqueTime::from(self.unique_time_done)
    }

    /// Create table and indices for this entity.
    pub async fn create_table_and_indices(db: &CassandraProvider, topic_id: &str) {
        let keyspace = &db.get_keyspace_from_topic(topic_id);
        db.create_table(
            keyspace,
            Self::CQL_TABLE_NAME,
            Self::CQL_TEMPLATE_CREATE_TABLE,
        )
        .await;
    }

    /// Insert entity unless it already exists.
    pub async fn insert_if_not_exists(&self, db: &CassandraProvider, topic_id: &str) -> bool {
        db.query_with_keyspace_and_values(
            Self::CQL_TEMPLATE_INSERT_IF_NOT_EXISTS,
            &db.get_keyspace_from_topic(topic_id),
            cdrs_tokio::query_values!(
                self.consumer_id.to_owned(),
                self.last_update_ts,
                self.latest_descriptor_version,
                self.unique_time_attempted,
                self.unique_time_done
            ),
        )
        .await
        .map(CassandraResultMapper::into_applied)
        .unwrap_or(false)
    }

    /// Select entity by the consumer identifier.
    pub async fn select_by_consumer_id(
        db: &CassandraProvider,
        topic_id: &str,
        consumer_id: &str,
    ) -> Option<Self> {
        let keyspace = &db.get_keyspace_from_topic(topic_id);
        let values = cdrs_tokio::query_values!("consumer_id" => consumer_id.to_owned());
        db.query_with_keyspace_and_values(Self::CQL_TEMPLATE_SELECT, keyspace, values)
            .await
            .map(CassandraResultMapper::into_entities)
            .unwrap_or_default()
            .first()
            .cloned()
    }

    /// Update last time a client from the consumer group connected.
    pub async fn update_last_update_ts(
        db: &CassandraProvider,
        topic_id: &str,
        consumer_id: &str,
        last_update_ts: u64,
    ) -> bool {
        db.query_with_keyspace_and_values(
            Self::CQL_TEMPLATE_UPDATE_LAST_SEEN,
            &db.get_keyspace_from_topic(topic_id),
            cdrs_tokio::query_values!(i64::from_unsigned(last_update_ts), consumer_id.to_owned()),
        )
        .await
        .map(CassandraResultMapper::into_applied)
        .unwrap_or(false)
    }

    /// Update latest descriptor version reported as supported by a client in
    /// the connected consumer group.
    pub async fn update_latest_descriptor_version(
        db: &CassandraProvider,
        topic_id: &str,
        consumer_id: &str,
        last_update_ts: Option<u64>,
    ) -> bool {
        db.query_with_keyspace_and_values(
            Self::CQL_TEMPLATE_UPDATE_LATEST_VERSION,
            &db.get_keyspace_from_topic(topic_id),
            cdrs_tokio::query_values!(
                i64::from_unsigned(last_update_ts.unwrap_or_default()),
                consumer_id.to_owned()
            ),
        )
        .await
        .map(CassandraResultMapper::into_applied)
        .unwrap_or(false)
    }

    /// Update [UniqueTime] baseline for attempted event delivery.
    pub async fn update_unique_time_attempted(
        db: &CassandraProvider,
        topic_id: &str,
        consumer_id: &str,
        unique_time_attempted: UniqueTime,
    ) -> bool {
        db.query_with_keyspace_and_values(
            Self::CQL_TEMPLATE_UPDATE_ATTEMPTED,
            &db.get_keyspace_from_topic(topic_id),
            cdrs_tokio::query_values!(
                unique_time_attempted.as_encoded_i64(),
                consumer_id.to_owned()
            ),
        )
        .await
        .map(CassandraResultMapper::into_applied)
        .unwrap_or(false)
    }

    /// Update [UniqueTime] baseline for completed event delivery.
    pub async fn update_unique_time_done(
        db: &CassandraProvider,
        topic_id: &str,
        consumer_id: &str,
        unique_time_done: UniqueTime,
    ) -> bool {
        db.query_with_keyspace_and_values(
            Self::CQL_TEMPLATE_UPDATE_DONE,
            &db.get_keyspace_from_topic(topic_id),
            cdrs_tokio::query_values!(unique_time_done.as_encoded_i64(), consumer_id.to_owned()),
        )
        .await
        .map(CassandraResultMapper::into_applied)
        .unwrap_or(false)
    }
}
