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

//! Event identifier lookup by UniqueTime entity and persistence.

use super::FromSignedOrDefault;
use super::FromUnsignedOrDefault;
use crate::CassandraProvider;
use crate::CassandraResultMapper;
use fragtale_dbp::mb::TopicEvent;
use fragtale_dbp::mb::UniqueTime;

/// Event identifier lookup by UniqueTime entity and persistence.
#[derive(
    Clone, Debug, cdrs_tokio::IntoCdrsValue, cdrs_tokio::TryFromRow, cdrs_tokio::TryFromUdt,
)]
pub struct EventIdByUniqueTimeEntity {
    // The bucket part of the unqiue time.
    unique_time_bucket: i64,
    /// Encoded unique time.
    unique_time: i64,
    /// The event identifier.
    event_id: String,
    /// Optional descriptor version
    descriptor_version: Option<i64>,
    /// Unique identifier that clients can propagate through the system
    correlation_token: String,
}

impl From<&TopicEvent> for EventIdByUniqueTimeEntity {
    fn from(value: &TopicEvent) -> Self {
        Self::new(
            value.get_unique_time(),
            value.get_event_id(),
            &value.get_descriptor_version(),
            value.get_correlation_token(),
        )
    }
}

impl EventIdByUniqueTimeEntity {
    pub(crate) const CQL_TABLE_NAME: &'static str = "event_id_by_unique_time";

    const CQL_TEMPLATE_CREATE_TABLE: &'static str = "
        CREATE TABLE IF NOT EXISTS event_id_by_unique_time (
            unique_time_bucket  bigint,
            unique_time         bigint,
            event_id            text,
            descriptor_version  bigint,
            correlation_token   text,
            PRIMARY KEY ((unique_time_bucket), unique_time)
        ) WITH CLUSTERING ORDER BY (unique_time ASC);
        ";

    /// QEBU1. Insert event by unique time lookup entity.
    const CQL_TEMPLATE_INSERT: &'static str = "
        INSERT INTO {{ keyspace }}.event_id_by_unique_time
        (unique_time_bucket, unique_time, event_id, descriptor_version, correlation_token)
        VALUES (?,?,?,?,?)
        ;";

    /// QEBU2. Get event identifiers (full entity) in UniqueTime range.
    const CQL_TEMPLATE_SELECT_BY_UNIQUE_TIME: &'static str = "
        SELECT unique_time_bucket, unique_time, event_id, descriptor_version, correlation_token
        FROM {{ keyspace }}.event_id_by_unique_time
        WHERE unique_time_bucket = ? AND unique_time > ?
        LIMIT {{ limit }}
        ";

    //// Return a new instance.
    pub fn new(
        unique_time: UniqueTime,
        event_id: &str,
        descriptor_version: &Option<u64>,
        correlation_token: &str,
    ) -> Self {
        Self {
            unique_time_bucket: unique_time.get_bucket_i64(),
            unique_time: unique_time.as_encoded_i64(),
            event_id: event_id.to_owned(),
            descriptor_version: descriptor_version.map(i64::from_unsigned),
            correlation_token: correlation_token.to_owned(),
        }
    }

    /// Return the [UniqueTime] of the event.
    pub fn get_unique_time(&self) -> UniqueTime {
        UniqueTime::from(self.unique_time)
    }

    /// Return the identifier of the event.
    pub fn get_event_id(&self) -> &str {
        &self.event_id
    }

    /// Return the event descriptor version the event adheres to.
    pub fn get_descriptor_version(&self) -> Option<u64> {
        self.descriptor_version.map(u64::from_signed)
    }

    /// Return the correlation token assigned to this event.
    pub fn get_correlation_token(&self) -> &str {
        &self.correlation_token
    }

    /// Create entity table and indices.
    pub async fn create_table_and_indices(db: &CassandraProvider, topic_id: &str) {
        let keyspace = &db.get_keyspace_from_topic(topic_id);
        db.create_table(
            keyspace,
            Self::CQL_TABLE_NAME,
            Self::CQL_TEMPLATE_CREATE_TABLE,
        )
        .await;
    }

    /// Insert entity (uncondictional).
    pub async fn insert(&self, db: &CassandraProvider, topic_id: &str) -> bool {
        db.query_with_keyspace_and_values(
            Self::CQL_TEMPLATE_INSERT,
            &db.get_keyspace_from_topic(topic_id),
            cdrs_tokio::query_values!(
                self.unique_time_bucket,
                self.unique_time,
                self.event_id.to_owned(),
                self.descriptor_version,
                self.correlation_token.to_owned()
            ),
        )
        .await
        .map(CassandraResultMapper::into_applied)
        .unwrap_or(false)
    }

    /// Select all entities with a encoded UniqueTime greater than
    /// `unique_time_low_exclusive`.
    pub async fn select_by_unique_time(
        db: &CassandraProvider,
        topic_id: &str,
        bucket: u64,
        unique_time_low_exclusive: u64,
        max_results: usize,
    ) -> Vec<Self> {
        let unique_time_low_exclusive = i64::from_unsigned(unique_time_low_exclusive);
        let keyspace = &db.get_keyspace_from_topic(topic_id);
        let values = cdrs_tokio::query_values!(bucket, unique_time_low_exclusive);
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
        .unwrap_or_default()
    }
}
