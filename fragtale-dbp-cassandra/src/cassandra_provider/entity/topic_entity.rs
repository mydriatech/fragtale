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

//! Topic entity and persistence.

use super::FromUnsignedOrDefault;
use crate::CassandraProvider;
use crate::CassandraResultMapper;

/// Topic entity and persistence.
#[derive(
    Clone, Debug, cdrs_tokio::IntoCdrsValue, cdrs_tokio::TryFromRow, cdrs_tokio::TryFromUdt,
)]
pub struct TopicEntity {
    /// Group all topics in single partition by using a common `topic_type`.
    topic_type: String,
    /// Sort by name to allow future batch retrieval of a very large number of topics.
    topic_id: String,
    /// Time of topic update in epoch microseconds
    last_update_ts: i64,
}

// Dev notes:
// Keyspace names can have up to 48 alpha-numeric characters and contain underscores
// 100MiB/row and 40 chars in topic name + extra >≃ 1 M topics.

impl TopicEntity {
    pub(crate) const CQL_TABLE_NAME: &'static str = "topic";

    const CQL_TEMPLATE_CREATE_TABLE: &'static str = "
        CREATE TABLE IF NOT EXISTS topic (
            topic_type      text,
            topic_id        text,
            last_update_ts  bigint,
            PRIMARY KEY ((topic_type), topic_id)
        ) WITH CLUSTERING ORDER BY (topic_id ASC)
        ;";

    /// QT1. Unconditional insert
    const CQL_TEMPLATE_INSERT_UNCONDITIONAL: &'static str = "
        INSERT INTO {{ keyspace }}.topic
        (topic_type, topic_id, last_update_ts)
        VALUES (?,?,?)
        ;";

    /// QT2. Get all entities with limit.
    const CQL_TEMPLATE_SELECT_ALL: &'static str = "
        SELECT topic_type, topic_id, last_update_ts
        FROM {{ keyspace }}.topic
        WHERE topic_type = ?
        LIMIT {{ limit }}
        ;";

    /// QT3. Get all entities with limit and topic_id is greater than.
    const CQL_TEMPLATE_SELECT_ALL_FROM: &'static str = "
        SELECT topic_type, topic_id, last_update_ts
        FROM {{ keyspace }}.topic
        WHERE topic_type = ? AND topic_id > ?
        LIMIT {{ limit }}
        ;";

    /// Keep all topics in a single ordered partition..
    const TOPIC_TYPE_DEFAULT: &'static str = "_topic";

    /// Return a new instance.
    pub fn new(topic_id: &str) -> Self {
        Self {
            topic_type: Self::TOPIC_TYPE_DEFAULT.to_owned(),
            topic_id: topic_id.to_owned(),
            last_update_ts: i64::from_unsigned(fragtale_client::time::get_timestamp_micros()),
        }
    }

    /// Return the topic identifier.
    pub fn get_topic_id(&self) -> &str {
        &self.topic_id
    }

    /// Create table and indices for this entity.
    pub async fn create_table_and_indices(db: &CassandraProvider) {
        db.create_table(
            &db.app_keyspace,
            Self::CQL_TABLE_NAME,
            Self::CQL_TEMPLATE_CREATE_TABLE,
        )
        .await;
    }

    /// Unconditional insert
    pub async fn insert(&self, db: &CassandraProvider, keyspace: &str) -> bool {
        db.query_with_keyspace_and_values(
            Self::CQL_TEMPLATE_INSERT_UNCONDITIONAL,
            keyspace,
            cdrs_tokio::query_values!(
                self.topic_type.to_owned(),
                self.topic_id.to_owned(),
                self.last_update_ts
            ),
        )
        .await
        .map(CassandraResultMapper::into_applied)
        .unwrap_or_else(|| {
            if log::log_enabled!(log::Level::Debug) {
                log::debug!("Failed insert of {self:?}");
            }
            false
        })
    }

    /// Retrieve all topic identifiers up to a max number of results.
    pub async fn select_all_topic_id(
        db: &CassandraProvider,
        keyspace: &str,
        max_results: usize,
    ) -> Vec<String> {
        let values = cdrs_tokio::query_values!(Self::TOPIC_TYPE_DEFAULT.to_owned());
        db.query_with_keyspace_and_values(
            &Self::CQL_TEMPLATE_SELECT_ALL.replacen("{{ limit }}", &max_results.to_string(), 1),
            keyspace,
            values,
        )
        .await
        .map(CassandraResultMapper::into_entities)
        .map(|entities| {
            entities
                .into_iter()
                .map(|entity: Self| entity.topic_id)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
    }

    /// Retrieve all topic identifiers up to a max number of results where
    /// topic_id is greater than the provided `from`.
    pub async fn select_all_topic_id_from(
        db: &CassandraProvider,
        keyspace: &str,
        from: &str,
        max_results: usize,
    ) -> Vec<String> {
        let values =
            cdrs_tokio::query_values!(Self::TOPIC_TYPE_DEFAULT.to_owned(), from.to_string());
        db.query_with_keyspace_and_values(
            &Self::CQL_TEMPLATE_SELECT_ALL_FROM.replacen(
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
                .map(|entity: Self| entity.topic_id)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
    }
}
