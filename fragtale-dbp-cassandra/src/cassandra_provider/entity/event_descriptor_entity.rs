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

//! Topic Schema entity and persistence

//use super::FromSignedOrDefault;
use super::FromUnsignedOrDefault;
use crate::CassandraProvider;
use crate::CassandraResultMapper;

// DEV NOTE: Indexing `schema_id` could allow schema retrieval when a consumer
// has not seen it before.
#[derive(
    Clone, Debug, cdrs_tokio::IntoCdrsValue, cdrs_tokio::TryFromRow, cdrs_tokio::TryFromUdt,
)]
pub struct EventDescriptorEntity {
    topic_id: String,
    version: i64,
    version_min: Option<i64>,
    schema_id: Option<String>,
    event_descriptor: String,
}

impl EventDescriptorEntity {
    pub(crate) const CQL_TABLE_NAME: &'static str = "event_descriptor";
    const CQL_COLUMN_NAME_SCHEMA_ID: &'static str = "schema_id";
    const CQL_INDEX_NAME_SCHEMA_ID: &'static str = "event_descriptor_by_schema_id_idx";

    const CQL_TEMPLATE_CREATE_TABLE: &'static str = "
        CREATE TABLE IF NOT EXISTS event_descriptor (
            topic_id            text,
            version             bigint,
            version_min         bigint,
            schema_id           text,
            event_descriptor    text,
            PRIMARY KEY ((topic_id), version)
        ) WITH CLUSTERING ORDER BY (version DESC);
        ";

    /// QTS2. Append schema update
    const CQL_TEMPLATE_INSERT_IF_NOT_EXISTS: &'static str = "
        INSERT INTO event_descriptor
        (topic_id, version, version_min, schema_id, event_descriptor)
        VALUES (?,?,?,?,?)
        IF NOT EXISTS
        ;";

    /// QTS1. Get event descriptors
    /// Get full entity
    const CQL_TEMPLATE_SELECT: &'static str = "
        SELECT topic_id, version, version_min, schema_id, event_descriptor
        FROM event_descriptor
        WHERE topic_id = ? AND version >= ?
        ";

    /// Return a new instance.
    pub fn new(
        topic_id: &str,
        version: u64,
        version_min: Option<u64>,
        schema_id: &Option<String>,
        event_descriptor: &str,
    ) -> Self {
        Self {
            topic_id: topic_id.to_owned(),
            version: i64::from_unsigned(version),
            version_min: version_min.map(i64::from_unsigned),
            schema_id: schema_id.to_owned(),
            event_descriptor: event_descriptor.to_owned(),
        }
    }

    /// Return the serialized event descriptor.
    pub fn get_event_descriptor(&self) -> &str {
        &self.event_descriptor
    }

    /// Create table and indices for this entity.
    pub async fn create_table_and_indices(db: &CassandraProvider) {
        db.create_table(
            &db.app_keyspace,
            Self::CQL_TABLE_NAME,
            Self::CQL_TEMPLATE_CREATE_TABLE,
        )
        .await;
        db.add_index(
            &db.app_keyspace,
            Self::CQL_TABLE_NAME,
            Self::CQL_COLUMN_NAME_SCHEMA_ID,
            Self::CQL_INDEX_NAME_SCHEMA_ID,
        )
        .await;
    }

    /// Conditional insert.
    pub async fn insert_if_not_exists(&self, db: &CassandraProvider, keyspace: &str) -> bool {
        db.query_with_keyspace_and_values(
            Self::CQL_TEMPLATE_INSERT_IF_NOT_EXISTS,
            keyspace,
            cdrs_tokio::query_values!(
                self.topic_id.to_owned(),
                self.version,
                self.version_min,
                self.schema_id.to_owned(),
                self.event_descriptor.to_owned()
            ),
        )
        .await
        .map(CassandraResultMapper::into_applied)
        .unwrap_or(false)
    }

    /// Return all entities that have the minimum version or greater for a
    /// topic.
    pub async fn select_by_topic_id(
        db: &CassandraProvider,
        keyspace: &str,
        topic_id: &str,
        min_descriptor_version: Option<u64>,
    ) -> Vec<Self> {
        let values = cdrs_tokio::query_values!(
            "topic_id" => topic_id.to_owned(),
            "version" => min_descriptor_version.map(i64::from_unsigned).unwrap_or(0)
        );
        db.query_with_keyspace_and_values(Self::CQL_TEMPLATE_SELECT, keyspace, values)
            .await
            .map(CassandraResultMapper::into_entities)
            .unwrap_or_default()
    }
}
