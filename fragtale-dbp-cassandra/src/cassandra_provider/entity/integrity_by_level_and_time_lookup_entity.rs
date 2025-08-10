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

//! Integrity protection lookup iteration helper entity and persistence

use super::FromSignedOrDefault;
use super::FromUnsignedOrDefault;
use crate::CassandraProvider;
use crate::CassandraResultMapper;

/** Integrity protection lookup iteration helper entity and persistence.

This entity provides ordered information about which "lookup buckets" that are
populated" in [super::IntegrityByLevelAndTimeEntity] to enable efficient
iterations.
*/
#[derive(
    Clone, Debug, cdrs_tokio::IntoCdrsValue, cdrs_tokio::TryFromRow, cdrs_tokio::TryFromUdt,
)]
pub struct IntegrityByLevelAndTimeLookupEntity {
    /// Tree level in protection hierachy
    level: i8,
    /// Time based bucket used in primary key of [super::IntegrityByLevelAndTimeEntity]
    lookup_ts_bucket: i64,
}

impl IntegrityByLevelAndTimeLookupEntity {
    pub(crate) const CQL_TABLE_NAME: &'static str = "integrity_blat_lookup";

    const CQL_TEMPLATE_CREATE_TABLE: &'static str = "
        CREATE TABLE IF NOT EXISTS integrity_blat_lookup (
            level                   tinyint,
            lookup_ts_bucket        bigint,
            PRIMARY KEY ((level), lookup_ts_bucket)
        ) WITH CLUSTERING ORDER BY (lookup_ts_bucket ASC);
        ";

    /// QIBLAL1: Insert new
    const CQL_TEMPLATE_INSERT: &'static str = "
        INSERT INTO integrity_blat_lookup
        (level, lookup_ts_bucket)
        VALUES (?,?)
        ";

    /// QIBLAL3: Grab latest `lookup_ts_bucket`.
    const CQL_TEMPLATE_SELECT_LATEST_BUCKET: &'static str = "
        SELECT level, lookup_ts_bucket
        FROM integrity_blat_lookup
        WHERE level=?
        ORDER BY lookup_ts_bucket DESC
        LIMIT 1
        ";

    /// QIBLAL3b: Grab first `lookup_ts_bucket`.
    const CQL_TEMPLATE_SELECT_FIRST_BUCKET: &'static str = "
        SELECT level, lookup_ts_bucket
        FROM integrity_blat_lookup
        WHERE level=?
        ORDER BY lookup_ts_bucket ASC
        LIMIT 1
        ";

    /// QIBLAL4: Get next bucket that is larger than the current
    const CQL_TEMPLATE_SELECT_NEXT_BUCKET: &'static str = "
        SELECT level, lookup_ts_bucket
        FROM integrity_blat_lookup
        WHERE level=? AND lookup_ts_bucket>?
        ORDER BY lookup_ts_bucket ASC
        LIMIT 1
        ";

    /// Initialize a new entity.
    pub fn new(level: u8, lookup_ts_bucket: u64) -> Self {
        Self {
            level: i8::from_unsigned(level),
            lookup_ts_bucket: i64::from_unsigned(lookup_ts_bucket),
        }
    }

    /// Get the level of this entity in the integrity protection hierarchy.
    pub fn get_level(&self) -> u8 {
        u8::from_signed(self.level)
    }

    /// Get the bucket (time shard) if this entity.
    pub fn get_lookup_ts_bucket(&self) -> u64 {
        u64::from_signed(self.lookup_ts_bucket)
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

    /// Unconditional insert.
    pub async fn insert(&self, db: &CassandraProvider, topic_id: &str) -> bool {
        db.query_with_keyspace_and_values(
            Self::CQL_TEMPLATE_INSERT,
            &db.get_keyspace_from_topic(topic_id),
            cdrs_tokio::query_values!(self.level, self.lookup_ts_bucket),
        )
        .await
        .map(CassandraResultMapper::into_applied)
        .unwrap_or(false)
    }

    /// Retrieve the latest by level in the integrity protection hierarchy.
    pub async fn select_latest_by_level(
        db: &CassandraProvider,
        topic_id: &str,
        level: u8,
    ) -> Option<Self> {
        let keyspace = &db.get_keyspace_from_topic(topic_id);
        let values = cdrs_tokio::query_values!(
            "level" => i8::from_unsigned(level)
        );
        db.query_with_keyspace_and_values(Self::CQL_TEMPLATE_SELECT_LATEST_BUCKET, keyspace, values)
            .await
            .map(CassandraResultMapper::into_entities)
            .and_then(|entities| entities.first().cloned())
    }

    /// Retrieve the first by level in the integrity protection hierarchy.
    pub async fn select_first_by_level(
        db: &CassandraProvider,
        topic_id: &str,
        level: u8,
    ) -> Option<Self> {
        let keyspace = &db.get_keyspace_from_topic(topic_id);
        let values = cdrs_tokio::query_values!(
            "level" => i8::from_unsigned(level)
        );
        db.query_with_keyspace_and_values(Self::CQL_TEMPLATE_SELECT_FIRST_BUCKET, keyspace, values)
            .await
            .map(CassandraResultMapper::into_entities)
            .and_then(|entities| entities.first().cloned())
    }

    /// Retrieve the next entity by level in the integrity protection hierarchy.
    pub async fn select_next_by_level(
        db: &CassandraProvider,
        topic_id: &str,
        level: u8,
        from_lookup_ts_bucket: u64,
    ) -> Option<Self> {
        let keyspace = &db.get_keyspace_from_topic(topic_id);
        let values = cdrs_tokio::query_values!(
            "level" => i8::from_unsigned(level),
            "lookup_ts_bucket" => i64::from_unsigned(from_lookup_ts_bucket)
        );
        db.query_with_keyspace_and_values(Self::CQL_TEMPLATE_SELECT_NEXT_BUCKET, keyspace, values)
            .await
            .map(CassandraResultMapper::into_entities)
            .and_then(|entities| entities.first().cloned())
    }
}
