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

//! Integrity protection lookup entity and persistence

use super::FromSignedOrDefault;
use super::FromUnsignedOrDefault;
use super::IntegrityEntity;
use crate::CassandraProvider;
use crate::CassandraResultMapper;

/** Integrity protection lookup entity and persistence.

This entity enables efficient iterations over [super::IntegrityEntity] for each
level in the protection hierarchy.

The repesented lookup table is sharded into "lookup buckets" to match the design
of the integrity protection heirarchy where each new level aggregates an
interval at the lower level.

[super::IntegrityByLevelAndTimeLookupEntity] provides ordered information about
which "lookup buckets" that are populated" to aid with the iteration.
*/
#[derive(
    Clone, Debug, cdrs_tokio::IntoCdrsValue, cdrs_tokio::TryFromRow, cdrs_tokio::TryFromUdt,
)]
pub struct IntegrityByLevelAndTimeEntity {
    /// Tree level in protection hierachy
    level: i8,
    /// Time based bucket used in primary key
    lookup_ts_bucket: i64,
    /// Time of creation
    protection_ts: i64,
    /// Time based bucket used in primary key
    protection_ts_bucket: i64,
    /// (Practically) unique identifier
    protection_id: String,
}

impl IntegrityByLevelAndTimeEntity {
    pub(crate) const CQL_TABLE_NAME: &'static str = "integrity_by_level_and_time";

    const CQL_TEMPLATE_CREATE_TABLE: &'static str = "
        CREATE TABLE IF NOT EXISTS integrity_by_level_and_time (
            level                   tinyint,
            lookup_ts_bucket        bigint,
            protection_ts           bigint,
            protection_ts_bucket    bigint,
            protection_id           text,
            PRIMARY KEY ((level, lookup_ts_bucket), protection_ts)
        ) WITH CLUSTERING ORDER BY (protection_ts ASC);
        ";

    /// QIBLAT1: Insert new
    const CQL_TEMPLATE_INSERT: &'static str = "
        INSERT INTO integrity_by_level_and_time
        (level, lookup_ts_bucket, protection_ts, protection_ts_bucket, protection_id)
        VALUES (?,?,?,?,?)
        ";

    /// QIBLAT2: Get full entity
    const CQL_TEMPLATE_SELECT: &'static str = "
        SELECT level, lookup_ts_bucket, protection_ts, protection_ts_bucket, protection_id
        FROM integrity_by_level_and_time
        WHERE level=? AND lookup_ts_bucket=? AND protection_ts=?
        ";

    /// QIBLAT3: Grab latest completed `topic.integrity` PK by level and `lookup_ts_bucket`.
    const CQL_TEMPLATE_SELECT_LATEST_IN_BUCKET: &'static str = "
        SELECT level, lookup_ts_bucket, protection_ts, protection_ts_bucket, protection_id
        FROM integrity_by_level_and_time
        WHERE level=? AND lookup_ts_bucket=?
        ORDER BY protection_ts DESC
        LIMIT 1
        ";

    /// QIBLAT4: Get the `limit` next items from `protection_ts` in the bucket
    const CQL_TEMPLATE_SELECT_NEXT: &'static str = "
        SELECT level, lookup_ts_bucket, protection_ts, protection_ts_bucket, protection_id
        FROM integrity_by_level_and_time
        WHERE level=? AND lookup_ts_bucket=? AND protection_ts>=?
        ORDER BY protection_ts ASC
        LIMIT {{ limit }}
        ";

    /// Bucket protections based on level in protection hierarchy.
    ///
    /// Example:
    ///   Events at level 0 get their protection_ts_micros rounded of to
    ///   a 4-min interval that is shared by others.
    ///
    ///   The consolidation service will select buckets on level 0 to
    ///   produce a protection on level 1.
    pub fn to_lookup_ts_bucket(level: u8, protection_ts_micros: u64) -> u64 {
        let interval_micros = match level {
            // Level 0: Bucket into 4 minute intevals
            0 => 1_000_000 * 240,
            // Level 1: Bucket into 7 day intevals
            1 => 1_000_000 * 3_600 * 24 * 7,
            // Level 2: Bucket into 365 day intevals
            2 => 1_000_000 * 3_600 * 24 * 365,
            unsupported_level => {
                panic!("Bucketing for level {unsupported_level} is not implemented.")
            }
        };
        protection_ts_micros - protection_ts_micros % interval_micros
    }

    /// Initialize a new entity.
    pub fn new(
        level: u8,
        lookup_ts_bucket: u64,
        protection_ts: u64,
        protection_id: String,
    ) -> Self {
        let protection_ts_bucket = IntegrityEntity::to_protection_ts_bucket(protection_ts);
        Self {
            level: i8::from_unsigned(level),
            lookup_ts_bucket: i64::from_unsigned(lookup_ts_bucket),
            protection_ts: i64::from_unsigned(protection_ts),
            protection_ts_bucket: i64::from_unsigned(protection_ts_bucket),
            protection_id: protection_id.to_owned(),
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

    /// Get the integrity protection ts this entity points to.
    pub fn get_protection_ts(&self) -> u64 {
        u64::from_signed(self.protection_ts)
    }

    /// Get the integrity protection ts bucket this entity points to.
    pub fn get_protection_ts_bucket(&self) -> u64 {
        u64::from_signed(self.protection_ts_bucket)
    }

    /// Get the integrity protection identifier this entity points to.
    pub fn get_protection_id(&self) -> &str {
        &self.protection_id
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
            cdrs_tokio::query_values!(
                self.level,
                self.lookup_ts_bucket,
                self.protection_ts,
                self.protection_ts_bucket,
                self.protection_id.to_owned()
            ),
        )
        .await
        .map(CassandraResultMapper::into_applied)
        .unwrap_or(false)
    }

    /// Retrive entity by protection hierarchy level and time of integrity
    /// protection.
    pub async fn select_by_level_and_ts(
        db: &CassandraProvider,
        topic_id: &str,
        level: u8,
        protection_ts_micros: u64,
    ) -> Option<Self> {
        let lookup_ts_bucket = Self::to_lookup_ts_bucket(level, protection_ts_micros);
        let keyspace = &db.get_keyspace_from_topic(topic_id);
        let values = cdrs_tokio::query_values!(
            "level" => i8::from_unsigned(level),
            "lookup_ts_bucket" => i64::from_unsigned(lookup_ts_bucket),
            "protection_ts" => i64::from_unsigned(protection_ts_micros)
        );
        db.query_with_keyspace_and_values(Self::CQL_TEMPLATE_SELECT, keyspace, values)
            .await
            .map(CassandraResultMapper::into_entities)
            .and_then(|entities| entities.first().cloned())
    }

    /// Retrive entity by protection hierarchy level and time bucket of
    /// integrity protection.
    pub async fn select_latest_by_level_and_ts_bucket(
        db: &CassandraProvider,
        topic_id: &str,
        level: u8,
        protection_ts_micros: u64,
    ) -> Option<Self> {
        let lookup_ts_bucket = Self::to_lookup_ts_bucket(level, protection_ts_micros);
        let keyspace = &db.get_keyspace_from_topic(topic_id);
        let values = cdrs_tokio::query_values!(
            "level" => i8::from_unsigned(level),
            "lookup_ts_bucket" => i64::from_unsigned(lookup_ts_bucket)
        );
        db.query_with_keyspace_and_values(
            Self::CQL_TEMPLATE_SELECT_LATEST_IN_BUCKET,
            keyspace,
            values,
        )
        .await
        .map(CassandraResultMapper::into_entities)
        .and_then(|entities| entities.first().cloned())
    }

    /// Retrive the next entity by protection hierarchy level and time of
    /// integrity protection.
    pub async fn select_next_by_level_and_ts(
        db: &CassandraProvider,
        topic_id: &str,
        level: u8,
        from_protection_ts_micros: u64,
        limit: usize,
    ) -> Vec<Self> {
        let lookup_ts_bucket = Self::to_lookup_ts_bucket(level, from_protection_ts_micros);
        let keyspace = &db.get_keyspace_from_topic(topic_id);
        let values = cdrs_tokio::query_values!(
            "level" => i8::from_unsigned(level),
            "lookup_ts_bucket" => i64::from_unsigned(lookup_ts_bucket),
            "protection_ts" => i64::from_unsigned(from_protection_ts_micros)
        );
        let query_template =
            Self::CQL_TEMPLATE_SELECT_NEXT.replace("{{ limit }}", &limit.to_string());
        db.query_with_keyspace_and_values(&query_template, keyspace, values)
            .await
            .map(CassandraResultMapper::into_entities)
            .unwrap_or_default()
    }
}
