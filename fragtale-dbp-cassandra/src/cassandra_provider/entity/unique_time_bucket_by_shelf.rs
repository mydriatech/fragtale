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

//! UniqueTime bucket event by shelf entity and persistence

use super::FromSignedOrDefault;
use super::FromUnsignedOrDefault;
use crate::CassandraProvider;
use crate::CassandraResultMapper;
use fragtale_dbp::mb::UniqueTime;

/// UniqueTime bucket event by shelf entity and persistence
#[derive(
    Clone, Debug, cdrs_tokio::IntoCdrsValue, cdrs_tokio::TryFromRow, cdrs_tokio::TryFromUdt,
)]
pub struct UniqueTimeBucketByShelfEntity {
    shelf: i16,
    bucket: i64,
}

impl UniqueTimeBucketByShelfEntity {
    pub(crate) const CQL_TABLE_NAME: &'static str = "unique_time_bucket_by_shelf";

    const CQL_TEMPLATE_CREATE_TABLE: &'static str = "
        CREATE TABLE IF NOT EXISTS unique_time_bucket_by_shelf (
            shelf       smallint,
            bucket      bigint,
            PRIMARY KEY ((shelf), bucket)
        ) WITH CLUSTERING ORDER BY (bucket ASC)
        ;";

    /// QUTB1. Add priority timestamp bucket, unless it exists.
    const CQL_TEMPLATE_INSERT: &'static str = "
        INSERT INTO unique_time_bucket_by_shelf
        (shelf, bucket)
        VALUES (?,?)
        ;";

    /// QUTB2. Get next used bucket.
    const CQL_TEMPLATE_SELECT_BY_SHELF_AND_BUCKET: &'static str = "
        SELECT shelf, bucket
        FROM unique_time_bucket_by_shelf
        WHERE shelf = ? AND bucket > ?
        LIMIT {{ limit }}
        ";

    /// QUTB3. Get specific entity..
    const CQL_TEMPLATE_SELECT_BY_SHELF_AND_BUCKET_EXACT: &'static str = "
        SELECT shelf, bucket
        FROM unique_time_bucket_by_shelf
        WHERE shelf = ? AND bucket = ?
        LIMIT 1
        ";

    /// Return a new instance.
    pub fn new(unique_time: UniqueTime) -> Self {
        Self {
            shelf: unique_time.get_shelf_i16(),
            bucket: unique_time.get_bucket_i64(),
        }
    }

    /// Get the event "shelf" time shard.
    pub fn get_shelf(&self) -> u16 {
        u16::from_signed(self.shelf)
    }

    /// Get the event "bucket" time shard.
    pub fn get_bucket(&self) -> u64 {
        u64::from_signed(self.bucket)
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
            cdrs_tokio::query_values!(self.shelf, self.bucket),
        )
        .await
        .map(CassandraResultMapper::into_applied)
        .unwrap_or(false)
    }

    /// Get the next entity (to get the bucket) for a shelf.
    pub async fn select_next_by_shelf_and_bucket(
        db: &CassandraProvider,
        topic_id: &str,
        current_shelf: u16,
        current_bucket: u64,
        max_results: usize,
    ) -> Vec<Self> {
        let keyspace = &db.get_keyspace_from_topic(topic_id);
        let values = cdrs_tokio::query_values!(
            i16::from_unsigned(current_shelf),
            i64::from_unsigned(current_bucket)
        );
        db.query_with_keyspace_and_values(
            &Self::CQL_TEMPLATE_SELECT_BY_SHELF_AND_BUCKET.replacen(
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

    /// Retrive a specific entity.
    pub async fn select_by_shelf_and_bucket_exact(
        db: &CassandraProvider,
        topic_id: &str,
        unique_time: UniqueTime,
    ) -> Option<Self> {
        let keyspace = &db.get_keyspace_from_topic(topic_id);
        let values =
            cdrs_tokio::query_values!(unique_time.get_shelf_i16(), unique_time.get_bucket_i64());
        db.query_with_keyspace_and_values(
            Self::CQL_TEMPLATE_SELECT_BY_SHELF_AND_BUCKET_EXACT,
            keyspace,
            values,
        )
        .await
        .map(CassandraResultMapper::into_entities)
        .and_then(|entities| entities.first().cloned())
    }
}
