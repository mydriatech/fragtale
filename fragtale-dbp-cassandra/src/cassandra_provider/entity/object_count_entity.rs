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

//! Object count entity and persistence

use super::FromSignedOrDefault;
use super::FromUnsignedOrDefault;
use crate::CassandraProvider;
use crate::CassandraResultMapper;
use fragtale_dbp::mb::ObjectCount;
use fragtale_dbp::mb::ObjectCountType;

/**
Estimation of the number of recent objects for change detection.

This is a per-topic table since given a large #instances it would be hard
to return them all in a single query anyway. This also allows this table to be
dropped if the topic is dropped.

A TTL ensures that the table only keeps relevant data about alive instances.

The object count is subject to group commits to avoid a large write overhead
during high load.

This value may not be updated if an instance crashes.
For fault tolerance the client must wake up and do its thing at intervals anyway.

Bucketing:

* Cassandra rows should be kept under 100MiB
* We store 2+8 bytes per update (+overhead)
* We design for 1Mops/s.
* We will never update the counter more often than every millisecond using
  group commits. (Probably a lot more seldom.)

→ Bucketing roughly by the hour is sufficient. (36 MiB per row + overhead)
 */
impl From<&ObjectCountEntity> for ObjectCount {
    fn from(value: &ObjectCountEntity) -> Self {
        Self::new(value.instance_id, value.object_count)
    }
}

#[derive(
    Clone, Debug, cdrs_tokio::IntoCdrsValue, cdrs_tokio::TryFromRow, cdrs_tokio::TryFromUdt,
)]
pub struct ObjectCountEntity {
    /// todo
    object_type: String,
    /// todo
    object_count_bucket: i32,
    /// todo
    instance_id: i16,
    /// todo
    object_count: i64,
}

impl ObjectCountEntity {
    pub(crate) const CQL_TABLE_NAME: &'static str = "object_count";

    const CQL_TEMPLATE_CREATE_TABLE: &'static str = "
        CREATE TABLE IF NOT EXISTS object_count (
            object_type             text,
            object_count_bucket     int,
            instance_id             smallint,
            object_count            bigint,
            PRIMARY KEY ((object_type, object_count_bucket), instance_id)
        ) WITH CLUSTERING ORDER BY (instance_id ASC)
        ";

    /// QOC1: Upsert object count
    const CQL_TEMPLATE_INSERT: &'static str = "
        INSERT INTO object_count
        (object_type, object_count_bucket, instance_id, object_count)
        VALUES (?,?,?,?)
        USING TTL {{ ttl }}
        ";

    /// Get full entity
    const CQL_TEMPLATE_SELECT: &'static str = "
        SELECT object_type, object_count_bucket, instance_id, object_count
        FROM object_count
        WHERE object_type = ? AND object_count_bucket = ?
        ";

    // Create a new ObjectCountEntity
    pub fn new(
        object_type: &ObjectCountType,
        epoch_micros: u64,
        instance_id: u16,
        object_count: u64,
    ) -> Self {
        Self {
            object_type: object_type.name().to_owned(),
            object_count_bucket: Self::object_count_bucket_from_ts(epoch_micros),
            instance_id: i16::from_unsigned(instance_id),
            object_count: i64::try_from(object_count).unwrap_or(i64::MAX),
        }
    }

    /// The type of counted object.
    pub fn get_object_type(&self) -> String {
        self.object_type.to_owned()
    }

    /// The time shard of the counted object type.
    pub fn get_object_count_bucket(&self) -> u32 {
        u32::from_signed(self.object_count_bucket)
    }

    /// Return the instance identifier the count is for.
    pub fn get_instance_id(&self) -> u16 {
        u16::from_signed(self.instance_id)
    }

    /// Return the count.
    pub fn get_object_count(&self) -> u64 {
        u64::from_signed(self.object_count)
    }

    /// Bucket
    fn object_count_bucket_from_ts(epoch_micros: u64) -> i32 {
        // 2^32 ≃> 12 minutes of from an hour
        i32::try_from(epoch_micros >> 32).unwrap()
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

    /// Unconditional insert (with TTL of 600 seconds).
    pub async fn insert(&self, db: &CassandraProvider, topic_id: &str) -> bool {
        let time_to_live_seconds = 600;
        let keyspace = &db.get_keyspace_from_topic(topic_id);
        db.query_with_keyspace_and_values(
            &Self::CQL_TEMPLATE_INSERT.replacen("{{ ttl }}", &time_to_live_seconds.to_string(), 1),
            keyspace,
            cdrs_tokio::query_values!(
                self.object_type.to_owned(),
                self.object_count_bucket,
                self.instance_id,
                self.object_count
            ),
        )
        .await
        .map(CassandraResultMapper::into_applied)
        .unwrap_or(false)
    }

    /// Get all objects counts in the current bucket.
    pub async fn select_by_topic_id_and_object_type(
        db: &CassandraProvider,
        topic_id: &str,
        now_micros: u64,
        object_count_type: &ObjectCountType,
    ) -> Vec<Self> {
        let keyspace = &db.get_keyspace_from_topic(topic_id);
        let values = cdrs_tokio::query_values!(
            "object_type" => object_count_type.name().to_owned(),
            "object_count_bucket" => Self::object_count_bucket_from_ts(now_micros)
        );
        db.query_with_keyspace_and_values(Self::CQL_TEMPLATE_SELECT, keyspace, values)
            .await
            .map(CassandraResultMapper::into_entities)
            .unwrap_or_default()
    }
}
