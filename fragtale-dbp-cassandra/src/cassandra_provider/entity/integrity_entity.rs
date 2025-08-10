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

//! Integrity potection entity and persistence.

use super::FromSignedOrDefault;
use super::FromUnsignedOrDefault;
use crate::CassandraProvider;
use crate::CassandraResultMapper;

/** Integrity potection entity and persistence.

Data is shared based on the time of protection and at the lowest level (0),
this is bucketed by 4 minutes.
Even if the interval at level 1 and 2 is much larger, there is little point in
using more complex sharding fo these intervals considering that the
`protection_data` might be rewritten when shared secrets are rolled over.

Iteration over this table is enabled using separate lookup tables represented by
[super::IntegrityByLevelAndTimeEntity] and
[super::IntegrityByLevelAndTimeLookupEntity].
 */
#[derive(
    Clone, Debug, cdrs_tokio::IntoCdrsValue, cdrs_tokio::TryFromRow, cdrs_tokio::TryFromUdt,
)]
pub struct IntegrityEntity {
    /// Time based bucket used in primary key
    protection_ts_bucket: i64,
    /// (Practically) unique identifier
    protection_id: String,
    /// Time of creation
    protection_ts: i64,
    /// Data protection payload
    protection_data: String,
    /// Row-external protection
    protection_ref: Option<String>,
}

impl IntegrityEntity {
    pub(crate) const CQL_TABLE_NAME: &'static str = "integrity";

    const CQL_TEMPLATE_CREATE_TABLE: &'static str = "
        CREATE TABLE IF NOT EXISTS integrity (
            protection_ts_bucket    bigint,
            protection_id           text,
            protection_ts           bigint,
            protection_data         text,
            protection_ref          text,
            PRIMARY KEY (protection_ts_bucket, protection_id)
        );
        ";

    /// QI1: Insert new
    const CQL_TEMPLATE_INSERT: &'static str = "
        INSERT INTO integrity
        (protection_ts_bucket, protection_id, protection_ts, protection_data, protection_ref)
        VALUES (?,?,?,?,?)
        ";

    /// QI2: Get full entity(/entities)
    const CQL_TEMPLATE_SELECT: &'static str = "
        SELECT protection_ts_bucket, protection_id, protection_ts, protection_data, protection_ref
        FROM integrity
        WHERE protection_ts_bucket=? AND protection_id=?
        ";

    /// QI3: Unconditional insert/update of `protection_ref`
    const CQL_TEMPLATE_UPSERT: &'static str = "
        INSERT INTO integrity
        (protection_ts_bucket, protection_id, protection_ref)
        VALUES (?,?,?)
        ";

    /// Bucket into 4 minute intevals
    pub fn to_protection_ts_bucket(protection_ts_micros: u64) -> u64 {
        protection_ts_micros - protection_ts_micros % 240_000_000
    }

    /// Initialize a new entity.
    pub fn new(protection_ts_micros: u64, protection_id: String, protection_data: String) -> Self {
        Self {
            protection_ts_bucket: i64::from_unsigned(Self::to_protection_ts_bucket(
                protection_ts_micros,
            )),
            protection_id,
            protection_ts: i64::from_unsigned(protection_ts_micros),
            protection_data,
            protection_ref: None,
        }
    }

    /// Time based bucket used in primary key
    pub fn get_protection_ts_bucket(&self) -> u64 {
        u64::from_signed(self.protection_ts_bucket)
    }

    /// Get protection identifier.
    pub fn get_protection_id(&self) -> &str {
        &self.protection_id
    }

    /// Get time of protection in microseconds.
    pub fn get_protection_ts(&self) -> u64 {
        u64::from_signed(self.protection_ts)
    }

    /// Get serialized integrity protection.
    pub fn get_protection_data(&self) -> &str {
        &self.protection_data
    }

    /// Get serialized reference to higher level in the hierarchy of integrity
    /// protection.
    pub fn get_protection_ref(&self) -> &Option<String> {
        &self.protection_ref
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
                self.protection_ts_bucket,
                self.protection_id.to_owned(),
                self.protection_ts,
                self.protection_data.to_owned(),
                self.protection_ref.to_owned()
            ),
        )
        .await
        .map(CassandraResultMapper::into_applied)
        .unwrap_or(false)
    }

    /// Update serilaized integrity protection reference.
    pub async fn upsert_protection_ref(
        db: &CassandraProvider,
        topic_id: &str,
        protection_ts_micros: u64,
        protection_id: &str,
        protection_ref: &str,
    ) -> bool {
        db.query_with_keyspace_and_values(
            Self::CQL_TEMPLATE_UPSERT,
            &db.get_keyspace_from_topic(topic_id),
            cdrs_tokio::query_values!(
                i64::from_unsigned(Self::to_protection_ts_bucket(protection_ts_micros,)),
                protection_id.to_owned(),
                protection_ref.to_owned()
            ),
        )
        .await
        .map(CassandraResultMapper::into_applied)
        .unwrap_or(false)
    }

    /// Retrieve a specific integrity protection entity.
    pub async fn select_by_protection_id(
        db: &CassandraProvider,
        topic_id: &str,
        protection_ts_micros: u64,
        protection_id: &str,
    ) -> Option<Self> {
        let keyspace = &db.get_keyspace_from_topic(topic_id);
        let values = cdrs_tokio::query_values!(
            "protection_ts_bucket" => i64::from_unsigned(Self::to_protection_ts_bucket(protection_ts_micros)),
            "protection_id" => protection_id.to_owned()
        );
        db.query_with_keyspace_and_values(Self::CQL_TEMPLATE_SELECT, keyspace, values)
            .await
            .map(CassandraResultMapper::into_entities)
            .and_then(|entities| entities.first().cloned())
    }
}
