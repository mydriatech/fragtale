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

//! Instance identity claim entity and persistence.

use super::FromSignedOrDefault;
use super::FromUnsignedOrDefault;
use crate::CassandraProvider;
use crate::CassandraResultMapper;

/// Instance identity claim entity and persistence.
///
/// A TTL on each claim is used to ensure that old and crashed nodes are
/// automatically excluded.
///
/// Instances that shut down gracefull should free (delete) their claim.
#[derive(
    Clone, Debug, cdrs_tokio::IntoCdrsValue, cdrs_tokio::TryFromRow, cdrs_tokio::TryFromUdt,
)]
pub struct IdentityClaimEntity {
    identity_type: String,
    identity_claim: i16,
    first_claim_ts: i64,
}

impl IdentityClaimEntity {
    pub(crate) const CQL_TABLE_NAME: &'static str = "identity_claim";

    const CQL_TEMPLATE_CREATE_TABLE: &'static str = "
        CREATE TABLE IF NOT EXISTS identity_claim (
            identity_type   text,
            identity_claim  smallint,
            first_claim_ts  bigint,
            PRIMARY KEY ((identity_type), identity_claim)
        ) WITH CLUSTERING ORDER BY (identity_claim ASC)
        ;";

    /// QIC1. Claim an identity for `ttl` seconds.
    const CQL_TEMPLATE_INSERT: &'static str = "
        INSERT INTO {{ keyspace }}.identity_claim
        (identity_type, identity_claim, first_claim_ts)
        VALUES (?,?,?)
        IF NOT EXISTS
        USING TTL {{ ttl }}
        ;";

    /// QIC2. Re-claim an identity for another `ttl` seconds.
    const CQL_TEMPLATE_INSERT_UNCONDITIONAL: &'static str = "
        INSERT INTO {{ keyspace }}.identity_claim
        (identity_type, identity_claim, first_claim_ts)
        VALUES (?,?,?)
        USING TTL {{ ttl }}
        ;";

    /// QIC3. Free an identity.
    const CQL_TEMPLATE_DELETE: &'static str = "
        DELETE
        FROM {{ keyspace }}.identity_claim
        WHERE identity_type = ? AND identity_claim = ?
        ;";

    /// QIC4. Retrieve a specific instance identity claim.
    const CQL_TEMPLATE_SELECT: &'static str = "
        SELECT identity_type, identity_claim, first_claim_ts
        FROM {{ keyspace }}.identity_claim
        WHERE identity_type = ? AND identity_claim = ?
        ;";

    /// QIC5. Retrieve all instance identity claim(s).
    const CQL_TEMPLATE_SELECT_ALL_CLAIMS: &'static str = "
        SELECT identity_type, identity_claim, first_claim_ts
        FROM {{ keyspace }}.identity_claim
        WHERE identity_type = ?
        LIMIT 1024
        ;";

    /// Default type.
    ///
    /// Using this a partition key groups all of the instance claims in a single
    /// partition.
    const ID_CLAIM_TYPE_INSTANCE: &'static str = "_instance";

    /// Return a new instance.
    pub fn new(identity_claim: u16, first_claim_ts_micros: u64) -> Self {
        Self {
            identity_type: Self::ID_CLAIM_TYPE_INSTANCE.to_owned(),
            identity_claim: i16::from_unsigned(identity_claim),
            first_claim_ts: i64::from_unsigned(first_claim_ts_micros),
        }
    }

    /// Get identity claim.
    pub fn get_identity_claim(&self) -> u16 {
        u16::from_signed(self.identity_claim)
    }

    /// Get the time of when the identity claim was first registered.
    pub fn get_first_claim_ts(&self) -> u64 {
        u64::from_signed(self.first_claim_ts)
    }

    /// Create the table and indices for this entity.
    pub async fn create_table_and_indices(db: &CassandraProvider) {
        db.create_table(
            &db.app_keyspace,
            Self::CQL_TABLE_NAME,
            Self::CQL_TEMPLATE_CREATE_TABLE,
        )
        .await;
    }

    /// Insert the entity unless it already exists.
    pub async fn insert_if_not_exists(
        &self,
        db: &CassandraProvider,
        keyspace: &str,
        time_to_live_seconds: u32,
    ) -> bool {
        db.query_with_keyspace_and_values(
            &Self::CQL_TEMPLATE_INSERT.replacen("{{ ttl }}", &time_to_live_seconds.to_string(), 1),
            keyspace,
            cdrs_tokio::query_values!(
                self.identity_type.to_owned(),
                self.identity_claim,
                self.first_claim_ts
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

    /// Insert the entity regardless of if this will overwrite a previous entity.
    pub async fn insert(
        &self,
        db: &CassandraProvider,
        keyspace: &str,
        time_to_live_seconds: u32,
    ) -> bool {
        db.query_with_keyspace_and_values(
            &Self::CQL_TEMPLATE_INSERT_UNCONDITIONAL.replacen(
                "{{ ttl }}",
                &time_to_live_seconds.to_string(),
                1,
            ),
            keyspace,
            cdrs_tokio::query_values!(
                self.identity_type.to_owned(),
                self.identity_claim,
                self.first_claim_ts
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

    /// Delete the entity.
    pub async fn delete(db: &CassandraProvider, keyspace: &str, identity_claim: u16) -> bool {
        let values = cdrs_tokio::query_values!(
            Self::ID_CLAIM_TYPE_INSTANCE.to_owned(),
            i16::from_unsigned(identity_claim)
        );
        db.query_with_keyspace_and_values(Self::CQL_TEMPLATE_DELETE, keyspace, values)
            .await
            .map(CassandraResultMapper::into_applied)
            .unwrap_or_else(|| {
                if log::log_enabled!(log::Level::Debug) {
                    log::debug!("Failed delete for identity_claim {identity_claim}.");
                }
                false
            })
    }

    /// Return the entity for a specific instance identity claim if it exists.
    pub async fn select(
        db: &CassandraProvider,
        keyspace: &str,
        identity_claim: u16,
    ) -> Option<Self> {
        let values = cdrs_tokio::query_values!(
            Self::ID_CLAIM_TYPE_INSTANCE.to_owned(),
            i16::from_unsigned(identity_claim)
        );
        db.query_with_keyspace_and_values(Self::CQL_TEMPLATE_SELECT, keyspace, values)
            .await
            .map(CassandraResultMapper::into_entities)
            .unwrap_or_default()
            .first()
            .cloned()
    }

    /// Return all instance identity claims.
    ///
    /// The TTL set on all claims will ensure that old and crashed nodes will
    /// stop showing up after TTL seconds.
    pub async fn select_all_identity_claim(db: &CassandraProvider, keyspace: &str) -> Vec<u16> {
        Self::select_all(db, keyspace)
            .await
            .iter()
            .map(Self::get_identity_claim)
            .collect::<Vec<_>>()
    }

    /// Return all instance identity claim entities.
    ///
    /// The TTL set on all claims will ensure that old and crashed nodes will
    /// stop showing up after TTL seconds.
    pub async fn select_all(db: &CassandraProvider, keyspace: &str) -> Vec<Self> {
        let values = cdrs_tokio::query_values!(Self::ID_CLAIM_TYPE_INSTANCE.to_owned());
        db.query_with_keyspace_and_values(Self::CQL_TEMPLATE_SELECT_ALL_CLAIMS, keyspace, values)
            .await
            .map(CassandraResultMapper::into_entities)
            .unwrap_or_default()
    }
}
