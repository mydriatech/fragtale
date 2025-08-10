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

//! Resource authorization grant entity and persistence.

//use super::FromUnsignedOrDefault;
use crate::CassandraProvider;
use crate::CassandraResultMapper;

/// Resource authorization grant entity and persistence.
#[derive(
    Clone, Debug, cdrs_tokio::IntoCdrsValue, cdrs_tokio::TryFromRow, cdrs_tokio::TryFromUdt,
)]
pub struct ResourceGrantEntity {
    /// The resource where authorization is granted. E.g. "/type/object_id/operation"
    resource: String,
    /// The identity that is granted access in serialized form.
    identity: String,
}

impl ResourceGrantEntity {
    pub(crate) const CQL_TABLE_NAME: &'static str = "resource_grant";

    const CQL_TEMPLATE_CREATE_TABLE: &'static str = "
        CREATE TABLE IF NOT EXISTS resource_grant (
            resource        text,
            identity        text,
            PRIMARY KEY ((resource), identity)
        ) WITH CLUSTERING ORDER BY (identity ASC);
        ;";

    /// QRG1. Unconditional insert
    const CQL_TEMPLATE_INSERT_UNCONDITIONAL: &'static str = "
        INSERT INTO {{ keyspace }}.resource_grant
        (resource, identity)
        VALUES (?,?)
        ;";

    /// QRG2. Get entity.
    const CQL_TEMPLATE_SELECT: &'static str = "
        SELECT resource, identity
        FROM {{ keyspace }}.resource_grant
        WHERE resource = ? AND identity = ?
        ;";

    /// QRG3. Get all entities for a resource.
    const CQL_TEMPLATE_SELECT_BY_RESOURCE: &'static str = "
        SELECT resource, identity
        FROM {{ keyspace }}.resource_grant
        WHERE resource = ?
        LIMIT {{ limit }}
        ;";

    /// QRG4. Delete/tombstone entity.
    const CQL_TEMPLATE_DELETE: &'static str = "
        DELETE
        FROM {{ keyspace }}.resource_grant
        WHERE resource = ? AND identity = ?
        ;";

    /// Return a new instance.
    pub fn new(resource: &str, identity: &str) -> Self {
        Self {
            resource: resource.to_owned(),
            identity: identity.to_owned(),
            //expires_ts: expires_ts.map(i64::from_unsigned), , ttl: Option<u64>
        }
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

    /// Unconditional insert with optional time to live in seconds.
    pub async fn insert(
        &self,
        db: &CassandraProvider,
        keyspace: &str,
        ttl_seconds: Option<u64>,
    ) -> bool {
        let query = if let Some(ttl) = ttl_seconds {
            &format!(
                "{} USING TTL {ttl}",
                Self::CQL_TEMPLATE_INSERT_UNCONDITIONAL
            )
        } else {
            Self::CQL_TEMPLATE_INSERT_UNCONDITIONAL
        };
        db.query_with_keyspace_and_values(
            query,
            keyspace,
            cdrs_tokio::query_values!(self.resource.to_owned(), self.identity.to_owned()),
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

    /// Return the entity for a specific resource and identity if it exists.
    pub async fn select(
        db: &CassandraProvider,
        keyspace: &str,
        resource: &str,
        identity: &str,
    ) -> Option<Self> {
        let values = cdrs_tokio::query_values!(resource.to_owned(), identity.to_owned());
        db.query_with_keyspace_and_values(Self::CQL_TEMPLATE_SELECT, keyspace, values)
            .await
            .map(CassandraResultMapper::into_entities)
            .unwrap_or_default()
            .first()
            .cloned()
    }

    /// Return the entity for a specific resource and identity if it exists.
    pub async fn select_by_resource(
        db: &CassandraProvider,
        keyspace: &str,
        resource: &str,
        max_results: usize,
    ) -> Vec<Self> {
        let values = cdrs_tokio::query_values!(resource.to_owned());
        db.query_with_keyspace_and_values(
            &Self::CQL_TEMPLATE_SELECT_BY_RESOURCE.replacen(
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

    /// Delete the entity for a specific resource and identity.
    pub async fn delete(
        db: &CassandraProvider,
        keyspace: &str,
        resource: &str,
        identity: &str,
    ) -> bool {
        let values = cdrs_tokio::query_values!(resource.to_owned(), identity.to_owned());
        db.query_with_keyspace_and_values(Self::CQL_TEMPLATE_DELETE, keyspace, values)
            .await
            .map(CassandraResultMapper::into_applied)
            .unwrap_or_default()
    }
}
