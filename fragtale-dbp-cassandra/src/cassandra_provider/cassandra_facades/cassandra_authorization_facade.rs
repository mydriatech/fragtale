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

//! Cassandra implementation of [AuthorizationFacade].

use crate::CassandraProvider;
use crate::cassandra_provider::entity::ResourceGrantEntity;
use fragtale_dbp::dbp::facades::AuthorizationFacade;
use std::sync::Arc;

/// Cassandra implementation of [AuthorizationFacade].
pub struct CassandraAuthorizationFacade {
    cassandra_provider: Arc<CassandraProvider>,
}

impl CassandraAuthorizationFacade {
    /// Return a new instance.
    pub fn new(cassandra_provider: &Arc<CassandraProvider>) -> Self {
        Self {
            cassandra_provider: Arc::clone(cassandra_provider),
        }
    }
}

#[async_trait::async_trait]
impl AuthorizationFacade for CassandraAuthorizationFacade {
    async fn is_authorized_to_resource(&self, identity: &str, resource: &str) -> bool {
        ResourceGrantEntity::select(
            &self.cassandra_provider,
            &self.cassandra_provider.app_keyspace,
            resource,
            identity,
        )
        .await
        .is_some()
    }

    async fn is_any_authorized_to_resource(&self, resource: &str) -> bool {
        !ResourceGrantEntity::select_by_resource(
            &self.cassandra_provider,
            &self.cassandra_provider.app_keyspace,
            resource,
            1,
        )
        .await
        .is_empty()
    }

    async fn grant_access_to_resource_for(
        &self,
        identity: &str,
        resource: &str,
        expires: Option<u64>,
    ) -> bool {
        let ttl_seconds = expires.map(|expires_micros| {
            let now = fragtale_client::time::get_timestamp_micros();
            if now < expires_micros {
                0
            } else {
                // Round off to nearest second.
                (expires_micros - now + 500_000) / 1_000_000
            }
        });
        ResourceGrantEntity::new(resource, identity)
            .insert(
                &self.cassandra_provider,
                &self.cassandra_provider.app_keyspace,
                ttl_seconds,
            )
            .await
    }

    async fn deny_access_to_resource_for(
        &self,
        identity: &str,
        resource: &str,
        _expires: Option<u64>,
    ) -> bool {
        ResourceGrantEntity::delete(
            &self.cassandra_provider,
            &self.cassandra_provider.app_keyspace,
            resource,
            identity,
        )
        .await
    }
}
