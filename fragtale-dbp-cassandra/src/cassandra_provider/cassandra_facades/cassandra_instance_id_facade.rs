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

//! Cassandra implementation of [InstanceIdFacade].

use crate::CassandraProvider;
use crate::cassandra_provider::entity::IdentityClaimEntity;
use fragtale_dbp::dbp::facades::InstanceIdFacade;
use fragtale_dbp::mb::UniqueTime;
use std::sync::Arc;

/// Cassandra implementation of [InstanceIdFacade].
pub struct CassandraInstanceIdFacade {
    cassandra_provider: Arc<CassandraProvider>,
}

impl CassandraInstanceIdFacade {
    /// Return a new insatace.
    pub fn new(cassandra_provider: &Arc<CassandraProvider>) -> Self {
        Self {
            cassandra_provider: Arc::clone(cassandra_provider),
        }
    }
}

#[async_trait::async_trait]
impl InstanceIdFacade for CassandraInstanceIdFacade {
    async fn claim(&self, time_to_live_seconds: u32) -> u16 {
        loop {
            // Get all claimed instance id from DB
            let claimed_identities = IdentityClaimEntity::select_all_identity_claim(
                &self.cassandra_provider,
                &self.cassandra_provider.app_keyspace,
            )
            .await;
            if claimed_identities.len() < usize::from(UniqueTime::MAX_INSTANCE_ID) {
                for identity_claim in 0..UniqueTime::MAX_INSTANCE_ID {
                    if !claimed_identities.contains(&identity_claim)
                        && IdentityClaimEntity::new(
                            identity_claim,
                            fragtale_client::time::get_timestamp_micros(),
                        )
                        .insert_if_not_exists(
                            &self.cassandra_provider,
                            &self.cassandra_provider.app_keyspace,
                            time_to_live_seconds,
                        )
                        .await
                    {
                        return identity_claim;
                    }
                }
            }
            // Back off a little before trying again
            tokio::task::yield_now().await;
        }
    }

    async fn free(&self, claimed_instance_id: u16) {
        IdentityClaimEntity::delete(
            &self.cassandra_provider,
            &self.cassandra_provider.app_keyspace,
            claimed_instance_id,
        )
        .await;
    }

    async fn refresh(&self, time_to_live_seconds: u32, claimed_instance_id: u16) -> bool {
        if let Some(ice) = IdentityClaimEntity::select(
            &self.cassandra_provider,
            &self.cassandra_provider.app_keyspace,
            claimed_instance_id,
        )
        .await
        {
            ice.insert(
                &self.cassandra_provider,
                &self.cassandra_provider.app_keyspace,
                time_to_live_seconds,
            )
            .await
        } else {
            log::warn!(
                "Failed to reclaim instance id, but it seems to be unused. Claiming it now."
            );
            IdentityClaimEntity::new(
                claimed_instance_id,
                fragtale_client::time::get_timestamp_micros(),
            )
            .insert_if_not_exists(
                &self.cassandra_provider,
                &self.cassandra_provider.app_keyspace,
                time_to_live_seconds,
            )
            .await
        }
    }

    async fn get_oldest_instance_id(&self) -> (u16, u64) {
        IdentityClaimEntity::select_all(
            &self.cassandra_provider,
            &self.cassandra_provider.app_keyspace,
        )
        .await
        .into_iter()
        .min_by_key(IdentityClaimEntity::get_first_claim_ts)
        .map(|ice| (ice.get_identity_claim(), ice.get_first_claim_ts()))
        .unwrap()
    }
}
