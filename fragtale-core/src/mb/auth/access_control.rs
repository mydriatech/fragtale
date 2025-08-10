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

//! Message broker access control.

mod access_control_cache;
mod policy_engine;
mod policy_engine_local;

pub use self::access_control_cache::*;
pub use self::policy_engine::*;
pub use self::policy_engine_local::*;
use super::ClientIdentity;
use fragtale_dbp::dbp::DatabaseProvider;
use fragtale_dbp::mb::MessageBrokerError;
use fragtale_dbp::mb::MessageBrokerErrorKind;
use std::sync::Arc;

/// Access controller.
pub struct AccessControl {
    cache: Arc<AccessControlCache>,
    policy_engine: Arc<dyn PolicyEngine>,
}

impl AccessControl {
    /// Return a new instance.
    pub async fn new(dbp: &Arc<DatabaseProvider>) -> Arc<Self> {
        Arc::new(Self {
            cache: AccessControlCache::new().await,
            policy_engine: PolicyEngineLocal::new(dbp).await,
        })
    }

    /// Error out with [MessageBrokerErrorKind::Unauthorized] if the client
    /// identity isn't allowed to write to the specified topic.
    pub async fn assert_allowed_topic_write(
        &self,
        identity: &ClientIdentity,
        topic_id: &str,
    ) -> Result<(), MessageBrokerError> {
        let resource = format!("/topic/{topic_id}/write");
        let res = self
            .assert_authorized_to_resource(identity, &resource)
            .await;
        // Check if this unclaimed and claim it if so.
        if !self
            .policy_engine
            .is_any_authorized_to_resource(&resource)
            .await
        {
            return self
                .grant_access_to_resource_for(identity, &resource, None)
                .await;
        }
        res
    }

    /// Error out with [MessageBrokerErrorKind::Unauthorized] if the client
    /// identity isn't allowed to read from the specified topic.
    pub async fn assert_allowed_topic_read(
        &self,
        identity: &ClientIdentity,
        topic_id: &str,
    ) -> Result<(), MessageBrokerError> {
        self.assert_authorized_to_resource(identity, &format!("/topic/{topic_id}/read"))
            .await
    }

    /// Error out with [MessageBrokerErrorKind::Unauthorized] if the client
    /// identity isn't allowed to read from the specified resource.
    async fn assert_authorized_to_resource(
        &self,
        identity: &ClientIdentity,
        resource: &str,
    ) -> Result<(), MessageBrokerError> {
        if self.cache.is_authorized_to_resource(identity, resource) {
            Ok(())?;
        }
        if self
            .policy_engine
            .is_authorized_to_resource(identity, resource)
            .await
        {
            // Update cache
            self.cache.insert(identity, resource);
            Ok(())
        } else {
            let msg = format!("Identity: '{identity}' is not authorized to '{resource}'.");
            log::info!("{msg}");
            Err(MessageBrokerErrorKind::Unauthorized.error_with_msg(msg))
        }
    }

    /// Grant access for client identity to the specified topic.
    pub async fn grant_write_to_topic_for(
        &self,
        identity: &ClientIdentity,
        topic_id: &str,
        expires: Option<u64>,
    ) -> Result<(), MessageBrokerError> {
        self.grant_access_to_resource_for(identity, &format!("/topic/{topic_id}/write"), expires)
            .await
    }

    /// Grant access for client identity to the specified resource.
    async fn grant_access_to_resource_for(
        &self,
        identity: &ClientIdentity,
        resource: &str,
        expires: Option<u64>,
    ) -> Result<(), MessageBrokerError> {
        self.policy_engine
            .grant_access_to_resource_for(identity, resource, expires)
            .await
            .then_some(())
            .ok_or_else(|| {
                let msg = format!(
                    "Failed to grant identity '{identity}' access to authorized '{resource}'."
                );
                log::warn!("{msg}");
                MessageBrokerErrorKind::Unspecified.error_with_msg(msg)
            })?;
        log::info!("Granted identity '{identity}' access to authorized '{resource}'.");
        Ok(())
    }
}
