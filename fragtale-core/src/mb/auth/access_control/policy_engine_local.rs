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

//! Built-in policy engine.

use super::PolicyEngine;
use crate::mb::auth::ClientIdentity;
use fragtale_dbp::dbp::DatabaseProvider;
use fragtale_dbp::dbp::facades::DatabaseProviderFacades;
use std::sync::Arc;

/// Built-in [PolicyEngine] implementation using app persistence.
///
/// This [PolicyEngine] will allow any authenticated client to read from topics,
/// since all event retrievals will be logged.
///
/// Writing to topic is only allowed by the identity that first performed an
/// operation that required write access.
///
/// This supports a model with a single topic "owner" and access to the topic's
/// data dont' have to be prevented, but should be auditable.
pub struct PolicyEngineLocal {
    dbp: Arc<DatabaseProvider>,
}

impl PolicyEngineLocal {
    /// Return a new instance.
    pub async fn new(dbp: &Arc<DatabaseProvider>) -> Arc<Self> {
        Arc::new(Self {
            dbp: Arc::clone(dbp),
        })
    }

    /// Split the resource of expected format `/type/object_id/operation` into
    /// parts.
    fn split_resource_into_parts(resource: &str) -> Result<(&str, &str, &str), String> {
        let mut resource_parts = resource[1..].split('/');
        let resource_type = resource_parts.next().ok_or_else(||
            format!("Resource '{resource}' is missing resource type. (Format: '/type/object_id/operation')")
        )?;
        let object_id = resource_parts.next().ok_or_else(||
            format!("Resource '{resource}' is missing object identifier part. (Format: '/type/object_id/operation')")
        )?;
        let operation = resource_parts.next().ok_or_else(||
            format!("Resource '{resource}' is missing operation part. (Format: '/type/object_id/operation')")
        )?;
        if resource_parts.next().is_some() {
            Err(format!(
                "Resource '{resource}' has too many parts. (Format: '/type/object_id/operation')"
            ))?;
        }
        Ok((resource_type, object_id, operation))
    }
}

#[async_trait::async_trait]
impl PolicyEngine for PolicyEngineLocal {
    async fn is_authorized_to_resource(&self, identity: &ClientIdentity, resource: &str) -> bool {
        if identity.is_local() {
            // The PolicyEngineLocal policy is to always allow local tokens access to anything.
            return true;
        }
        let (resource_type, _object_id, operation) = match Self::split_resource_into_parts(resource)
        {
            Ok(value) => value,
            Err(e) => {
                log::info!("Denied access to '{resource}': {e}");
                return false;
            }
        };
        match resource_type {
            "topic" => {
                match operation {
                    "read" => {
                        // The PolicyEngineLocal policy is to always allow topic reads.
                        true
                    }
                    "write" => {
                        self.dbp
                            .authorization_facade()
                            .is_authorized_to_resource(identity.identity_string(), resource)
                            .await
                    }
                    _ => {
                        log::info!(
                            "Denied access to '{resource}', since operation '{operation}' is unknown."
                        );
                        false
                    }
                }
            }
            _ => {
                log::info!(
                    "Denied access to '{resource}', since resource type '{resource_type}' is unknown."
                );
                false
            }
        }
    }

    async fn is_any_authorized_to_resource(&self, resource: &str) -> bool {
        let (resource_type, _object_id, operation) = match Self::split_resource_into_parts(resource)
        {
            Ok(value) => value,
            Err(e) => {
                log::info!("Denied access to '{resource}': {e}");
                return false;
            }
        };
        match resource_type {
            "topic" => {
                match operation {
                    "read" => {
                        // The PolicyEngineLocal policy is to always allow topic reads.
                        true
                    }
                    "write" => {
                        self.dbp
                            .authorization_facade()
                            .is_any_authorized_to_resource(resource)
                            .await
                    }
                    _ => {
                        log::info!(
                            "Denied access to '{resource}', since operation '{operation}' is unknown."
                        );
                        false
                    }
                }
            }
            _ => {
                log::info!(
                    "Denied access to '{resource}', since resource type '{resource_type}' is unknown."
                );
                false
            }
        }
    }

    async fn grant_access_to_resource_for(
        &self,
        identity: &ClientIdentity,
        resource: &str,
        expires: Option<u64>,
    ) -> bool {
        if identity.is_local() {
            // NOOP: The PolicyEngineLocal policy is to always allow local tokens access to anything.
            return true;
        }
        let (resource_type, _object_id, operation) =
            Self::split_resource_into_parts(resource).unwrap();
        match resource_type {
            "topic" => {
                match operation {
                    "read" => {
                        // NOOP: The PolicyEngineLocal policy is to always allow topic reads.
                        true
                    }
                    "write" => {
                        self.dbp
                            .authorization_facade()
                            .grant_access_to_resource_for(
                                identity.identity_string(),
                                resource,
                                expires,
                            )
                            .await
                    }
                    _ => {
                        log::warn!(
                            "Unable to grant access to '{resource}', since operation '{operation}' is unknown."
                        );
                        false
                    }
                }
            }
            _ => {
                log::warn!(
                    "Unable to grant access to '{resource}', since resource type '{resource_type}' is unknown."
                );
                false
            }
        }
    }
}
