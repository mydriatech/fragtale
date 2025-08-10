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

//! Ephemeral in-memory implementation of [AuthorizationFacade].

use crate::InMemoryDatabaseProvider;
use crossbeam_skiplist::SkipSet;
use fragtale_dbp::dbp::facades::AuthorizationFacade;
use std::sync::Arc;

/// Ephemeral in-memory specific database code
pub struct InMemAuthorizationFacade {
    //inmem_provider: Arc<InMemoryDatabaseProvider>,
    authorizations: SkipSet<String>,
}

impl InMemAuthorizationFacade {
    /// Return a new instance.
    pub fn new(_inmem_provider: &Arc<InMemoryDatabaseProvider>) -> Self {
        Self {
            //inmem_provider: Arc::clone(inmem_provider),
            authorizations: SkipSet::default(),
        }
    }

    /// Concat identity and resource into a common lookup key using a char
    /// that isn't allowed in the identity string.
    fn to_key(identity: &str, resource: &str) -> String {
        identity.to_string() + "|" + resource
    }
}

#[async_trait::async_trait]
impl AuthorizationFacade for InMemAuthorizationFacade {
    async fn is_authorized_to_resource(&self, identity: &str, resource: &str) -> bool {
        self.authorizations
            .contains(&Self::to_key(identity, resource))
    }

    async fn is_any_authorized_to_resource(&self, resource: &str) -> bool {
        let pat = ";".to_string() + resource;
        self.authorizations
            .iter()
            .any(|entry| entry.value().ends_with(&pat))
    }

    async fn grant_access_to_resource_for(
        &self,
        identity: &str,
        resource: &str,
        _expires: Option<u64>,
    ) -> bool {
        self.authorizations.insert(Self::to_key(identity, resource));
        true
    }

    async fn deny_access_to_resource_for(
        &self,
        identity: &str,
        resource: &str,
        _expires: Option<u64>,
    ) -> bool {
        self.authorizations
            .remove(&Self::to_key(identity, resource));
        true
    }
}
