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

//! Policy engine interface.

use crate::mb::auth::ClientIdentity;

/// The policy engine is responsible for finding out if a [ClientIdentity] is
/// authorized to a resource.
#[async_trait::async_trait]
pub trait PolicyEngine: Sync + Send {
    /// Return `true` if the `identity` is authorized to `resource`.
    async fn is_authorized_to_resource(&self, identity: &ClientIdentity, resource: &str) -> bool;

    /// Return `true` if any `identity` that is authorized to `resource`.
    async fn is_any_authorized_to_resource(&self, resource: &str) -> bool;

    /// Grant `identity` authorization for `resource`.
    async fn grant_access_to_resource_for(
        &self,
        identity: &ClientIdentity,
        resource: &str,
        expires: Option<u64>,
    ) -> bool;
}
