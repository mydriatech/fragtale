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

//! Database facade for operation related to instance identifier reservation.

/// Database facade for operation related to instance identifier reservation.
#[async_trait::async_trait]
pub trait InstanceIdFacade: Send + Sync {
    /// Claim a unique identifier for this app-instance.
    async fn claim(&self, time_to_live_seconds: u32) -> u16;

    /// Free up the instance id.
    async fn free(&self, claimed_instance_id: u16);

    /// Refresh claim of identifier for this app-instance
    ///
    /// Returns `false` if the instance id could not be reclaimed.
    async fn refresh(&self, time_to_live_seconds: u32, claimed_instance_id: u16) -> bool;

    /// Return the oldest alive instance id claim and when it was claimed.
    ///
    /// This can be used to determine when new configuration has been rolled
    /// out or to ensure that a task is only performed at a single instance
    /// (the oldest one).
    async fn get_oldest_instance_id(&self) -> (u16, u64);
}
