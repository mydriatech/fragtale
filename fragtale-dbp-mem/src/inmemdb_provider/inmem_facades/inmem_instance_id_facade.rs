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

//! Ephemeral in-memory implementation of [InstanceIdFacade].

use fragtale_dbp::dbp::facades::InstanceIdFacade;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

/// Ephemeral in-memory implementation of [InstanceIdFacade].
#[derive(Default)]
pub struct InMemInstanceIdFacade {
    first_claim: AtomicU64,
}

#[async_trait::async_trait]
impl InstanceIdFacade for InMemInstanceIdFacade {
    async fn claim(&self, _time_to_live_seconds: u32) -> u16 {
        self.first_claim.store(
            fragtale_client::time::get_timestamp_micros(),
            Ordering::Relaxed,
        );
        0
    }

    async fn free(&self, _claimed_instance_id: u16) {
        // NOOP
    }

    async fn get_oldest_instance_id(&self) -> (u16, u64) {
        (0, self.first_claim.load(Ordering::Relaxed))
    }

    async fn refresh(&self, _time_to_live_seconds: u32, _claimed_instance_id: u16) -> bool {
        // NOOP: In-mem instance lives forever
        true
    }
}
