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

//! Database Provider abstraction

pub mod facades;

use self::facades::*;
use std::sync::Arc;

/// The Database Provider.
///
/// Implementation logic is abstracted by [DatabaseProviderFacades] for related
/// operations.
pub struct DatabaseProvider {
    facades: Box<Arc<dyn DatabaseProviderFacades>>,
}

impl DatabaseProvider {
    /// Return a new instgance.
    pub fn new(database_provider_facades: Arc<dyn DatabaseProviderFacades>) -> Self {
        Self {
            facades: Box::new(database_provider_facades),
        }
    }
}

impl DatabaseProviderFacades for DatabaseProvider {
    fn authorization_facade(&self) -> &dyn AuthorizationFacade {
        self.facades.authorization_facade()
    }

    fn consumer_delivery_facade(&self) -> &dyn ConsumerDeliveryFacade {
        self.facades.consumer_delivery_facade()
    }

    fn event_tracking_facade(&self) -> &dyn EventTrackingFacade {
        self.facades.event_tracking_facade()
    }

    fn event_facade(&self) -> &dyn EventFacade {
        self.facades.event_facade()
    }

    fn instance_id_facade(&self) -> &dyn InstanceIdFacade {
        self.facades.instance_id_facade()
    }

    fn integrity_protection_facade(&self) -> &dyn IntegrityProtectionFacade {
        self.facades.integrity_protection_facade()
    }

    fn topic_facade(&self) -> &dyn TopicFacade {
        self.facades.topic_facade()
    }
}
