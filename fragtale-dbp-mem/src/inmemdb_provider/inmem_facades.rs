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

//! Ephemeral in-memory specific database code.

mod inmem_authorization_facade;
mod inmem_consumer_delivery_facade;
mod inmem_event_facade;
mod inmem_event_tracking_facade;
mod inmem_instance_id_facade;
mod inmem_integrity_protection_facade;
mod inmem_topic_facade;

pub use self::inmem_authorization_facade::*;
pub use self::inmem_consumer_delivery_facade::*;
pub use self::inmem_event_facade::*;
pub use self::inmem_event_tracking_facade::*;
pub use self::inmem_instance_id_facade::*;
pub use self::inmem_integrity_protection_facade::*;
pub use self::inmem_topic_facade::*;
use super::InMemoryDatabaseProvider;
use fragtale_dbp::dbp::facades::*;
use std::sync::Arc;

/// Ephemeral in-memory specific database code.
pub struct InMemProviderFacades {
    authorization_facade: InMemAuthorizationFacade,
    consumer_delivery_facade: InMemConsumerDeliveryFacade,
    event_tracking_facade: InMemEventTrackingFacade,
    event_facade: InMemEventFacade,
    instance_id_facade: InMemInstanceIdFacade,
    integrity_protection_facade: InMemIntegrityProtectionFacade,
    topic_facade: InMemTopicFacade,
}

impl InMemProviderFacades {
    /// Return a new instance.
    pub fn new(inmem_provider: &Arc<InMemoryDatabaseProvider>) -> Self {
        Self {
            authorization_facade: InMemAuthorizationFacade::new(inmem_provider),
            consumer_delivery_facade: InMemConsumerDeliveryFacade::new(inmem_provider),
            event_tracking_facade: InMemEventTrackingFacade::new(inmem_provider),
            event_facade: InMemEventFacade::new(inmem_provider),
            instance_id_facade: InMemInstanceIdFacade::default(),
            integrity_protection_facade: InMemIntegrityProtectionFacade::default(),
            topic_facade: InMemTopicFacade::new(inmem_provider),
        }
    }
}

impl DatabaseProviderFacades for InMemProviderFacades {
    fn authorization_facade(&self) -> &dyn AuthorizationFacade {
        &self.authorization_facade
    }

    fn consumer_delivery_facade(&self) -> &dyn ConsumerDeliveryFacade {
        &self.consumer_delivery_facade
    }

    fn event_tracking_facade(&self) -> &dyn EventTrackingFacade {
        &self.event_tracking_facade
    }

    fn event_facade(&self) -> &dyn EventFacade {
        &self.event_facade
    }

    fn instance_id_facade(&self) -> &dyn InstanceIdFacade {
        &self.instance_id_facade
    }

    fn integrity_protection_facade(&self) -> &dyn IntegrityProtectionFacade {
        &self.integrity_protection_facade
    }

    fn topic_facade(&self) -> &dyn TopicFacade {
        &self.topic_facade
    }
}
