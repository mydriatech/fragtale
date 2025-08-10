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

//! Cassandra specific database code

mod cassandra_authorization_facade;
mod cassandra_consumer_delivery_facade;
mod cassandra_event_facade;
mod cassandra_event_tracking_facade;
mod cassandra_instance_id_facade;
mod cassandra_integrity_protection_facade;
mod cassandra_topic_facade;

pub use self::cassandra_authorization_facade::*;
pub use self::cassandra_consumer_delivery_facade::*;
pub use self::cassandra_event_facade::*;
pub use self::cassandra_event_tracking_facade::*;
pub use self::cassandra_instance_id_facade::*;
pub use self::cassandra_integrity_protection_facade::*;
pub use self::cassandra_topic_facade::*;
use crate::CassandraProvider;
use fragtale_dbp::dbp::facades::*;
use std::sync::Arc;

pub struct CassandraProviderFacades {
    authorization_facade: CassandraAuthorizationFacade,
    consumer_delivery_facade: CassandraConsumerDeliveryFacade,
    event_tracking_facade: CassandraEventTrackingFacade,
    event_facade: CassandraEventFacade,
    instance_id_facade: CassandraInstanceIdFacade,
    integrity_protection_facade: CassandraIntegrityProtectionFacade,
    topic_facade: CassandraTopicFacade,
}

impl CassandraProviderFacades {
    pub fn new(cassandra_provider: &Arc<CassandraProvider>) -> Self {
        Self {
            authorization_facade: CassandraAuthorizationFacade::new(cassandra_provider),
            consumer_delivery_facade: CassandraConsumerDeliveryFacade::new(cassandra_provider),
            event_tracking_facade: CassandraEventTrackingFacade::new(cassandra_provider),
            event_facade: CassandraEventFacade::new(cassandra_provider),
            instance_id_facade: CassandraInstanceIdFacade::new(cassandra_provider),
            integrity_protection_facade: CassandraIntegrityProtectionFacade::new(
                cassandra_provider,
            ),
            topic_facade: CassandraTopicFacade::new(cassandra_provider),
        }
    }

    pub fn get_bucket_from_timestamp_u64(timestamp_micros: u64) -> u64 {
        timestamp_micros / Self::get_bucket_span_micros()
    }

    pub fn get_shelf_from_timestamp_u16(timestamp_micros: u64) -> u16 {
        // Group bucket in a shelf, by roughly one year
        // 3,1536×10^13 microseconds per year, 2^45 ≃ 3,5×10^13
        u16::try_from(timestamp_micros >> 45).unwrap()
    }

    fn get_bucket_span_micros() -> u64 {
        // Hard max limit is 2 * 10^9 columns per row.
        // Assume approx 100 bytes per row and max 100 MiB per partition
        let max_bucket_rows: u64 = 1_000_000;
        let max_ops_per_second: u64 = 1_000_000;
        max_bucket_rows / max_ops_per_second * 1_000_000
    }
}

impl DatabaseProviderFacades for CassandraProviderFacades {
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
