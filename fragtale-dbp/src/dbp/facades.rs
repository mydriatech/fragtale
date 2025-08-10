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

//! Database facades.

mod authorization_facade;
mod consumer_delivery_facade;
mod event_facade;
mod event_tracking_facade;
mod instance_id_facade;
mod integrity_protection_facade;
mod topic_facade;

pub use self::authorization_facade::*;
pub use self::consumer_delivery_facade::*;
pub use self::event_facade::*;
pub use self::event_tracking_facade::*;
pub use self::instance_id_facade::*;
pub use self::integrity_protection_facade::*;
pub use self::topic_facade::*;

/// Provide access to database facades.
pub trait DatabaseProviderFacades: Send + Sync {
    /// See [AuthorizationFacade].
    fn authorization_facade(&self) -> &dyn AuthorizationFacade;

    /// See [ConsumerDeliveryFacade].
    fn consumer_delivery_facade(&self) -> &dyn ConsumerDeliveryFacade;

    /// See [EventTrackingFacade].
    fn event_tracking_facade(&self) -> &dyn EventTrackingFacade;

    /// See [EventFacade].
    fn event_facade(&self) -> &dyn EventFacade;

    /// See [InstanceIdFacade].
    fn instance_id_facade(&self) -> &dyn InstanceIdFacade;

    /// See [IntegrityProtectionFacade].
    fn integrity_protection_facade(&self) -> &dyn IntegrityProtectionFacade;

    /// See [TopicFacade].
    fn topic_facade(&self) -> &dyn TopicFacade;
}
