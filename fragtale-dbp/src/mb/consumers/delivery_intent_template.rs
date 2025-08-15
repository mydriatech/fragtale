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

//! Meta-data about an event used during delivery.

use crate::mb::UniqueTime;

/// Meta-data about an event used during delivery.
#[derive(Clone)]
pub struct DeliveryIntentTemplate {
    unique_time: UniqueTime,
    event_id: String,
    descriptor_version: Option<u64>,
    failed_intent_ts: Option<u64>,
}
impl DeliveryIntentTemplate {
    /// Return a new instance.
    pub fn new(
        unique_time: UniqueTime,
        event_id: String,
        descriptor_version: Option<u64>,
        failed_intent_ts: Option<u64>,
    ) -> Self {
        Self {
            unique_time,
            event_id,
            descriptor_version,
            failed_intent_ts,
        }
    }

    /// Return the event's `UniqueTime`.
    pub fn get_unique_time(&self) -> UniqueTime {
        self.unique_time
    }

    /// Return the event's identifier.
    pub fn get_event_id(&self) -> &str {
        &self.event_id
    }

    /// Return the version of the event's descriptor (schema and extractors).
    pub fn get_descriptor_version(&self) -> &Option<u64> {
        &self.descriptor_version
    }

    /// Return when delivery of the event failed (if any).
    pub fn get_failed_intent_ts(&self) -> &Option<u64> {
        &self.failed_intent_ts
    }
}
