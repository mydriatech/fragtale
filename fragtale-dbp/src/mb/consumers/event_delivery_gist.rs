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

//! The core information that makes up an event.

use crate::mb::UniqueTime;

/// The core information that makes up an event.
pub struct EventDeliveryGist {
    unique_time: UniqueTime,
    document: String,
    protection_ref: String,
    correlation_token: String,
}

impl EventDeliveryGist {
    /// Return a new instance.
    pub fn new(
        unique_time: UniqueTime,
        document: String,
        protection_ref: String,
        correlation_token: String,
    ) -> Self {
        Self {
            unique_time,
            document,
            protection_ref,
            correlation_token,
        }
    }

    /// Return the event's `UniqueTime`.
    pub fn get_unique_time(&self) -> UniqueTime {
        self.unique_time
    }

    /// Return the event document.
    pub fn get_document(&self) -> &str {
        &self.document
    }

    /// Return the String encoded `IntegrityProtectionReference`.
    pub fn get_protection_ref(&self) -> &str {
        &self.protection_ref
    }

    /// Return the String encoded `CorrelationToken`.
    pub fn get_correlation_token(&self) -> &str {
        &self.correlation_token
    }

    /// Deconstruct this struct into its parts.
    pub fn into_parts(self) -> (UniqueTime, String, String, String) {
        (
            self.unique_time,
            self.document,
            self.protection_ref,
            self.correlation_token,
        )
    }
}
