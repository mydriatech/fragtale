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

//! Event model.

use crate::mb::ExtractedValue;
use crate::mb::UniqueTime;
use std::collections::HashMap;
use tyst::Tyst;

/// Event model.
pub struct TopicEvent {
    event_id: String,
    document: String,
    priority: u8,
    protection_ref: String,
    correlation_token: String,
    additional_columns: HashMap<String, ExtractedValue>,
    descriptor_version: Option<u64>,
    unique_time: UniqueTime,
}

impl TopicEvent {
    /// Return a new instance.
    pub fn new(
        document: &str,
        priority: u8,
        protection_ref: &str,
        correlation_token: &str,
        additional_columns: HashMap<String, ExtractedValue>,
        descriptor_version: Option<u64>,
        unique_time: UniqueTime,
    ) -> Self {
        Self {
            event_id: Self::event_id_from_document(document),
            document: document.to_owned(),
            priority,
            protection_ref: protection_ref.to_owned(),
            correlation_token: correlation_token.to_owned(),
            additional_columns,
            descriptor_version,
            unique_time,
        }
    }

    /// Return the event_id (fingerprint) of the document.
    pub fn event_id_from_document(document: &str) -> String {
        tyst::encdec::hex::encode(
            &Tyst::instance()
                .digests()
                .by_oid(&tyst::encdec::oid::as_string(tyst::oids::digest::SHA3_512))
                .unwrap()
                .hash(document.as_bytes()),
        )
    }

    /// Return the event identifier (message digest of the document).
    pub fn get_event_id(&self) -> &str {
        &self.event_id
    }

    /// Return the event document.
    pub fn get_document(&self) -> &str {
        &self.document
    }

    /// Return the event priority.
    pub fn get_priority(&self) -> u8 {
        self.priority
    }

    /// Return the event integrity protection reference.
    pub fn get_protection_ref(&self) -> &str {
        &self.protection_ref
    }

    /// Return the event's correlation token-
    pub fn get_correlation_token(&self) -> &str {
        &self.correlation_token
    }

    /// Return a key-value map of extracted document fields.
    pub fn get_additional_columns(&self) -> &HashMap<String, ExtractedValue> {
        &self.additional_columns
    }

    /// Return the version of the event's descriptor.
    pub fn get_descriptor_version(&self) -> Option<u64> {
        self.descriptor_version.to_owned()
    }

    /// Return the event's [UniqueTime].
    pub fn get_unique_time(&self) -> UniqueTime {
        self.unique_time.to_owned()
    }
}
