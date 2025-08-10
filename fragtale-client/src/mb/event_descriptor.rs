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

//! Event schema, schema versioning and indexed column extraction.

mod descriptor_version;
mod event_schema;
mod extractor;

pub use self::descriptor_version::DescriptorVersion;
pub use self::event_schema::EventSchema;
pub use self::extractor::Extractor;
use serde::Deserialize;
use serde::Serialize;

/// Specify handling of events when published to a topic.
///
/// Contains event schema, schema versioning and indexed document value
/// extraction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct EventDescriptor {
    /// The current version.
    ///
    /// See [Self::get_version].
    version: u64,
    /// The minumum supported version that is allowed to be used once this
    /// version is en effect.
    ///
    /// See [Self::get_version_min].
    version_min: Option<u64>,
    /// Optional event schema used to validate event documents.
    ///
    /// See [Self::get_event_schema].
    #[schema(inline)]
    event_schema: Option<EventSchema>,
    /// Optional extractors for indexing document values.
    ///
    /// See [Self::get_extractors].
    #[schema(inline)]
    extractors: Option<Vec<Extractor>>,
}

impl EventDescriptor {
    /// Return a new instance.
    pub fn new(
        version: u64,
        version_min: Option<u64>,
        event_schema: Option<EventSchema>,
        extractors: Option<Vec<Extractor>>,
    ) -> Self {
        Self {
            version,
            version_min,
            event_schema,
            extractors,
        }
    }

    /// Return as a JSON serialized String.
    pub fn as_string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    /// Return a new instance from JSON serialization.
    pub fn from_string<S: AsRef<str>>(value: S) -> Self {
        serde_json::from_str(value.as_ref()).unwrap()
    }

    /// Return a new schema-less instance of version `1.0.0`.
    pub fn from_extractors(extractors: &[Extractor]) -> Self {
        Self::new(
            DescriptorVersion::new(1, 0, 0).as_encoded(),
            None,
            None,
            Some(extractors.to_vec()),
        )
    }

    /// Version of [EventDescriptor] for topic.
    pub fn get_version(&self) -> u64 {
        self.version
    }

    /// Minimum support version of [EventDescriptor] for topic once this is in
    /// effect.
    pub fn get_version_min(&self) -> Option<u64> {
        self.version_min
    }

    /// Optional event schema.
    pub fn get_event_schema(&self) -> &Option<EventSchema> {
        &self.event_schema
    }

    /// Extractors of document values for indexing.
    ///
    /// Data can still be extracted even if a schema isn't enforced.
    pub fn get_extractors(&self) -> &Option<Vec<Extractor>> {
        &self.extractors
    }
}
