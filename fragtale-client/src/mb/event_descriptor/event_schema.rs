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

//! Event schema.

use serde::Deserialize;
use serde::Serialize;

/// The even schema for each topic is an optional feature to ensure that
/// documents are well formed.
///
/// Schemas must be self-contained.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct EventSchema {
    schema_id: String,
    schema_type: String,
    schema_data: String,
}

impl EventSchema {
    /// Return a new instance.
    pub fn new(schema_id: String, schema_type: String, schema_data: String) -> Self {
        Self {
            schema_id,
            schema_type,
            schema_data,
        }
    }

    /// Return the schema identifier.
    ///
    /// Example: `https://example.com/json-schema-for-topic-x-v0.0.1.json`
    pub fn get_schema_id(&self) -> &str {
        &self.schema_id
    }

    /// Return the schema type.
    ///
    /// Example: `https://json-schema.org/draft/2020-12/schema`
    pub fn get_schema_type(&self) -> &str {
        &self.schema_type
    }

    /// Return the actual self-contained schema.
    ///
    /// Example: `{ "$schema": "....", "$id": "..."}`
    pub fn get_schema_data(&self) -> &str {
        &self.schema_data
    }
}
