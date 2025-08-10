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

//! Description of what and how to extract values from event documents.

use serde::Deserialize;
use serde::Serialize;

/// Description of what and how to extract values from event documents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct Extractor {
    /// Name of index key.
    result_name: String,
    /// One of a subset of data types defined by Cassandra.
    ///
    /// Example: "text" or "bigint"
    result_type: String,
    /// Type of extraction: "jsonpointer"
    extraction_type: String,
    /// When extraction_type is "jsonpointer", this points to the value to extract.
    /// E.g. "/property-of-document-root".
    extraction_path: String,
}

impl Extractor {
    // Prefix of database column name where the extracted value is stored.
    //pub const EXTRACTED_COLUMN_PREFIX: &'static str = "doc_";

    /// Return a new instance.
    pub fn new(
        result_name: String,
        result_type: String,
        extraction_type: String,
        extraction_path: String,
    ) -> Self {
        Self {
            result_name,
            result_type,
            extraction_type,
            extraction_path,
        }
    }

    /// Return a new instance for extracting text values from the
    /// `root_property` property in the root of the JSON document.
    pub fn from_string_root_property<S: AsRef<str>>(root_property: S) -> Self {
        Self {
            result_name: root_property.as_ref().to_string(),
            result_type: "text".to_string(),
            extraction_type: "jsonpointer".to_string(),
            extraction_path: "/".to_string() + root_property.as_ref(),
        }
    }

    /// Name of the extracted property..
    pub fn get_result_name(&self) -> &str {
        &self.result_name
    }

    /// Type of the extracted property.
    pub fn get_result_type(&self) -> &str {
        &self.result_type
    }

    /// The type of extraction used.
    pub fn get_extraction_type(&self) -> &str {
        &self.extraction_type
    }

    /// The path of the extracted value in a form suitable for the type of
    /// extraction used.
    pub fn get_extraction_path(&self) -> &str {
        &self.extraction_path
    }
}
