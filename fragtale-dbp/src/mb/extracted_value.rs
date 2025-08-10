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

//! Extracted document value.

/// Extracted JSON document value.
#[derive(Debug, Clone)]
pub enum ExtractedValue {
    /// Document value in String format.
    Text(String),
    /// Document value in numeric format.
    BigInt(i64),
}

impl ExtractedValue {
    /// Return new instance from JSON value.
    pub fn new(result_type: &str, value: &serde_json::Value) -> Option<Self> {
        match result_type {
            "text" => {
                if let Some(text) = value.as_str() {
                    Some(ExtractedValue::Text(text.to_owned()))
                } else {
                    log::debug!(
                        "Failed to parse json value '{value:?}' as result_type '{result_type}'. Ignoring."
                    );
                    None
                }
            }
            "bigint" => {
                if let Some(number) = value.as_i64() {
                    Some(ExtractedValue::BigInt(number))
                } else {
                    log::debug!(
                        "Failed to parse json value '{value:?}' as result_type '{result_type}'. Ignoring."
                    );
                    None
                }
            }
            result_type => {
                log::debug!("Ignoring unsupported result_type {result_type}.");
                None
            }
        }
    }
}
