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

//! JSON Pointer extraction.

use fragtale_dbp::mb::ExtractedValue;
use fragtale_dbp::mb::MessageBrokerError;
use fragtale_dbp::mb::MessageBrokerErrorKind;

/// See [RFC 6901](https://www.rfc-editor.org/rfc/rfc6901)
pub fn extract_jsonpointer(
    document: &str,
    pointer: &str,
    result_type: &str,
) -> Result<Option<ExtractedValue>, MessageBrokerError> {
    // https://docs.rs/serde_json/latest/serde_json/value/enum.Value.html#method.pointer
    let document: serde_json::Value = serde_json::from_str(document).map_err(|e| {
        MessageBrokerErrorKind::PreStorageProcessorError
            .error_with_msg(format!("Failed to parse document as JSON: {e:?}"))
    })?;
    if let Some(value) = document.pointer(pointer) {
        Ok(ExtractedValue::new(result_type, value))
    } else {
        Ok(None)
    }
}
