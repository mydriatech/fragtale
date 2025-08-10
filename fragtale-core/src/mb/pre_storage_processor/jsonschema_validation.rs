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

//! JSON Schema validation.

use fragtale_dbp::mb::MessageBrokerError;
use fragtale_dbp::mb::MessageBrokerErrorKind;
use jsonschema::Draft;

/// [JSON Schema](https://json-schema.org/) validation.
pub fn validate_draft202012(schema: &str, document: &str) -> Result<(), MessageBrokerError> {
    let schema = serde_json::from_str(schema).map_err(|e| {
        MessageBrokerErrorKind::PreStorageProcessorError
            .error_with_msg(format!("Failed to parse schema as JSON: {e:?}"))
    })?;
    let document = serde_json::from_str(document).map_err(|e| {
        MessageBrokerErrorKind::PreStorageProcessorError
            .error_with_msg(format!("Failed to parse document as JSON: {e:?}"))
    })?;
    let compiled = jsonschema::options()
        .with_draft(Draft::Draft202012)
        .build(&schema)
        .map_err(|e| {
            MessageBrokerErrorKind::PreStorageProcessorError
                .error_with_msg(format!("Failed to compile JSONSchema: {e:?}"))
        })?;
    compiled.validate(&document).map_err(|e| {
        log::debug!("Validation error at '{}': {}", e.instance_path, e);
        MessageBrokerErrorKind::PreStorageProcessorError
            .error_with_msg("Failed to validate document")
    })
}
