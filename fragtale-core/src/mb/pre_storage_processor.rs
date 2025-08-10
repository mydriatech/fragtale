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

//! Schema validation and indexed column extraction from documents.

mod jsonpointer_extraction;
mod jsonschema_validation;

use super::event_descriptor_cache::EventDescriptorCache;
use fragtale_client::mb::event_descriptor::DescriptorVersion;
use fragtale_client::mb::event_descriptor::EventDescriptor;
use fragtale_dbp::mb::ExtractedValue;
use fragtale_dbp::mb::MessageBrokerError;
use fragtale_dbp::mb::MessageBrokerErrorKind;
use std::collections::HashMap;
use std::sync::Arc;

/// Validates schema and extracts indexed column(s) from document.
pub struct PreStorageProcessor {
    event_descriptor_cache: Arc<EventDescriptorCache>,
}

impl PreStorageProcessor {
    /// Return a new instance.
    pub fn new(event_descriptor_cache: &Arc<EventDescriptorCache>) -> Arc<Self> {
        Arc::new(Self {
            event_descriptor_cache: Arc::clone(event_descriptor_cache),
        })
    }

    /// Validate document schema used in the [DescriptorVersion] and extract any
    /// indexed column found in the document.
    pub async fn validate_and_extract(
        &self,
        topic_id: &str,
        event_document: &str,
        descriptor_version: Option<DescriptorVersion>,
    ) -> Result<(HashMap<String, ExtractedValue>, Option<DescriptorVersion>), MessageBrokerError>
    {
        // Check if "descriptor_version" is still allowed → Error if not
        self.assert_allowed_descriptor_version(topic_id, &descriptor_version)?;
        // Get schema (if any) from cache
        let event_descriptor_opt = self
            .get_event_descriptor(topic_id, &descriptor_version)
            .await?;
        let column_to_value_map = if let Some(event_descriptor) = &event_descriptor_opt {
            // Validate document against schema, if present
            Self::assert_event_schema_compliance(event_descriptor, event_document)?;
            // Extract values of interest from the document
            Self::extract_values_from_document(event_descriptor, event_document)?
        } else {
            HashMap::new()
        };
        Ok((
            column_to_value_map,
            event_descriptor_opt
                .as_deref()
                .map(EventDescriptor::get_version)
                .map(DescriptorVersion::from_encoded),
        ))
    }

    /// Check if "descriptor_version" is still allowed → Error if not
    fn assert_allowed_descriptor_version(
        &self,
        topic_id: &str,
        descriptor_version_opt: &Option<DescriptorVersion>,
    ) -> Result<(), MessageBrokerError> {
        if let Some(version) = descriptor_version_opt
            && let Some(version_min) = &self
                .event_descriptor_cache
                .get_descriptor_version_min(topic_id)
            && version < version_min
        {
            Err(
                MessageBrokerErrorKind::PreStorageProcessorError.error_with_msg(format!(
                    "The description version {version:?} is no longer allowed (<{version_min:?})."
                )),
            )?;
        }
        Ok(())
    }

    /// Get the EventDescriptor from cache (if any)
    async fn get_event_descriptor(
        &self,
        topic_id: &str,
        descriptor_version_opt: &Option<DescriptorVersion>,
    ) -> Result<Option<Arc<EventDescriptor>>, MessageBrokerError> {
        if let Some(descriptor_version) = descriptor_version_opt {
            let event_descriptor = self
                .event_descriptor_cache
                .get_event_descriptor_by_topic_and_version(topic_id, descriptor_version)
                .await;
            // Fail if producer failed to register it.
            if event_descriptor.is_none() {
                Err(
                    MessageBrokerErrorKind::PreStorageProcessorError.error_with_msg(format!(
                        "The description version {descriptor_version:?} does not exist. Please register before use."
                    )),
                )?;
            }
            Ok(event_descriptor)
        } else {
            Ok(self
                .event_descriptor_cache
                .get_event_descriptor_by_topic_latest(topic_id))
        }
    }

    /// Validate document against schema, if present
    fn assert_event_schema_compliance(
        event_descriptor: &EventDescriptor,
        event_document: &str,
    ) -> Result<(), MessageBrokerError> {
        if let Some(event_schema) = event_descriptor.get_event_schema() {
            match event_schema.get_schema_type() {
                "https://json-schema.org/draft/2020-12/schema" => {
                    jsonschema_validation::validate_draft202012(
                        event_schema.get_schema_data(),
                        event_document,
                    )?
                }
                schema_type => {
                    Err(MessageBrokerErrorKind::PreStorageProcessorError
                        .error_with_msg(format!("Unsupported schema type: '{schema_type}'")))?;
                }
            }
        }
        Ok(())
    }

    /// Extract indexed values from the document
    fn extract_values_from_document(
        event_descriptor: &EventDescriptor,
        event_document: &str,
    ) -> Result<HashMap<String, ExtractedValue>, MessageBrokerError> {
        let mut column_to_value_map = HashMap::new();
        if let Some(extractors) = event_descriptor.get_extractors() {
            for extractor in extractors {
                if let Some(value) = match extractor.get_extraction_type() {
                    "jsonpointer" => jsonpointer_extraction::extract_jsonpointer(
                        event_document,
                        extractor.get_extraction_path(),
                        extractor.get_result_type(),
                    )?,
                    extraction_type => {
                        return Err(MessageBrokerErrorKind::PreStorageProcessorError
                            .error_with_msg(format!(
                                "Unsupported extraction type: '{extraction_type}'"
                            )));
                    }
                } {
                    column_to_value_map.insert(extractor.get_result_name().to_owned(), value);
                }
            }
        }
        Ok(column_to_value_map)
    }
}
