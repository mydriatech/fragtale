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

//! Topic facade implementation for Cassandra.

use crate::CassandraProvider;
use crate::cassandra_provider::EventDescriptorEntity;
use crate::cassandra_provider::EventEntity;
use crate::cassandra_provider::entity::TopicEntity;
use fragtale_dbp::dbp::facades::TopicFacade;
use fragtale_dbp::mb::MessageBrokerError;
use fragtale_dbp::mb::MessageBrokerErrorKind;
use std::sync::Arc;

/// Topic facade implementation for Cassandra.
pub struct CassandraTopicFacade {
    cassandra_provider: Arc<CassandraProvider>,
}

impl CassandraTopicFacade {
    /// Return a new instance.
    pub fn new(cassandra_provider: &Arc<CassandraProvider>) -> Self {
        Self {
            cassandra_provider: Arc::clone(cassandra_provider),
        }
    }

    const ALLOWED_TOPIC_ID_CHARS: &str = "abcdefghijklmnopqrstuvwxyz0123456789_";

    async fn assert_topic_id_well_formed(&self, topic_id: &str) -> Result<(), MessageBrokerError> {
        if topic_id
            .chars()
            .any(|c| !Self::ALLOWED_TOPIC_ID_CHARS.contains(c))
        {
            Err(MessageBrokerErrorKind::MalformedIdentifier.error_with_msg(
                "Invalid chars in topic id '{topic_id}'. Only a-z0-9_ are allowed.",
            ))?;
        }
        let topic_keyspace_len = self.cassandra_provider.app_keyspace.len() + 1 + topic_id.len();
        if topic_id.is_empty() || topic_keyspace_len > 48 {
            let max_len = 48 - self.cassandra_provider.app_keyspace.len() + 1;
            Err(
                MessageBrokerErrorKind::MalformedIdentifier.error_with_msg(format!(
                    "Invalid length of topic id '{topic_id}'. Must be of length 1-{max_len}."
                )),
            )?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl TopicFacade for CassandraTopicFacade {
    async fn ensure_topic_setup(&self, topic_id: &str) -> Result<(), MessageBrokerError> {
        self.assert_topic_id_well_formed(topic_id).await?;
        self.cassandra_provider
            .ensure_topic_exists_internal(topic_id)
            .await;
        Ok(())
    }

    /// Return an ordered list of existing topic indentifiers and an indicator
    /// if there might be additional results.
    async fn get_topic_ids(&self, from: &Option<String>) -> (Vec<String>, bool) {
        let max_topic_id_len = 48 - self.cassandra_provider.app_keyspace.len() + 1;
        // For the app_keyspace "fragtale", this implies a limit of 6721 items.
        let limit = 256 * 1024 / max_topic_id_len;
        let res = if let Some(from) = from {
            TopicEntity::select_all_topic_id_from(
                &self.cassandra_provider,
                &self.cassandra_provider.app_keyspace,
                from,
                limit,
            )
            .await
        } else {
            TopicEntity::select_all_topic_id(
                &self.cassandra_provider,
                &self.cassandra_provider.app_keyspace,
                limit,
            )
            .await
        };
        let potentially_more_results = res.len() == limit;
        (res, potentially_more_results)
    }

    async fn event_descriptor_persists(
        &self,
        topic_id: &str,
        version: u64,
        version_min: Option<u64>,
        schema_id: &Option<String>,
        event_descriptor: &str,
    ) -> bool {
        EventDescriptorEntity::new(topic_id, version, version_min, schema_id, event_descriptor)
            .insert_if_not_exists(
                &self.cassandra_provider,
                &self.cassandra_provider.app_keyspace,
            )
            .await
    }

    async fn event_descriptors_by_topic_id(
        &self,
        topic_id: &str,
        min_descriptor_version: Option<u64>,
    ) -> Vec<String> {
        EventDescriptorEntity::select_by_topic_id(
            &self.cassandra_provider,
            &self.cassandra_provider.app_keyspace,
            topic_id,
            min_descriptor_version,
        )
        .await
        .iter()
        .map(EventDescriptorEntity::get_event_descriptor)
        .map(|value| value.to_owned())
        .collect::<Vec<_>>()
    }

    async fn extraction_setup_searchable(
        &self,
        topic_id: &str,
        name_and_type_slice: &[(String, String)],
    ) {
        let keyspace_name = &self.cassandra_provider.get_keyspace_from_topic(topic_id);
        // Get a list of existing columns
        let column_names = self
            .cassandra_provider
            .get_column_names(keyspace_name, EventEntity::CQL_TABLE_NAME)
            .await;
        // Get a list of existing indexes
        let index_names = self
            .cassandra_provider
            .get_index_names(keyspace_name, EventEntity::CQL_TABLE_NAME)
            .await;
        // Filter out those that don't have the right prefix
        let filtered_columns = column_names
            .into_iter()
            .filter(|column_name| column_name.starts_with(EventEntity::EXTRACTED_COLUMN_PREFIX))
            .collect::<Vec<_>>();
        for (result_name, result_type) in name_and_type_slice {
            let supposed_column_name =
                EventEntity::EXTRACTED_COLUMN_PREFIX.to_owned() + result_name;
            if !filtered_columns.contains(&supposed_column_name) {
                // Create column that does not exist
                self.cassandra_provider
                    .add_column(
                        keyspace_name,
                        EventEntity::CQL_TABLE_NAME,
                        &supposed_column_name,
                        result_type,
                    )
                    .await;
            }
            let supposed_index_name =
                EventEntity::CQL_TABLE_NAME.to_owned() + "_by_" + &supposed_column_name;
            if !index_names.contains(&supposed_index_name) {
                // Create StorageAttachedIndex on column
                self.cassandra_provider
                    .add_index(
                        keyspace_name,
                        EventEntity::CQL_TABLE_NAME,
                        &supposed_column_name,
                        &supposed_index_name,
                    )
                    .await;
            }
        }
    }
}
