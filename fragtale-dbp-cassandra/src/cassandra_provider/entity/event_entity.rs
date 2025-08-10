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

//! Event entity and persistence.

use super::FromSignedOrDefault;
use crate::CassandraProvider;
use crate::CassandraResultMapper;
use cdrs_tokio::query::QueryValues;
use cdrs_tokio::types::prelude::Value;
use fragtale_dbp::mb::ExtractedValue;
use fragtale_dbp::mb::TopicEvent;
use fragtale_dbp::mb::UniqueTime;
use fragtale_dbp::mb::consumers::EventDeliveryGist;
use std::collections::HashMap;

/// Event entity and persistence.
#[derive(
    Clone, Debug, cdrs_tokio::IntoCdrsValue, cdrs_tokio::TryFromRow, cdrs_tokio::TryFromUdt,
)]
pub struct EventEntity {
    /// The event document fingerprint.
    event_id: String,
    /// Clusterwide unique timestamp of when the event was recieved.
    ///
    /// Multiple inserts of the exact same content (and hence `event_id`)
    /// will still be delivered as separate events thanks to this descriminator.
    unique_time: i64,
    /// The event document.
    document: String,
    /// The event level integrity protection reference.
    protection_ref: String,
    /// Unique identifier that clients can propagate through the system.
    correlation_token: String,
}

impl From<&TopicEvent> for EventEntity {
    fn from(value: &TopicEvent) -> Self {
        Self::new(
            value.get_event_id(),
            value.get_unique_time(),
            value.get_document(),
            value.get_protection_ref(),
            value.get_correlation_token(),
        )
    }
}

impl EventEntity {
    pub(crate) const CQL_TABLE_NAME: &'static str = "event";
    /// Prefix of database column name where the extracted value is stored.
    pub const EXTRACTED_COLUMN_PREFIX: &'static str = "doc_";

    const CQL_TEMPLATE_CREATE_TABLE: &'static str = "
        CREATE TABLE IF NOT EXISTS event (
            event_id            text,
            unique_time         bigint,
            document            text,
            protection_ref      text,
            correlation_token   text,
            PRIMARY KEY ((event_id), unique_time)
        ) WITH CLUSTERING ORDER BY (unique_time DESC);
        ";

    /// QE1. Persist new event
    const CQL_TEMPLATE_INSERT: &'static str = "
        INSERT INTO event
        (event_id, unique_time, document, protection_ref, correlation_token {{ column_names }})
        VALUES (?,?,?,?,? {{ column_placeholders }})
        ;";

    /// QE2. Get full entities by event (document) identifier.
    const CQL_TEMPLATE_SELECT: &'static str = "
        SELECT event_id, unique_time, document, protection_ref, correlation_token
        FROM event
        WHERE event_id=?
        LIMIT {{ limit }}
        ";

    /// QE3. Get full entity by event (document) identifier and UniqueTime.
    const CQL_TEMPLATE_SELECT_BY_ID_AND_UNIQUE: &'static str = "
        SELECT event_id, unique_time, document, protection_ref, correlation_token
        FROM event
        WHERE event_id = ? AND unique_time = ?
        ";

    /// QE4. Get full entity by correlation token.
    const CQL_TEMPLATE_SELECT_BY_CID: &'static str = "
        SELECT event_id, unique_time, document, protection_ref, correlation_token
        FROM event
        WHERE correlation_token=?
        ";

    /// QE5. Get full entity by indexed column. (Columns might vary for each topic.)
    const CQL_TEMPLATE_SELECT_IDS_BY_COLUMN: &'static str = "
        SELECT event_id, unique_time
        FROM event
        WHERE {{ column_name }} = ?
        LIMIT {{ limit }}
        ";

    /// Return a new instance.
    pub fn new(
        event_id: &str,
        unique_time: UniqueTime,
        document: &str,
        protection_ref: &str,
        correlation_token: &str,
    ) -> Self {
        Self {
            event_id: event_id.to_owned(),
            unique_time: i64::from(unique_time),
            document: document.to_owned(),
            protection_ref: protection_ref.to_owned(),
            correlation_token: correlation_token.to_owned(),
        }
    }

    /// Return the event document fingerprint.
    pub fn get_event_id(&self) -> &str {
        &self.event_id
    }

    /// Return the encoded clusterwide unique timestamp of when the event
    /// happened.
    pub fn get_unique_time(&self) -> u64 {
        u64::from_signed(self.unique_time)
    }

    /// Return the event document.
    pub fn get_document(&self) -> &str {
        &self.document
    }

    /// Return the event level integrity protection reference.
    pub fn get_protection_ref(&self) -> &str {
        &self.protection_ref
    }

    /// Return the unique identifier that clients can propagate through the
    /// system to correlate events from different topics.
    pub fn get_correlation_token(&self) -> &str {
        &self.correlation_token
    }

    /// Consume this instance into parts for delivery.
    pub fn into_event_delivery_gist(self) -> EventDeliveryGist {
        EventDeliveryGist::new(
            UniqueTime::from(u64::from_signed(self.unique_time)),
            self.document,
            self.protection_ref,
            self.correlation_token,
        )
    }

    /// Create a new table and indices.
    pub async fn create_table_and_indices(db: &CassandraProvider, topic_id: &str) {
        let keyspace = &db.get_keyspace_from_topic(topic_id);
        db.create_table(
            keyspace,
            Self::CQL_TABLE_NAME,
            Self::CQL_TEMPLATE_CREATE_TABLE,
        )
        .await;
        db.add_index(
            keyspace,
            Self::CQL_TABLE_NAME,
            "correlation_token",
            "event_by_correlation_token",
        )
        .await;
    }

    /// Insert the entity (unconditional).
    pub async fn insert(
        &self,
        db: &CassandraProvider,
        topic_id: &str,
        additional_columns: HashMap<String, ExtractedValue>,
    ) -> bool {
        let mut simple_values = vec![
            Value::from(self.event_id.to_owned()),
            Value::from(self.unique_time),
            Value::from(self.document.to_owned()),
            Value::from(self.protection_ref.to_owned()),
            Value::from(self.correlation_token.to_owned()),
        ];
        let mut column_names = String::new();
        let mut column_placeholders = String::new();
        for (key, value) in additional_columns {
            column_names = column_names + ", " + Self::EXTRACTED_COLUMN_PREFIX + &key;
            column_placeholders += ",?";
            match value {
                ExtractedValue::Text(value) => {
                    simple_values.push(cdrs_tokio::types::prelude::Value::from(value));
                }
                ExtractedValue::BigInt(value) => {
                    simple_values.push(cdrs_tokio::types::prelude::Value::from(value));
                }
            }
        }
        let query_values = QueryValues::SimpleValues(simple_values);
        if log::log_enabled!(log::Level::Trace) {
            log::trace!("query_values: {query_values:?}")
        }
        let query_template = Self::CQL_TEMPLATE_INSERT
            .replace("{{ column_names }}", &column_names)
            .replace("{{ column_placeholders }}", &column_placeholders);
        db.query_with_keyspace_and_values(
            &query_template,
            &db.get_keyspace_from_topic(topic_id),
            query_values,
        )
        .await
        .map(CassandraResultMapper::into_applied)
        .unwrap_or(false)
    }

    /// Return all event entities for a event document identifier.
    ///
    /// The largest UniqueTime (newest) is returned first.
    pub async fn select_by_event_id(
        db: &CassandraProvider,
        topic_id: &str,
        event_id: &str,
        max_results: usize,
    ) -> Vec<Self> {
        let keyspace = &db.get_keyspace_from_topic(topic_id);
        let values = cdrs_tokio::query_values!("event_id" => event_id.to_owned());
        db.query_with_keyspace_and_values(
            &Self::CQL_TEMPLATE_SELECT.replacen("{{ limit }}", &max_results.to_string(), 1),
            keyspace,
            values,
        )
        .await
        .map(CassandraResultMapper::into_entities)
        .unwrap_or_default()
    }

    /// Return an event entity for a event document identifier and a specific
    /// UniqueTime.
    pub async fn select_by_event_id_and_unique_time(
        db: &CassandraProvider,
        topic_id: &str,
        event_id: &str,
        unique_time: UniqueTime,
    ) -> Option<Self> {
        let keyspace = &db.get_keyspace_from_topic(topic_id);
        let values = cdrs_tokio::query_values!(
            "event_id" => event_id.to_owned(),
            "unique_time" => unique_time.as_encoded_i64()
        );
        db.query_with_keyspace_and_values(
            Self::CQL_TEMPLATE_SELECT_BY_ID_AND_UNIQUE,
            keyspace,
            values,
        )
        .await
        .map(CassandraResultMapper::into_entities)
        .unwrap_or_default()
        .first()
        .cloned()
    }

    /// Return event by correlation token.
    pub async fn select_by_correlation_token(
        db: &CassandraProvider,
        topic_id: &str,
        correlation_token: &str,
    ) -> Option<Self> {
        let keyspace = &db.get_keyspace_from_topic(topic_id);
        let values = cdrs_tokio::query_values!("correlation_token" => correlation_token.to_owned());
        db.query_with_keyspace_and_values(Self::CQL_TEMPLATE_SELECT_BY_CID, keyspace, values)
            .await
            .map(CassandraResultMapper::into_entities)
            .unwrap_or_default()
            .first()
            .cloned()
    }

    /// Return event document identifiers by index key.
    ///
    /// The results are sorted by Cassandra token order which is stable, but
    /// the order depends on how the Cassandra cluster is setup.
    pub async fn select_ids_and_unique_time_by_index(
        db: &CassandraProvider,
        topic_id: &str,
        index_column: &str,
        index_key: &str,
        max_results: usize,
    ) -> Vec<(String, u64)> {
        let keyspace = &db.get_keyspace_from_topic(topic_id);
        let values = cdrs_tokio::query_values!(index_key.to_owned());
        let query_template = Self::CQL_TEMPLATE_SELECT_IDS_BY_COLUMN
            .replacen("{{ column_name }}", index_column, 1)
            .replacen("{{ limit }}", &max_results.to_string(), 1);
        db.query_with_keyspace_and_values(&query_template, keyspace, values)
            .await
            .map(CassandraResultMapper::into_string_u64_tuplet_vec)
            .unwrap_or_default()
    }
}
