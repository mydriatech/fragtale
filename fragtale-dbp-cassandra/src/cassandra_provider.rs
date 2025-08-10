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

//! Cassandra implementation of [DatabaseProvider].

mod cassandra_facades;
mod cassandra_result_mapper;
mod cassandra_schema;
mod cassandra_session;
mod entity;
mod schema_tracker;

use self::cassandra_facades::CassandraProviderFacades;
pub use self::cassandra_result_mapper::CassandraResultMapper;
use self::cassandra_session::CassandraSession;
use self::entity::*;
use self::schema_tracker::SchemaTracker;
use cassandra_schema::CassandraSchema;
use cdrs_tokio::frame::message_response::ResponseBody;
use cdrs_tokio::query::QueryValues;
use crossbeam_skiplist::SkipSet;
use entity::IntegrityByLevelAndTimeEntity;
use entity::IntegrityByLevelAndTimeLookupEntity;
use entity::TopicEntity;
use fragtale_dbp::dbp::DatabaseProvider;
use std::sync::Arc;
use tokio::time::{Duration, sleep};

/// Cassandra [DatabaseProvider] implementation.
pub struct CassandraProvider {
    /// Application keyspace and topic prefix
    app_keyspace: String,
    /// Connection to Cassandra.
    cs: Arc<CassandraSession>,
    /// Tracks schema changes
    schema_tracker: Arc<SchemaTracker>,
    /// Cache of topic existance.
    topic_exists_check: SkipSet<String>,
    /// Replication factor (copies of the same data)
    replication_factor: usize,
}

impl CassandraProvider {
    /// Return a new instance.
    pub async fn new(
        app_keyspace: &str,
        endpoints: &[String],
        username: &str,
        password: &str,
        replication_factor: usize,
    ) -> Arc<Self> {
        let cs = CassandraSession::connect(endpoints, username, password, replication_factor).await;
        let schema_tracker = SchemaTracker::new(&cs).await;
        cs.attach_schema_change_listener(&schema_tracker.as_schema_change_listener());
        Arc::new(Self {
            app_keyspace: app_keyspace.to_owned(),
            cs,
            schema_tracker,
            topic_exists_check: SkipSet::default(),
            replication_factor,
        })
        .init()
        .await
    }

    /// Initialize
    async fn init(self: Arc<Self>) -> Arc<Self> {
        self.ensure_keyspace_exists(&self.app_keyspace).await;
        self.ensure_app_tables_exists().await;
        self
    }

    /// Get [DatabaseProvider] instance.
    pub fn as_database_provider(self: &Arc<Self>) -> DatabaseProvider {
        DatabaseProvider::new(Arc::new(CassandraProviderFacades::new(self)))
    }

    /// Return true when the keyspace already existed
    async fn ensure_keyspace_exists(&self, keyspace: &str) -> bool {
        if self.schema_tracker.get_keyspace_exists(keyspace).await {
            true
        } else {
            let (_schema_version, _node_count) =
                self.schema_tracker.wait_for_stable_schema_version().await;
            if self.schema_tracker.get_keyspace_exists(keyspace).await {
                true
            } else {
                CassandraSchema::create_keyspace(&self.cs, keyspace, self.replication_factor).await;
                // Wait for server event to report that keyspace now exists
                while !self.schema_tracker.get_keyspace_exists(keyspace).await {
                    sleep(Duration::from_millis(100)).await;
                }
                // Wait for gossip to settle
                let (schema_version, _node_count) =
                    self.schema_tracker.wait_for_stable_schema_version().await;
                if log::log_enabled!(log::Level::Trace) {
                    log::trace!(
                        "Keyspace '{keyspace}' exists in schema_version '{schema_version}'."
                    );
                }
                false
            }
        }
    }

    /// Return all the column names of a table.
    pub async fn get_column_names(&self, keyspace_name: &str, table_name: &str) -> Vec<String> {
        CassandraSchema::column_names_by_keyspace_and_table(&self.cs, keyspace_name, table_name)
            .await
    }

    /// Return all the index names of a table.
    pub async fn get_index_names(&self, keyspace_name: &str, table_name: &str) -> Vec<String> {
        CassandraSchema::index_names_by_keyspace_and_table(&self.cs, keyspace_name, table_name)
            .await
    }

    /// Add a column to a table.
    pub async fn add_column(
        &self,
        keyspace: &str,
        table_name: &str,
        column_name: &str,
        cql_type: &str,
    ) {
        CassandraSchema::alter_table_add_column(
            &self.cs,
            keyspace,
            table_name,
            column_name,
            cql_type,
        )
        .await;
        let (schema_version, _node_count) =
            self.schema_tracker.wait_for_stable_schema_version().await;
        if log::log_enabled!(log::Level::Debug) {
            log::debug!(
                "Column {keyspace}.{table_name}.{column_name} of type '{cql_type}' now exist in schema_version '{schema_version}'"
            );
        }
    }

    /// Add a index to a table.
    pub async fn add_index(
        &self,
        keyspace: &str,
        table_name: &str,
        column_name: &str,
        index_name: &str,
    ) {
        self.schema_tracker.wait_for_stable_schema_version().await;
        if !self
            .schema_tracker
            .get_index_exists(keyspace, table_name, index_name)
            .await
        {
            CassandraSchema::alter_table_add_index(
                &self.cs,
                keyspace,
                table_name,
                column_name,
                index_name,
            )
            .await;
            // Wait for server event to report that table now exists
            while !self
                .schema_tracker
                .get_index_exists(keyspace, table_name, index_name)
                .await
            {
                sleep(Duration::from_millis(100)).await;
            }
            let (schema_version, _node_count) =
                self.schema_tracker.wait_for_stable_schema_version().await;
            if log::log_enabled!(log::Level::Debug) {
                log::debug!(
                    "Index '{index_name}' for '{keyspace}.{table_name}.{column_name}' now exist in schema_version '{schema_version}'"
                );
            }
        }
    }

    /// Create a new database table in the namespace.
    pub async fn create_table(&self, keyspace: &str, table_name: &str, query_template: &str) {
        self.schema_tracker.wait_for_stable_schema_version().await;
        if !self
            .schema_tracker
            .get_table_exists(keyspace, table_name)
            .await
        {
            self.query_with_keyspace(query_template, keyspace).await;
            // Wait for server event to report that table now exists
            while !self
                .schema_tracker
                .get_table_exists(keyspace, table_name)
                .await
            {
                sleep(Duration::from_millis(100)).await;
            }
            let (schema_version, _node_count) =
                self.schema_tracker.wait_for_stable_schema_version().await;
            if log::log_enabled!(log::Level::Debug) {
                log::debug!(
                    "Table '{table_name}' in keyspace '{keyspace}' exist in schema_version '{schema_version}'."
                );
            }
        }
    }

    /// Ensure that all the application level tables exist in the application's
    /// keyspace.
    ///
    /// This will create the application level tables if needed.
    async fn ensure_app_tables_exists(&self) {
        IdentityClaimEntity::create_table_and_indices(self).await;
        ResourceGrantEntity::create_table_and_indices(self).await;
        EventDescriptorEntity::create_table_and_indices(self).await;
        TopicEntity::create_table_and_indices(self).await;
        let schema_version = self.schema_tracker.wait_for_stable_schema_version().await;
        if log::log_enabled!(log::Level::Trace) {
            log::trace!("App tables exist in schema_version '{schema_version:?}'.");
        }
    }

    async fn is_table_absent_in_unstable_tracking(&self, keyspace: &str, table_name: &str) -> bool {
        !self
            .schema_tracker
            .get_table_exists(keyspace, table_name)
            .await
    }

    /// Ensure that all the topic level tables exist in the application's
    /// keyspace.
    ///
    /// This will create the topic level tables if needed.
    async fn ensure_topic_exists_internal(&self, topic_id: &str) {
        if self.topic_exists_check.contains(topic_id) {
            return;
        }
        let topic_keyspace = self.get_keyspace_from_topic(topic_id);
        let mut all_ok = self.ensure_keyspace_exists(&topic_keyspace).await;
        let topic_table_names = [
            ObjectCountEntity::CQL_TABLE_NAME,
            ConsumerEntity::CQL_TABLE_NAME,
            DeliveryIntentEntity::CQL_TABLE_NAME,
            EventEntity::CQL_TABLE_NAME,
            EventIdByUniqueTimeEntity::CQL_TABLE_NAME,
            IntegrityByLevelAndTimeLookupEntity::CQL_TABLE_NAME,
            IntegrityByLevelAndTimeEntity::CQL_TABLE_NAME,
            IntegrityEntity::CQL_TABLE_NAME,
            UniqueTimeBucketByShelfEntity::CQL_TABLE_NAME,
        ];
        for table_name in topic_table_names {
            all_ok &= self
                .is_table_absent_in_unstable_tracking(&topic_keyspace, table_name)
                .await;
        }
        if !all_ok {
            ObjectCountEntity::create_table_and_indices(self, topic_id).await;
            ConsumerEntity::create_table_and_indices(self, topic_id).await;
            DeliveryIntentEntity::create_table_and_indices(self, topic_id).await;
            EventEntity::create_table_and_indices(self, topic_id).await;
            EventIdByUniqueTimeEntity::create_table_and_indices(self, topic_id).await;
            IntegrityByLevelAndTimeLookupEntity::create_table_and_indices(self, topic_id).await;
            IntegrityByLevelAndTimeEntity::create_table_and_indices(self, topic_id).await;
            IntegrityEntity::create_table_and_indices(self, topic_id).await;
            UniqueTimeBucketByShelfEntity::create_table_and_indices(self, topic_id).await;
            // Mark the topic as existing
            TopicEntity::new(topic_id)
                .insert(self, &self.app_keyspace)
                .await;
        }
        self.topic_exists_check.insert(topic_id.to_owned());
    }

    /// Execute a keyspaced query.
    async fn query_with_keyspace(
        &self,
        query_template: &str,
        keyspace: &str,
    ) -> Option<ResponseBody> {
        self.query_with_keyspace_and_values(query_template, keyspace, cdrs_tokio::query_values!())
            .await
    }

    /// Execute a keyspaced query with value parameters.
    async fn query_with_keyspace_and_values(
        &self,
        query_template: &str,
        keyspace: &str,
        values: QueryValues,
    ) -> Option<ResponseBody> {
        self.cs
            .query_with_keyspace_and_values(query_template, keyspace, values)
            .await
    }

    /// Return the topic's keyspace using the application keyspace as prefix.
    pub fn get_keyspace_from_topic(&self, topic_id: &str) -> arrayvec::ArrayString<48> {
        // Keyspace names can have up to 48 alpha-numeric characters and contain underscores
        let mut string = arrayvec::ArrayString::<48>::new();
        string.push_str(&self.app_keyspace);
        string.push('_');
        string.push_str(topic_id);
        string
    }
}
