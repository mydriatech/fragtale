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

//! Tracking of existing keyspaces, tables and indices.

mod gossip_tracker;

use self::gossip_tracker::GossipTracker;
use super::cassandra_schema::CassandraSchema;
use super::cassandra_session::CassandraSchemaChangeListener;
use super::cassandra_session::CassandraSession;
use cdrs_tokio::frame::events::SchemaChange;
use cdrs_tokio::frame::events::SchemaChangeOptions;
use cdrs_tokio::frame::events::SchemaChangeTarget;
use cdrs_tokio::frame::events::SchemaChangeType;
use crossbeam_skiplist::SkipSet;
use std::sync::Arc;

/// Tracks of existing keyspaces, tables and indices.
pub struct SchemaTracker {
    cs: Arc<CassandraSession>,
    gossip_tracker: Arc<GossipTracker>,
    keyspaces: SkipSet<String>,
    tables: SkipSet<String>,
    indexes: SkipSet<String>,
}

impl CassandraSchemaChangeListener for SchemaTracker {
    fn handle_schema_change(&self, schema_change: &SchemaChange) {
        match schema_change.target {
            SchemaChangeTarget::Keyspace => {
                if let SchemaChangeOptions::Keyspace(keyspace) = &schema_change.options {
                    match schema_change.change_type {
                        SchemaChangeType::Created => {
                            self.keyspaces.insert(keyspace.to_owned());
                        }
                        SchemaChangeType::Updated => {
                            self.keyspaces.insert(keyspace.to_owned());
                        }
                        SchemaChangeType::Dropped => {
                            self.keyspaces.remove(keyspace);
                        }
                        _ => {
                            log::debug!("Unsupported server event received: {schema_change:?}");
                        }
                    }
                } else {
                    log::debug!(
                        "Unsupported schema change options: {:?}",
                        schema_change.options
                    );
                }
            }
            SchemaChangeTarget::Table => {
                if let SchemaChangeOptions::TableType(keyspace, table_name) = &schema_change.options
                {
                    let keyspace_dot_table_name = keyspace.to_owned() + "." + table_name;
                    match schema_change.change_type {
                        SchemaChangeType::Created => {
                            self.tables.insert(keyspace_dot_table_name);
                        }
                        SchemaChangeType::Updated => {
                            self.tables.insert(keyspace_dot_table_name);
                        }
                        SchemaChangeType::Dropped => {
                            self.tables.remove(&keyspace_dot_table_name);
                        }
                        _ => {
                            log::debug!("Unsupported server event received: {schema_change:?}");
                        }
                    }
                } else {
                    log::debug!(
                        "Unsupported schema change options: {:?}",
                        schema_change.options
                    );
                }
            }
            _ => {
                log::debug!(
                    "Unsupported schema change target: {:?}",
                    schema_change.target
                );
            }
        }
    }
}

impl SchemaTracker {
    pub async fn new(cs: &Arc<CassandraSession>) -> Arc<Self> {
        Arc::new(Self {
            cs: Arc::clone(cs),
            gossip_tracker: GossipTracker::new(cs).await,
            keyspaces: SkipSet::new(),
            tables: SkipSet::new(),
            indexes: SkipSet::new(),
        })
    }

    pub fn as_schema_change_listener(self: &Arc<Self>) -> Arc<dyn CassandraSchemaChangeListener> {
        Arc::clone(self) as Arc<dyn CassandraSchemaChangeListener>
    }

    pub async fn get_index_exists(
        &self,
        keyspace: &str,
        table_name: &str,
        index_name: &str,
    ) -> bool {
        let keyspace_dot_table_name_dot_index =
            keyspace.to_owned() + "." + table_name + "." + index_name;
        if self.indexes.contains(&keyspace_dot_table_name_dot_index) {
            return true;
        }
        let res = CassandraSchema::keyspace_table_index_exists(
            &self.cs, keyspace, table_name, index_name,
        )
        .await;
        if res {
            self.indexes.insert(keyspace_dot_table_name_dot_index);
        } else {
            self.indexes.remove(&keyspace_dot_table_name_dot_index);
        }
        res
    }

    pub async fn get_keyspace_exists(&self, keyspace: &str) -> bool {
        if self.keyspaces.contains(keyspace) {
            return true;
        }
        let ret = CassandraSchema::keyspace_exists(&self.cs, keyspace).await;
        if ret {
            self.keyspaces.insert(keyspace.to_owned());
        }
        ret
    }

    pub async fn get_table_exists(&self, keyspace: &str, table_name: &str) -> bool {
        let keyspace_dot_table_name = keyspace.to_owned() + "." + table_name;
        if self.tables.contains(&keyspace_dot_table_name) {
            return true;
        }
        let ret = CassandraSchema::keyspace_table_exists(&self.cs, keyspace, table_name).await;
        if ret {
            self.tables.insert(keyspace_dot_table_name);
        }
        ret
    }

    pub async fn wait_for_stable_schema_version(&self) -> (String, usize) {
        let (uuid, node_count) = self.gossip_tracker.wait_for_stable_schema_version().await;
        (uuid.to_string(), node_count)
    }
}
