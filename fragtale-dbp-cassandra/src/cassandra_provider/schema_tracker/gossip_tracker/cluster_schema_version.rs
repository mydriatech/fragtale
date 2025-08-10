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

//! Cassandra cluster schema version.

use crate::cassandra_provider::cassandra_schema::CassandraSchema;
use crate::cassandra_provider::cassandra_session::CassandraSession;
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;
use uuid::Uuid;

/// Query database for version schema that is active on all nodes.
pub struct ClusterSchemaVersion {
    schema_version_by_host_id: SkipMap<Uuid, Uuid>,
}

impl std::fmt::Display for ClusterSchemaVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        for entry in self.schema_version_by_host_id.iter() {
            write!(
                f,
                "(host_id: {}, schema_version: {}),",
                entry.key(),
                entry.value()
            )?;
        }
        write!(f, "]")?;
        Ok(())
    }
}

impl ClusterSchemaVersion {
    /// Return a snapshot with the state of the database schema version on all
    /// cluster nodes.
    pub async fn new_snapshot(cs: &Arc<CassandraSession>) -> Self {
        let schema_version_by_host_id = SkipMap::default();
        // Read local (if there is only one node)
        let (host_id, schema_version) = CassandraSchema::get_host_id_and_schema_version_local(cs)
            .await
            .unwrap();
        schema_version_by_host_id.insert(host_id, schema_version);
        // Read peers twice (if there is round robin LB among nodes)
        for _ in 0..2 {
            CassandraSchema::get_host_id_and_schema_versions_of_peers(cs)
                .await
                .into_iter()
                .for_each(|(host_id, schema_version)| {
                    schema_version_by_host_id.insert(host_id, schema_version);
                });
        }
        Self {
            schema_version_by_host_id,
        }
    }

    /// Return the number of cluster nodes that was detected in this snapshot.
    pub fn get_node_count(&self) -> usize {
        self.schema_version_by_host_id.len()
    }

    /// Returns a common database schema version Uuid if there is consensus.
    pub fn get_stable_schema_version(&self) -> Option<Uuid> {
        let mut seen = None;
        if self
            .schema_version_by_host_id
            .iter()
            .map(|entry| entry.value().to_owned())
            .filter(|value| {
                if seen.is_none() {
                    seen = Some(*value);
                    true
                } else {
                    seen.is_some_and(|seen| !seen.eq(value))
                }
            })
            .count()
            == 1
        {
            seen
        } else {
            None
        }
    }
}
