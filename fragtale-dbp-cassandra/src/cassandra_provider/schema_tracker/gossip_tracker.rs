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

//! Tracking of Cassandra cluster schema version.

mod cluster_schema_version;

use self::cluster_schema_version::ClusterSchemaVersion;
use crate::cassandra_provider::cassandra_session::CassandraSession;
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use tokio::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

/// Track Cassandra schema version in all cluster nodes to detect when all nodes
/// are in sync.
///
/// Since Cassandra uses the gossip protocol, a consistent schema version
/// across nodes is a good indication that a schema changes has propagated
/// through the cluster.
///
/// To prevent that multiple callers try to query the database for the same
/// information, a background job handles this and the callers can await the
/// result.
pub struct GossipTracker {
    cs: Arc<CassandraSession>,
    awaiting_count: Arc<AtomicUsize>,
    stable_schema_version: SkipMap<(), (Uuid, usize)>,
}

impl GossipTracker {
    /// Return a new instance.
    pub async fn new(cs: &Arc<CassandraSession>) -> Arc<Self> {
        Arc::new(Self {
            cs: Arc::clone(cs),
            awaiting_count: Arc::default(),
            stable_schema_version: SkipMap::default(),
        })
        .init()
        .await
    }

    /// Initialize background task(s).
    async fn init(self: Arc<Self>) -> Arc<Self> {
        let self_clone = Arc::clone(&self);
        tokio::spawn(async move { self_clone.detect_stable_schema_version().await });
        self
    }

    /// Background tasks that updates schema version while there are awaiters
    async fn detect_stable_schema_version(&self) {
        loop {
            sleep(Duration::from_millis(125)).await;
            if self.awaiting_count.load(Ordering::Relaxed) > 0 {
                let cluster_schema_version = ClusterSchemaVersion::new_snapshot(&self.cs).await;
                if let Some(uuid) = cluster_schema_version.get_stable_schema_version() {
                    self.stable_schema_version
                        .insert((), (uuid, cluster_schema_version.get_node_count()));
                } else {
                    self.stable_schema_version.clear();
                    if log::log_enabled!(log::Level::Trace) {
                        log::trace!("cluster_schema_version: {cluster_schema_version}");
                    }
                }
            } else {
                self.stable_schema_version.clear();
                sleep(Duration::from_millis(125)).await;
            }
        }
    }

    /// Wait for all database cluster nodes to have a consistent schema version.
    ///
    /// This will wait forever if the cluster can't agree on schema version.
    pub async fn wait_for_stable_schema_version(&self) -> (Uuid, usize) {
        self.awaiting_count.fetch_add(1, Ordering::Relaxed);
        let mut wait_counter = 0u64;
        loop {
            if let Some((uuid, node_count)) = self
                .stable_schema_version
                .front()
                .map(|entry| entry.value().to_owned())
            {
                self.awaiting_count.fetch_sub(1, Ordering::Relaxed);
                return (uuid, node_count);
            }
            wait_counter += 1;
            if wait_counter % (8 * 20) == 0 {
                log::info!("Still waiting for schema gossip to settle...");
            }
            sleep(Duration::from_millis(125)).await;
        }
    }
}
