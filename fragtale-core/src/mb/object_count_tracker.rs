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

//! Maintain count of objects of a different types in the cluster.

mod per_topic_tracker;

use self::per_topic_tracker::PerTopicTracker;
use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::map::Entry;
use fragtale_dbp::dbp::DatabaseProvider;
use fragtale_dbp::dbp::facades::DatabaseProviderFacades;
use fragtale_dbp::mb::ObjectCount;
use fragtale_dbp::mb::ObjectCountType;
use std::sync::Arc;
use tokio::time::Duration;
use tokio::time::sleep;

/// Track number of events of different types in topics to detect changes.
///
/// This is an optmization to avoid having to query the DB for the actual objects.
pub struct ObjectCountTracker {
    // Database provider.
    dbp: Arc<DatabaseProvider>,
    // Local instance id
    instance_id: u16,
    // Per topic counts and tracking
    per_topic_tracker: SkipMap<String, Arc<PerTopicTracker>>,
}

impl ObjectCountTracker {
    /// Return a new instance.
    pub async fn new(dbp: &Arc<DatabaseProvider>, instance_id: u16) -> Arc<Self> {
        Arc::new(Self {
            dbp: Arc::clone(dbp),
            instance_id,
            per_topic_tracker: SkipMap::new(),
        })
        .initialize()
        .await
    }

    /// Kick off background tasks.
    async fn initialize(self: Arc<Self>) -> Arc<Self> {
        let self_clone = Arc::clone(&self);
        tokio::spawn(async move {
            // Persist all local values at regular intervals (if there is a change)
            loop {
                self_clone.persist_changed_local_counts().await;
                sleep(Duration::from_micros(100_000)).await
            }
        });
        let self_clone = Arc::clone(&self);
        tokio::spawn(async move {
            // Detect changes in object counts for all topics and signal any awaiter.
            loop {
                self_clone.detect_changes().await;
                sleep(Duration::from_micros(100_000)).await
            }
        });
        self
    }

    fn tracking_by_topic(&self, topic_id: &str) -> Arc<PerTopicTracker> {
        // Avoid cloing `topic_id` if map entry already exists.
        self.per_topic_tracker
            .get(topic_id)
            .as_ref()
            .map(Entry::value)
            .map(Arc::clone)
            .unwrap_or_else(|| {
                Arc::clone(
                    self.per_topic_tracker
                        .get_or_insert_with(topic_id.to_owned(), PerTopicTracker::new)
                        .value(),
                )
            })
    }

    pub fn inc(&self, topic_id: &str, object_count_type: &ObjectCountType) {
        let old = self
            .tracking_by_topic(topic_id)
            .local_count_by_type(object_count_type)
            .inc_current();
        if log::log_enabled!(log::Level::Trace) {
            log::trace!("After increase of {object_count_type:?} old is {old}.");
        }
        // Shortcut for changes in local count to avoid db roundtrip
        self.tracking_by_topic(topic_id)
            .awaiter_remove_and_signal(object_count_type);
    }

    /// Persist all local values (if there is a change)
    async fn persist_changed_local_counts(&self) {
        for entry in self.per_topic_tracker.iter() {
            let topic_id = entry.key();
            for entry in entry.value().local_counts() {
                let object_count_type = entry.key();
                let object_count = entry.value().as_ref();
                let current = object_count.get_current();
                let persisted = object_count.get_persisted();
                if current != persisted {
                    if log::log_enabled!(log::Level::Trace) {
                        log::trace!(
                            "After increase of {topic_id} {object_count_type:?} current: {current}, persisted: {persisted}."
                        );
                    }
                    // Persist and update local counter
                    self.dbp
                        .event_tracking_facade()
                        .object_count_insert(topic_id, object_count_type, self.instance_id, current)
                        .await;
                    object_count.set_persisted(current);
                }
            }
        }
    }

    /// Detect changes in object counts and signal any awaiter.
    async fn detect_changes(&self) {
        // for each topic with waiters
        for per_topic_entry in self.per_topic_tracker.iter() {
            let topic_id = per_topic_entry.key();
            // for each type of count with waiters
            for entry in per_topic_entry.value().awaiters() {
                let object_count_type = entry.key();
                // Get the count for every instance from DB
                let object_counts = self
                    .dbp
                    .event_tracking_facade()
                    .object_count_by_topic_and_type(topic_id, object_count_type)
                    .await;
                // compare found counts with the local counts while updating them -> save diff
                let per_instance_count = per_topic_entry
                    .value()
                    .per_instance_counts_by_type(object_count_type);
                let mut changed = false;
                for object_count in object_counts {
                    let change = per_instance_count.upsert(
                        object_count.get_instance_id(),
                        object_count.get_object_count(),
                    );
                    if change && log::log_enabled!(log::Level::Debug) {
                        log::debug!(
                            "Change detected for '{topic_id}' '{object_count_type:?}': {}",
                            object_count.get_object_count()
                        );
                    }
                    changed |= change;
                }
                if changed {
                    // notify awaiter (registered for topic and type)
                    if entry.remove() {
                        entry.value().signal();
                    }
                }
            }
        }
    }

    /// Return the total number of objects of a specififc type for a topic.
    pub async fn get_total_object_count(
        &self,
        topic_id: &str,
        object_count_type: &ObjectCountType,
    ) -> u64 {
        self.dbp
            .event_tracking_facade()
            .object_count_by_topic_and_type(topic_id, object_count_type)
            .await
            .iter()
            .map(ObjectCount::get_object_count)
            .sum()
    }

    /// Wait for changes in the number of object of a specific types in a topic.
    pub async fn await_change(
        &self,
        topic_id: &str,
        object_count_type: &ObjectCountType,
        max_wait_micros: u64,
    ) {
        if let Err(_e) = tokio::time::timeout(
            Duration::from_micros(max_wait_micros),
            self.tracking_by_topic(topic_id)
                .awaiter_by_type(object_count_type)
                .wait_for_signal(),
        )
        .await
            && log::log_enabled!(log::Level::Trace)
        {
            log::trace!(
                "No object count change of {object_count_type:?} in topic {topic_id} before timeout of {max_wait_micros} micros."
            );
        }
    }
}
