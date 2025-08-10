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

//! Cached and versioned event descriptors for topics.

mod per_topic_event_descriptor;

use self::per_topic_event_descriptor::PerTopicEventDescriptor;
use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::map::Entry;
use fragtale_client::mb::event_descriptor::DescriptorVersion;
use fragtale_client::mb::event_descriptor::EventDescriptor;
use fragtale_dbp::dbp::DatabaseProvider;
use fragtale_dbp::dbp::facades::DatabaseProviderFacades;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::sync::Semaphore;
use tokio::time::Duration;
use tokio::time::sleep;

/// Maintains an event descriptor cache for each topic.
pub struct EventDescriptorCache {
    dbp: Arc<DatabaseProvider>,
    event_descriptors: SkipMap<String, PerTopicEventDescriptor>,
    update_in_progress_map: SkipMap<String, Arc<(Semaphore, u64)>>,
    reload_marker_generator: AtomicU64,
}

impl EventDescriptorCache {
    /// Return a new instance.
    pub async fn new(dbp: &Arc<DatabaseProvider>) -> Arc<Self> {
        Arc::new(Self {
            dbp: Arc::clone(dbp),
            event_descriptors: SkipMap::default(),
            update_in_progress_map: SkipMap::default(),
            reload_marker_generator: AtomicU64::default(),
        })
        .init()
        .await
    }

    async fn init(self: Arc<Self>) -> Arc<Self> {
        // Load right away
        self.reload_for_topics().await;
        // Start background reload
        let self_clone = Arc::clone(&self);
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(10_000)).await;
                self_clone.reload_for_topics().await
            }
        });
        self
    }

    async fn reload_for_topics(&self) {
        let mut from = None;
        loop {
            let (topic_ids, more) = self.dbp.topic_facade().get_topic_ids(&from).await;
            for topic_id in &topic_ids {
                self.reload_for_topic(topic_id).await;
            }
            if !more {
                break;
            }
            from = topic_ids.last().cloned();
        }
    }

    /// Reload [EventDescriptor] for topic.
    ///
    /// If there are multiple concurrent callers, only the first will make the
    /// database lookup.
    pub async fn reload_for_topic(&self, topic_id: &str) {
        // Only allow a single caller at the time to do the actual reload
        let marker = self.reload_marker_generator.fetch_add(1, Ordering::Relaxed);
        let tuplet = Arc::clone(
            self.update_in_progress_map
                .get_or_insert_with(topic_id.to_owned(), || {
                    Arc::new((Semaphore::new(0), marker))
                })
                .value(),
        );
        let (semaphore, inserted_marker) = tuplet.as_ref();
        if marker == *inserted_marker {
            // Current caller created the semaphore
            self.reload_for_topic_internal(topic_id).await;
            self.update_in_progress_map.remove(topic_id);
            semaphore.add_permits(Semaphore::MAX_PERMITS);
        }
        let _permit = semaphore.acquire().await.unwrap();
    }

    /// Reload the EventDescriptor from database for a topic.
    async fn reload_for_topic_internal(&self, topic_id: &str) {
        // Load all event descriptors
        let min_descriptor_version = None;
        let mut event_descriptors = self
            .dbp
            .topic_facade()
            .event_descriptors_by_topic_id(topic_id, min_descriptor_version)
            .await
            .iter()
            .map(EventDescriptor::from_string)
            .collect::<Vec<_>>();
        //  track version_min and version_latest
        let version_latest = event_descriptors.iter().map(|ed| ed.get_version()).max();
        if let Some(version_latest) = version_latest {
            if let Some(current) = self.event_descriptors.get(topic_id)
                && current.value().get_version_latest() == version_latest
            {
                // No change
                return;
            }
            let version_min = event_descriptors
                .iter()
                .filter_map(EventDescriptor::get_version_min)
                .max();
            if let Some(version_min) = version_min {
                // Don't allow minimum version to be a future version
                let version_min = std::cmp::min(version_min, version_latest);
                //  filter out any where version<version_min
                event_descriptors = event_descriptors
                    .into_iter()
                    .filter(|ed| ed.get_version() >= version_min)
                    .collect::<Vec<_>>();
            };
            //  Update event_descriptors
            self.event_descriptors.insert(
                topic_id.to_owned(),
                PerTopicEventDescriptor::new(version_latest, version_min, event_descriptors),
            );
            if log::log_enabled!(log::Level::Debug) {
                log::debug!(
                    "Found description version {} for topic '{}'.",
                    version_latest,
                    &topic_id
                );
            }
        } else {
            self.event_descriptors.remove(topic_id);
        }
    }

    /// Get minimum allowed descriptor version for a topic.
    pub fn get_descriptor_version_min(&self, topic_id: &str) -> Option<DescriptorVersion> {
        self.event_descriptors
            .get(topic_id)
            .as_ref()
            .map(Entry::value)
            .and_then(PerTopicEventDescriptor::get_version_min)
    }

    /// Get a specific version of the event description for a topic.
    pub async fn get_event_descriptor_by_topic_and_version(
        &self,
        topic_id: &str,
        descriptor_version: &DescriptorVersion,
    ) -> Option<Arc<EventDescriptor>> {
        let mut ret =
            self.get_event_descriptor_by_topic_and_version_interal(topic_id, descriptor_version);
        if ret.is_none() {
            // Reload cache if the requested descriptor is not present yet (e.g. registered in another instance)
            self.reload_for_topic(topic_id).await;
            ret = self
                .get_event_descriptor_by_topic_and_version_interal(topic_id, descriptor_version);
        }
        ret
    }

    /// Get a specific version of the event description for a topic.
    fn get_event_descriptor_by_topic_and_version_interal(
        &self,
        topic_id: &str,
        descriptor_version: &DescriptorVersion,
    ) -> Option<Arc<EventDescriptor>> {
        self.event_descriptors
            .get(topic_id)
            .as_ref()
            .map(Entry::value)
            .and_then(|pted| pted.get_event_descriptor_by_version(descriptor_version))
    }

    /// Get the latest version of the event description for a topic.
    pub fn get_event_descriptor_by_topic_latest(
        &self,
        topic_id: &str,
    ) -> Option<Arc<EventDescriptor>> {
        self.event_descriptors
            .get(topic_id)
            .as_ref()
            .map(Entry::value)
            .and_then(PerTopicEventDescriptor::get_event_descriptor_latest)
    }
}
