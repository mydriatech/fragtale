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

//! Cached and versioned event descriptors a topic.

use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::map::Entry;
use fragtale_client::mb::event_descriptor::DescriptorVersion;
use fragtale_client::mb::event_descriptor::EventDescriptor;
use std::sync::Arc;

/// Caches and handles versioning of event descriptors for a topic.
pub struct PerTopicEventDescriptor {
    version_latest: u64,
    version_min: Option<u64>,
    event_descriptors: SkipMap<u64, Arc<EventDescriptor>>,
}

impl PerTopicEventDescriptor {
    /// Return a new instance.
    pub fn new(
        version_latest: u64,
        version_min: Option<u64>,
        event_descriptors: Vec<EventDescriptor>,
    ) -> Self {
        let event_descriptors =
            SkipMap::from_iter(event_descriptors.into_iter().map(|event_descriptor| {
                (event_descriptor.get_version(), Arc::new(event_descriptor))
            }));
        Self {
            version_latest,
            version_min,
            event_descriptors,
        }
    }

    /// Get the minimum required version of the event description clients need
    /// to support.
    pub fn get_version_min(&self) -> Option<DescriptorVersion> {
        self.version_min.map(DescriptorVersion::from_encoded)
    }

    /// Get the minimum required version of the event description clients need
    /// to support.
    pub fn get_version_latest(&self) -> u64 {
        self.version_latest
    }

    /// Get the latest event description for this topic.
    pub fn get_event_descriptor_latest(&self) -> Option<Arc<EventDescriptor>> {
        self.event_descriptors
            .get(&self.version_latest)
            .as_ref()
            .map(Entry::value)
            .map(Arc::clone)
    }

    /// Get a specific version of the event description for this topic.
    pub fn get_event_descriptor_by_version(
        &self,
        version: &DescriptorVersion,
    ) -> Option<Arc<EventDescriptor>> {
        self.event_descriptors
            .get(&version.as_encoded())
            .as_ref()
            .map(Entry::value)
            .map(Arc::clone)
    }
}
