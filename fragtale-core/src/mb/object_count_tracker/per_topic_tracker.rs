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

//! Maintain count of objects of a different types in the cluster for a topic.

mod local_object_count;
mod per_instance_count;

use self::local_object_count::LocalObjectCount;
use self::per_instance_count::PerInstanceCount;
use crate::util::SignalAwaiter;
use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::map::Iter;
use fragtale_dbp::mb::ObjectCountType;
use std::sync::Arc;

/// Per topic counts and awaiters.
pub struct PerTopicTracker {
    // Local instance's count and current persisted value of this count.
    local_count: SkipMap<ObjectCountType, Arc<LocalObjectCount>>,
    // Cached cluster count for each instance identifier.
    per_instance_counts_by_object_type: SkipMap<ObjectCountType, Arc<PerInstanceCount>>,
    // Count change awaiters by object type.
    awaiters: SkipMap<ObjectCountType, Arc<SignalAwaiter>>,
}

impl PerTopicTracker {
    /// Return a new instance.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            local_count: SkipMap::default(),
            per_instance_counts_by_object_type: SkipMap::default(),
            awaiters: SkipMap::default(),
        })
    }

    /// Return cached cluster count for each instance identifier for the
    /// `ObjectCountType`.
    pub fn per_instance_counts_by_type(
        &self,
        object_count_type: &ObjectCountType,
    ) -> Arc<PerInstanceCount> {
        // Avoid cloning key if map entry already exists.
        self.per_instance_counts_by_object_type
            .get(object_count_type)
            .as_ref()
            .map(Entry::value)
            .map(Arc::clone)
            .unwrap_or_else(|| {
                Arc::clone(
                    self.per_instance_counts_by_object_type
                        .get_or_insert_with(object_count_type.to_owned(), PerInstanceCount::new)
                        .value(),
                )
            })
    }

    /// Return local instance count and current persisted value of this count
    /// for the `ObjectCountType`.
    pub fn local_count_by_type(
        &self,
        object_count_type: &ObjectCountType,
    ) -> Arc<LocalObjectCount> {
        // Avoid cloning key if map entry already exists.
        self.local_count
            .get(object_count_type)
            .as_ref()
            .map(Entry::value)
            .map(Arc::clone)
            .unwrap_or_else(|| {
                Arc::clone(
                    self.local_count
                        .get_or_insert_with(object_count_type.to_owned(), LocalObjectCount::new)
                        .value(),
                )
            })
    }

    /// Return local instance counts and current persisted value of this count
    /// for all the `ObjectCountType`s.
    pub fn local_counts(&self) -> Iter<'_, ObjectCountType, Arc<LocalObjectCount>> {
        self.local_count.iter()
    }

    // Return the count change awaiters by `ObjectCountType`.
    pub fn awaiter_by_type(&self, object_count_type: &ObjectCountType) -> Arc<SignalAwaiter> {
        // Avoid cloning key if map entry already exists.
        self.awaiters
            .get(object_count_type)
            .as_ref()
            .map(Entry::value)
            .map(Arc::clone)
            .unwrap_or_else(|| {
                Arc::clone(
                    self.awaiters
                        .get_or_insert_with(object_count_type.to_owned(), SignalAwaiter::new)
                        .value(),
                )
            })
    }

    // Return the count change awaiters for all the `ObjectCountType`s.
    pub fn awaiters(&self) -> Iter<'_, ObjectCountType, Arc<SignalAwaiter>> {
        self.awaiters.iter()
    }

    /// Remove and signal the count change awaiter for the `ObjectCountType`.
    pub fn awaiter_remove_and_signal(&self, object_count_type: &ObjectCountType) {
        if let Some(signal_awaiter) = self
            .awaiters
            .remove(object_count_type)
            .as_ref()
            .map(Entry::value)
        {
            signal_awaiter.signal()
        }
    }
}
