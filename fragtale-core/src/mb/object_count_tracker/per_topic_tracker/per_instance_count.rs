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

//! Cached cluster count for each instance identifier.

use crossbeam_skiplist::SkipMap;
use std::sync::Arc;

/// Cached cluster count for each instance identifier.
pub struct PerInstanceCount {
    count_by_instance: SkipMap<u16, u64>,
}

impl PerInstanceCount {
    /// Return a new instance.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            count_by_instance: SkipMap::default(),
        })
    }

    /// Update the count for a specific instance id and return `true` if the
    /// entry was updated.
    pub fn upsert(&self, instance_id: u16, value: u64) -> bool {
        if let Some(old) = self.count_by_instance.get(&instance_id) {
            if old.value() == &value {
                return false;
            } else {
                self.count_by_instance.insert(instance_id, value);
            }
        } else {
            self.count_by_instance.insert(instance_id, value);
        }
        true
    }
}
