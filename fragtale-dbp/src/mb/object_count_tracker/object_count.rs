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

//! Number of objects counted on an instance.

/// Holds number of counted objects on an instance.
pub struct ObjectCount {
    /// Instance identifier.
    instance_id: u16,
    /// Number of objects.
    object_count: u64,
}

impl ObjectCount {
    /// Return a new instance.
    pub fn new(instance_id: i16, object_count: i64) -> Self {
        Self {
            instance_id: u16::try_from(instance_id).unwrap(),
            object_count: u64::try_from(object_count).unwrap(),
        }
    }

    /// Return instance identifier.
    pub fn get_instance_id(&self) -> u16 {
        self.instance_id
    }

    /// Return the number of objects.
    pub fn get_object_count(&self) -> u64 {
        self.object_count
    }
}
