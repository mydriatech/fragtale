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

//! Local instance's count and current persisted value of this count.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

/// Local instance's count and current persisted value of this count.
#[derive(Default)]
pub struct LocalObjectCount {
    current: AtomicU64,
    persisted: AtomicU64,
}

impl LocalObjectCount {
    /// Return a new instance.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            current: AtomicU64::default(),
            persisted: AtomicU64::default(),
        })
    }

    /// Return the current count.
    pub fn get_current(&self) -> u64 {
        self.current.load(Ordering::Relaxed)
    }

    /// Increase the current local count by 1.
    pub fn inc_current(&self) -> u64 {
        self.current.fetch_add(1, Ordering::Relaxed)
    }

    /// Return the last known peristed local count.
    pub fn get_persisted(&self) -> u64 {
        self.persisted.load(Ordering::Relaxed)
    }

    /// Set the last known peristed local count.
    pub fn set_persisted(&self, value: u64) {
        self.persisted.store(value, Ordering::Relaxed)
    }
}
