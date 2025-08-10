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

//! Ephemeral in-memory implementation a delivery intent.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

/// Ephemeral in-memory implementation a delivery intent.
#[derive(Debug, Default)]
pub struct InMemDeliveryIntent {
    intent_ts_micros: u64,
    done: AtomicBool,
}

impl InMemDeliveryIntent {
    /// Return a new instance.
    pub fn new(intent_ts_micros: u64) -> Self {
        Self {
            intent_ts_micros,
            done: AtomicBool::default(),
        }
    }

    /// Return the time of the intent creation.
    pub fn get_intent_ts_micros(&self) -> u64 {
        self.intent_ts_micros
    }

    /// Return `true` if no more processing of this event should happen.
    pub fn is_done(&self) -> bool {
        self.done.load(Ordering::Relaxed)
    }

    /// Set to `true` if no more processing of this event should happen.
    pub fn set_done(&self, done: bool) {
        self.done.store(done, Ordering::Relaxed);
    }
}
