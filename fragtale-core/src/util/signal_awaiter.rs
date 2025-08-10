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

//! Signalling of async awaiters.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::sync::Semaphore;

/// The `SignalAwaiter` allows async functions to wait a signal.
pub struct SignalAwaiter {
    signaled: AtomicBool,
    semaphore: Semaphore,
}

impl SignalAwaiter {
    /// Return a new instance.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            signaled: AtomicBool::default(),
            semaphore: Semaphore::new(0),
        })
    }

    /// Wait for signal.
    pub async fn wait_for_signal(&self) {
        let _ = self.semaphore.acquire().await;
    }

    /// Signal any awaiting thread.
    pub fn signal(&self) {
        // Only add permits once.
        if !self.signaled.swap(true, Ordering::Relaxed) {
            self.semaphore.add_permits(Semaphore::MAX_PERMITS);
        }
    }
}
