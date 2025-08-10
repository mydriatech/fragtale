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

//! Quickly respond to correlation requests when a matching event is seen.

use crate::conf::AppConfig;
use crate::util::LogScopeDuration;
use crossbeam_skiplist::SkipMap;
use fragtale_client::mb::correlation_token::CorrelationToken;
use fragtale_dbp::dbp::DatabaseProvider;
use fragtale_dbp::dbp::facades::DatabaseProviderFacades;
use fragtale_dbp::mb::consumers::EventDeliveryGist;
use fragtale_dbp::mb::correlation::CorrelationResultListener;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::{Duration, sleep};

/// Holds a semaphore which the correlation requestor tries to accuire.
struct HotlistEntry {
    semaphore: Semaphore,
    request_ts: u64,
}
impl HotlistEntry {
    pub fn new(request_ts: u64) -> Self {
        Self {
            semaphore: Semaphore::new(0),
            request_ts,
        }
    }
}

impl CorrelationResultListener for CorrelationHotlist {
    fn notify_hotlist_entry(&self, topic_id: &str, correlation_token: &str) -> bool {
        if let Some(entry) = self.hotlist.get(topic_id) {
            let per_correlation_token_map = entry.value();
            if let Some(entry) = per_correlation_token_map.remove(correlation_token) {
                entry.value().semaphore.add_permits(1);
                return true;
            }
        }
        false
    }
}

/// Tracks new events to wake up correlation token requests if a matching event
/// is seen.
pub struct CorrelationHotlist {
    dbp: Arc<DatabaseProvider>,
    /// topic_id, correlation_id, data
    hotlist: SkipMap<String, SkipMap<String, HotlistEntry>>,
    correlation_oid: Vec<u32>,
    correlation_secret: Vec<u8>,
}
impl CorrelationHotlist {
    const HOTLIST_DURATION_MICROS: u64 = 5_000_000 * 2;

    /// Return a new instance.
    pub async fn new(app_config: &Arc<AppConfig>, dbp: &Arc<DatabaseProvider>) -> Arc<Self> {
        let (correlation_oid, correlation_secret) = app_config.integrity.correlation_secret();
        Arc::new(Self {
            dbp: Arc::clone(dbp),
            hotlist: SkipMap::new(),
            correlation_oid,
            correlation_secret,
        })
        .initialize()
        .await
    }

    async fn initialize(self: Arc<Self>) -> Arc<Self> {
        let self_clone = Arc::clone(&self);
        tokio::spawn(async move { self_clone.wake_up_too_old().await });
        let self_clone = Arc::clone(&self);
        tokio::spawn(async move { self_clone.track_new_events().await });
        self
    }

    /// Remove items from hotlist if they are too old
    async fn wake_up_too_old(&self) {
        loop {
            sleep(Duration::from_millis(1000)).await;
            let mut count = 0u64;
            let now = fragtale_client::time::get_timestamp_micros();
            self.hotlist.iter().for_each(|per_topic_entry| {
                let per_topic_map = per_topic_entry.value();
                per_topic_map.iter().for_each(|entry| {
                    count += 1;
                    let hotlist_entry = entry.value();
                    if hotlist_entry.request_ts + Self::HOTLIST_DURATION_MICROS < now
                        && let Some(entry) = per_topic_map.remove(entry.key())
                    {
                        entry.value().semaphore.add_permits(1);
                        log::info!("Unlocked hotlist entry '{}' due to timeout.", entry.key());
                    }
                });
            });
            if !self.hotlist.is_empty() && count > 0 && log::log_enabled!(log::Level::Trace) {
                log::trace!(
                    "Topics in hotlist: {}. Total number of items: {}",
                    self.hotlist.len(),
                    count
                );
            }
        }
    }

    /// Watch for new events and trigger hot-list items when found
    async fn track_new_events(self: &Arc<Self>) {
        loop {
            let mut any_changes = false;
            let mut any_waiters = false;
            for per_topic_entry in self.hotlist.iter() {
                let topic_id = per_topic_entry.key();
                let per_correlation_token_map = per_topic_entry.value();
                if per_correlation_token_map.is_empty() {
                    // If no-one is waiting for events on this topic, do the next instead
                    continue;
                }
                any_waiters = true;
                if log::log_enabled!(log::Level::Trace) {
                    log::trace!(
                        "per_correlation_token_map.len is {}",
                        per_correlation_token_map.len()
                    );
                }
                let chl_clone = Arc::clone(self);
                let chlu: Box<Arc<dyn CorrelationResultListener>> = Box::new(chl_clone);
                if self
                    .dbp
                    .event_tracking_facade()
                    .track_new_events_in_topic(topic_id, chlu, Self::HOTLIST_DURATION_MICROS)
                    .await
                {
                    any_changes = true;
                }
            }
            if !any_changes && !any_waiters {
                // Avoid killing the CPU on an idle system where no-one is waiting for results
                tokio::time::sleep(tokio::time::Duration::from_millis(128)).await;
            } else {
                // Give other tasks (like waiting sempaphores a change to run)
                //tokio::task::yield_now().await;
                tokio::time::sleep(tokio::time::Duration::from_millis(48)).await;
            }
        }
    }

    /// Return the event document for the provided [CorrelationToken] if such
    /// entry exists in the `topic_id`.
    pub async fn get_event_by_correlation_token(
        &self,
        topic_id: &str,
        correlation_token_str: &str,
    ) -> Option<EventDeliveryGist> {
        // Validate token
        let request_ts =
            if let Some(correlation_token) = self.parse_and_validate(correlation_token_str) {
                correlation_token.get_timestamp_micros()
            } else {
                if log::log_enabled!(log::Level::Debug) {
                    log::debug!("Failed to verify correlation token.");
                }
                return None;
            };
        // Get timestamp from token
        let mut lock_and_unlocked = false;
        if request_ts + Self::HOTLIST_DURATION_MICROS
            > fragtale_client::time::get_timestamp_micros()
        {
            // Insert topic if not yet exists
            let entry = self
                .hotlist
                .get_or_insert_with(topic_id.to_string(), SkipMap::new);
            let map = entry.value();
            // Insert HotlistEntry for correlation_id
            let entry = map.insert(
                correlation_token_str.to_owned(),
                HotlistEntry::new(request_ts),
            );
            if log::log_enabled!(log::Level::Trace) {
                log::trace!("Inserted hotlist for correlation token {correlation_token_str}");
            }
            let lsd = LogScopeDuration::new(
                log::Level::Debug,
                module_path!(),
                "waiting for semaphore for correlation token",
                2_500_000,
            );
            if log::log_enabled!(log::Level::Trace) {
                let start_ts = lsd
                    .as_ref()
                    .map(LogScopeDuration::start_ts_micros)
                    .unwrap_or(u64::MAX);
                log::trace!(
                    "Inserted hotlist for correlation token {} micros after creation.",
                    start_ts - request_ts
                );
            }
            // await semaphore
            let _permit_res = entry.value().semaphore.acquire().await;
            lock_and_unlocked = true;
        }
        let ret = self
            .dbp
            .event_facade()
            .event_document_by_correlation_token(topic_id, correlation_token_str)
            .await;

        if lock_and_unlocked && log::log_enabled!(log::Level::Debug) {
            if ret.is_some() {
                if log::log_enabled!(log::Level::Trace) {
                    log::trace!("Found event.");
                }
            } else {
                log::debug!("No event after unlock!");
                /*
                 This implies
                    * that the message was never delivered OR
                        * due to the original message never being delivered (missing deliveryintent!) ← FAIL!
                    * that the correlation id is not usable in an index
                        * due to bad encoding
                        * or SAI bug/limitation
                */
            }
        }
        ret
    }

    /// Create a new CorrelationToken if none exists or can't be validated.
    pub fn validate_or_protect(
        &self,
        correlation_token_opt: Option<String>,
        event_ts: u64,
    ) -> String {
        correlation_token_opt
            .and_then(|value| self.parse_and_validate(value.as_str()))
            .unwrap_or_else(|| {
                // Generate a new token if none was provided
                CorrelationToken::new(&self.correlation_oid, &self.correlation_secret, event_ts)
            })
            .as_string()
    }

    /// Return `Some(CorrelationToken)` if the string could be parsed and the
    /// token is valid.
    fn parse_and_validate(&self, correlation_token: &str) -> Option<CorrelationToken> {
        CorrelationToken::from_string(correlation_token)
            .map_err(|e| {
                log::info!("Failed to parse correlation token: {e}");
            })
            .ok()
            .and_then(|ct| {
                ct.verify(&self.correlation_oid, &self.correlation_secret)
                    .then_some(ct)
            })
    }
}
