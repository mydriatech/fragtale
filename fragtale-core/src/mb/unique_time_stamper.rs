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

//! Produce enhanced cluster wide unique timestamps.

use crossbeam_skiplist::SkipMap;
use fragtale_dbp::dbp::DatabaseProvider;
use fragtale_dbp::dbp::facades::DatabaseProviderFacades;
use fragtale_dbp::mb::UniqueTime;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::time::Duration;
use tokio::time::sleep;

/** Track all recently used timestamps to ensure that there is never any
conflict for the local node.

By leveraging an instance identifier that unique across all instances, we can
construct a combined [UniqueTime] that is also unique across the cluster.
*/
pub struct UniqueTimeStamper {
    /// See [DatabaseProvider].
    dbp: Arc<DatabaseProvider>,
    /// The local instance identifier (guaranteed to be unqiue in the cluster).
    instance_id: u16,
    /// Time of latest successful claim/refrash of the instance id.
    latest_claim_success_micros: AtomicU64,
    /// Monotonic increasing counter to producer of per instance unique numbers.
    marker_generator: AtomicU64,
    /// Used event epoch micros times and a marker.
    used_timestamps: SkipMap<u64, u64>,
    /// Cached value. See [Self::get_oldest_first_claim_ts_micros].
    oldest_instance_claim_ts_cache: AtomicU64,
    /// Cache update time. See [Self::get_oldest_first_claim_ts_micros].
    oldest_instance_claim_ts_check: AtomicU64,
}

impl UniqueTimeStamper {
    /// The duration an instance claim will live before the node is considered
    /// dead.
    pub const CLAIM_TIME_TO_LIVE_SECONDS: u32 = 900;

    /// Return a new instance.
    pub async fn new(dbp: &Arc<DatabaseProvider>) -> Arc<Self> {
        let latest_claim_success_micros = fragtale_client::time::get_timestamp_micros();
        let instance_id = Self::claim_instance_id(dbp).await;
        Arc::new(Self {
            dbp: Arc::clone(dbp),
            instance_id,
            latest_claim_success_micros: AtomicU64::new(latest_claim_success_micros),
            marker_generator: AtomicU64::default(),
            used_timestamps: SkipMap::default(),
            oldest_instance_claim_ts_cache: AtomicU64::default(),
            oldest_instance_claim_ts_check: AtomicU64::default(),
        })
        .initialize()
        .await
    }

    async fn initialize(self: Arc<Self>) -> Arc<Self> {
        let self_clone = Arc::clone(&self);
        tokio::spawn(async move { self_clone.refresh_instance_id_claim().await });
        let self_clone = Arc::clone(&self);
        tokio::spawn(async move { self_clone.purge_old_used_timestamps().await });
        self
    }

    /// Return the local instance identifier.
    pub fn get_instance_id(&self) -> u16 {
        self.instance_id
    }

    /// Claim (reserve) a instance identifier for the local instance.
    async fn claim_instance_id(dbp: &DatabaseProvider) -> u16 {
        let identity_claim = dbp
            .instance_id_facade()
            .claim(Self::CLAIM_TIME_TO_LIVE_SECONDS)
            .await;
        log::debug!("Claimed instance identity {identity_claim}.");
        identity_claim
    }

    /// Release the local instance identifier reservation.
    pub async fn free_instance_id(&self) {
        self.dbp.instance_id_facade().free(self.instance_id).await;
        log::debug!("Freed instance identity {}.", self.instance_id);
    }

    /// Return `true` when there is still sufficient time to renew the instance
    /// id claim before the platform should kill the instance.
    pub fn is_instance_id_still_valid(&self) -> bool {
        self.time_left_to_refresh_micros() > 60_000_000
    }

    fn time_left_to_refresh_micros(&self) -> u64 {
        let ttl_micros = u64::from(Self::CLAIM_TIME_TO_LIVE_SECONDS) * 1_000_000;
        let now_micros = fragtale_client::time::get_timestamp_micros();
        let latest_claim_success_micros = self.latest_claim_success_micros.load(Ordering::Relaxed);
        if latest_claim_success_micros > now_micros {
            // Refresh just happened
            return ttl_micros;
        }
        let time_since_claim_success_micros = now_micros - latest_claim_success_micros;
        if time_since_claim_success_micros > ttl_micros {
            // Refresh is overdue
            return 0;
        }
        ttl_micros - time_since_claim_success_micros
    }

    /// Refresh the local instance id claim reservation.
    async fn refresh_instance_id_claim(&self) {
        loop {
            // Back off a little before trying again
            let time_left_micros = self.time_left_to_refresh_micros();
            sleep(Duration::from_micros(std::cmp::max(
                time_left_micros * 2 / 3,
                10_000_000,
            )))
            .await;
            // Insert held claim again to refresh TTL
            let successful_refresh = self
                .dbp
                .instance_id_facade()
                .refresh(Self::CLAIM_TIME_TO_LIVE_SECONDS, self.instance_id)
                .await;
            if successful_refresh {
                self.latest_claim_success_micros.store(
                    fragtale_client::time::get_timestamp_micros(),
                    Ordering::Relaxed,
                );
                if log::log_enabled!(log::Level::Trace) {
                    log::trace!("Refreshed claimed instance identity {}.", self.instance_id);
                }
            } else {
                // When this instance fails to refresh the claim before it
                // expires, the liveness health check will be raised and the
                // application killed by the platform.
                //
                // See [Self::is_instance_id_still_valid()].
                if log::log_enabled!(log::Level::Debug) {
                    log::error!(
                        "Failed to refreshed claimed instance identity {}.",
                        self.instance_id
                    );
                }
            }
        }
    }

    /// Remove older used timestamps
    async fn purge_old_used_timestamps(&self) {
        loop {
            // Back off a little before trying again
            sleep(Duration::from_micros(10_000_000)).await;
            let start_ts = fragtale_client::time::get_timestamp_micros();
            let cutoff_ts = start_ts - 10_000_000u64;
            while self
                .used_timestamps
                .front()
                .is_some_and(|entry| *entry.key() < cutoff_ts)
            {
                self.used_timestamps.pop_front();
            }
            if log::log_enabled!(log::Level::Trace) && !self.used_timestamps.is_empty() {
                let duration = fragtale_client::time::get_timestamp_micros() - start_ts;
                if duration > 10_000 {
                    log::trace!(
                        "Purge took {duration} micros. used_timestamps.len: {}",
                        self.used_timestamps.len()
                    );
                }
            }
        }
    }

    /// Transform event time into a unique timestamp.
    pub fn get_unique_timestamp(&self, event_ts_micros: u64, priority: u8) -> UniqueTime {
        let marker = self
            .marker_generator
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        for i in 0..100 {
            // Account for priority so thet we check for priority_ts uniqueness in this instance.
            let priority_ts = Self::get_priority_ts(event_ts_micros + i, priority);
            let entry = self.used_timestamps.get_or_insert(priority_ts, marker);
            if &marker == entry.value() {
                // Successful claim of unique time
                return UniqueTime::new(priority_ts, self.instance_id);
            }
        }
        panic!("Failed to generate unique timestamp after 100 attempts!")
    }

    /// Encode priority information into the unique time by adding up to 0.45 s for
    /// low priority events.
    fn get_priority_ts(event_ts: u64, priority: u8) -> u64 {
        let priority_max_delay_micros = 450_000u64;
        let delay_percent = u64::from(100u8 - std::cmp::max(priority, 100u8));
        event_ts + (delay_percent * priority_max_delay_micros) / 100
    }

    /// Return the time in micros of the oldest alive's node instance claim.
    ///
    /// This can be leveraged to determine when all instances are running the
    /// the current version of the software and configuration.
    ///
    /// Alive in this context should be interpreted as "not confirmed to be dead".
    pub async fn get_oldest_first_claim_ts_micros(&self) -> u64 {
        let now_micros = fragtale_client::time::get_timestamp_micros();
        if self.oldest_instance_claim_ts_check.load(Ordering::Relaxed) < now_micros - 10_000 {
            self.oldest_instance_claim_ts_check
                .store(now_micros, Ordering::Relaxed);
            let (_oldest_instance_id, oldest_claim_micros) =
                self.dbp.instance_id_facade().get_oldest_instance_id().await;
            self.oldest_instance_claim_ts_cache
                .store(oldest_claim_micros, Ordering::Relaxed);
            oldest_claim_micros
        } else {
            self.oldest_instance_claim_ts_cache.load(Ordering::Relaxed)
        }
    }

    /// Return `true` if this is the oldest alive instance.
    ///
    /// Alive in this context should be interpreted as "not confirmed to be dead".
    pub async fn is_oldest_instance(&self) -> bool {
        let (oldest_instance_id, _oldest_claim_micros) =
            self.dbp.instance_id_facade().get_oldest_instance_id().await;
        self.instance_id == oldest_instance_id
    }
}
