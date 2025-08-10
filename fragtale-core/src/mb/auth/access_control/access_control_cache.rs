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

//! Cache successful authorization lookups

use crate::mb::auth::ClientIdentity;
use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::map::Entry;
use std::sync::Arc;

/// Cache successful authorization lookups for up to roughly 5 minutes.
pub struct AccessControlCache {
    cache_with_expiration: SkipMap<String, u64>,
}

impl AccessControlCache {
    /// Cache items for approximately this duration. (+0..10%)
    const CACHE_DURATION_ESTIMATE_MICROS: u64 = 300_000_000;

    /// Return a new instance.
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {
            cache_with_expiration: SkipMap::default(),
        })
        .init()
        .await
    }

    /// Initialize background tasks.
    async fn init(self: Arc<Self>) -> Arc<Self> {
        let ret = Arc::clone(&self);
        tokio::spawn(async move {
            self.purge_expired().await;
        });
        ret
    }

    /// Transform identity and resouce into a lookup key.
    fn as_key(identity: &ClientIdentity, resource: &str) -> String {
        identity.identity_string().to_string() + ";" + resource
    }

    /// Remove all expired cache entries.
    async fn purge_expired(&self) {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_micros(
                Self::CACHE_DURATION_ESTIMATE_MICROS / 10,
            ))
            .await;
            let now = fragtale_client::time::get_timestamp_micros();
            for entry in self.cache_with_expiration.iter() {
                if *entry.value() < now {
                    entry.remove();
                }
            }
        }
    }

    /// Return `true` if the `identity` is authorized to `resource`.
    pub fn is_authorized_to_resource(&self, identity: &ClientIdentity, resource: &str) -> bool {
        let now = fragtale_client::time::get_timestamp_micros();
        self.cache_with_expiration
            .get(&AccessControlCache::as_key(identity, resource))
            .as_ref()
            .map(Entry::value)
            .filter(|expiration| expiration < &&now)
            .is_some()
    }

    /// Insert cache entry that `identity` is authorized to `resource`.
    pub fn insert(&self, identity: &ClientIdentity, resource: &str) {
        let now = fragtale_client::time::get_timestamp_micros();
        self.cache_with_expiration.insert(
            AccessControlCache::as_key(identity, resource),
            now + Self::CACHE_DURATION_ESTIMATE_MICROS,
        );
    }
}
