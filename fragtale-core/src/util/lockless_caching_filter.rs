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

//! Lock-less caching filter.

use crossbeam_skiplist::SkipMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

struct QueueEntry {
    content: Vec<u8>,
    visited: AtomicBool,
}

impl QueueEntry {
    /// Return a new instance.
    pub fn new(content: &[u8]) -> Self {
        Self {
            content: content.to_vec(),
            visited: AtomicBool::default(),
        }
    }

    /// Return a reference to the copy of the content.
    pub fn get_content(&self) -> &[u8] {
        &self.content
    }

    /// Set the `visited` flag and return previous value.
    pub fn set_visited(&self, visited: bool) -> bool {
        self.visited.swap(visited, Ordering::Relaxed)
    }
}

/** Lock-less caching filter.

[SIEVE](https://junchengyang.com/publication/nsdi24-SIEVE.pdf) is a simple cache
eviction algorithm that has been shown to be work well for web caches.

It is implemented here in a lock-less version with lazy backgroud eviction, so
actual cache size will overshoot the target during high load.
*/
pub struct LocklessCachingFilter {
    eviction_running: AtomicBool,
    target_max_size: u64,
    pos: AtomicU64,
    count: AtomicU64,
    queue_map: SkipMap<u64, QueueEntry>,
    cache: SkipMap<Vec<u8>, u64>,
    hand: AtomicU64,
}

/* Implementation notes:
```
            SIEVE   SkipMap
Oldest      tail    .front()
Newest      head    .back()
Previous    .prev   .next()
```
*/
impl LocklessCachingFilter {
    /// Return a new instance.
    pub fn new(target_max_size: u64) -> Arc<Self> {
        Arc::new(Self {
            eviction_running: AtomicBool::default(),
            target_max_size,
            pos: AtomicU64::default(),
            count: AtomicU64::default(),
            queue_map: SkipMap::default(),
            cache: SkipMap::default(),
            hand: AtomicU64::default(),
        })
    }

    /// Return `true` if the cache hold the requested content.
    pub fn contains(&self, content: &[u8]) -> bool {
        self.cache
            .get(content)
            .is_some_and(|entry| self.set_visited(entry.value(), true) | true)
    }

    /// Set the visited flag for cache entry and return the old value.
    fn set_visited(&self, pos: &u64, visited: bool) -> bool {
        self.queue_map
            .get(pos)
            .is_some_and(|entry| entry.value().set_visited(visited))
    }

    /// Insert content unless it already was present.
    pub async fn insert(self: &Arc<Self>, content: &[u8]) {
        let pos = self.pos.fetch_add(1, Ordering::Relaxed);
        let pos_in_cache = *self
            .cache
            .get_or_insert_with(content.to_owned(), || pos)
            .value();
        if pos.eq(&pos_in_cache) {
            // Cache entry did not exist
            self.queue_map.insert(pos, QueueEntry::new(content));
            let count = self.count.fetch_add(1, Ordering::Relaxed) + 1;
            if log::log_enabled!(log::Level::Trace) {
                log::trace!("After insert of '{content:?}' cache will contain {count} entries.");
            }
            if count > self.target_max_size {
                // Run cache eviction (eventually)
                let self_clone = Arc::clone(self);
                tokio::spawn(async move { self_clone.run_eviction().await });
            }
        }
    }

    async fn run_eviction(&self) {
        if self.eviction_running.swap(true, Ordering::SeqCst) {
            // Eviction is already running
            return;
        }
        let start_hand_pos = self.hand.load(Ordering::Relaxed);
        let mut hand_entry_opt = self.queue_map.get(&start_hand_pos);
        while self.count.load(Ordering::Relaxed) > self.target_max_size {
            if hand_entry_opt.is_none() {
                // Try from the oldest (tail) entry again
                if log::log_enabled!(log::Level::Trace) {
                    log::trace!("Moving hand to tail.");
                }
                hand_entry_opt = self.queue_map.front();
            }
            if let Some(hand_entry) = hand_entry_opt.as_ref() {
                let hand_pos = *hand_entry.key();
                if !self.set_visited(&hand_pos, false) {
                    let content = hand_entry.value().get_content();
                    if log::log_enabled!(log::Level::Trace) {
                        log::trace!("Entry '{content:?}' will be evicted.");
                    }
                    self.queue_map.remove(&hand_pos);
                    self.cache.remove(content);
                    // Kicked one out
                    let counter_after_eviction = self.count.fetch_sub(1, Ordering::Relaxed) - 1;
                    if counter_after_eviction - self.target_max_size <= self.target_max_size >> 2 {
                        // Unless we overshoot max size by for than 25%.. don't stop the world..
                        tokio::task::yield_now().await;
                    }
                    if log::log_enabled!(log::Level::Trace) {
                        log::trace!(
                            "After kicking out entry '{:x?}...' the cache holds {counter_after_eviction} entries.",
                            &content[0..8]
                        );
                    }
                }
                hand_entry_opt = hand_entry.next();
            } else {
                // Unable to get front of queue...
                break;
            }
        }
        if let Some(hand_entry) = hand_entry_opt {
            // Store new hand pos
            self.hand.store(*hand_entry.key(), Ordering::Relaxed);
        }
        // Eviction is no longer running
        self.eviction_running.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    pub fn initialize_env_logger() {
        env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Debug)
            .try_init()
            .map_err(|e| {
                log::trace!("Env logger for testing was probably already initialized: {e:?}")
            })
            .ok();
    }

    #[tokio::test]
    async fn test_caching_filter() {
        initialize_env_logger();
        let cache = LocklessCachingFilter::new(2);
        assert!(!cache.contains(b"value1"));
        cache.insert(b"value1").await;
        cache.insert(b"value2").await;
        assert!(cache.contains(b"value1"));
        assert!(cache.contains(b"value2"));
        cache.insert(b"value3").await;
        assert!(cache.contains(b"value3"));
        await_eviction_run(&cache).await;
        assert!(!cache.contains(b"value1"));
        assert!(cache.contains(b"value2"));
        assert!(cache.contains(b"value3"));
        cache.insert(b"value4").await;
        await_eviction_run(&cache).await;
        assert!(cache.contains(b"value3"));
        cache.insert(b"value5").await;
        await_eviction_run(&cache).await;
        assert!(!cache.contains(b"value4"));
    }

    // In order make tests predictable we need to wait for eviction to happen
    async fn await_eviction_run(cache: &Arc<LocklessCachingFilter>) {
        while cache.count.load(Ordering::Relaxed) > cache.target_max_size {
            tokio::time::sleep(tokio::time::Duration::from_millis(64)).await;
        }
    }
}
