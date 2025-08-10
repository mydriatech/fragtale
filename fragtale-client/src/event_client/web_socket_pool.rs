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

//! WebSocket connection pool.

mod subscriber_command;
mod subscriber_response;
mod web_socket_connection;

use crate::authentication::BearerTokenCache;

pub use self::subscriber_command::SubscriberCommand;
pub use self::subscriber_response::SubscriberResponse;
use self::web_socket_connection::WebSocketConnection;
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;

/// WebSocket connection pool.
///
/// The pool manages several connections and recieved messages from any
/// connection is stored in a common pool queue.
pub struct WebSocketPool {
    url: String,
    bearer_token_cache: Arc<BearerTokenCache>,
    tx: UnboundedSender<SubscriberResponse>,
    rx: Arc<Mutex<UnboundedReceiver<SubscriberResponse>>>,
    ws_connections: SkipMap<u64, Arc<WebSocketConnection>>,
    rr_counter: AtomicU64,
    pool_size: u64,
    min_pool_size: u64,
    initialized: AtomicBool,
    last_get_next_ts: AtomicU64,
}

impl WebSocketPool {
    /// Ping interval
    pub const PING_INTERVAL_MICROS: u64 = 5_000_000;

    /// Return a new instance.
    pub async fn new(url: &str, pool_size: usize, min_pool_size: usize) -> Arc<Self> {
        let bearer_token_cache = BearerTokenCache::new().await;
        let (tx, rx) = mpsc::unbounded_channel();
        Arc::new(Self {
            //client,
            url: url.to_owned(),
            bearer_token_cache,
            tx,
            rx: Arc::new(Mutex::new(rx)),
            ws_connections: SkipMap::new(),
            rr_counter: AtomicU64::new(0),
            pool_size: u64::try_from(pool_size).unwrap(),
            min_pool_size: u64::try_from(min_pool_size).unwrap(),
            initialized: AtomicBool::new(false),
            last_get_next_ts: AtomicU64::new(u64::MAX),
        })
    }

    /// Lazy initialization on use.
    async fn lazy_init(self: &Arc<Self>) {
        if !self.initialized.swap(true, Ordering::Relaxed) {
            for ws_instance_id in 0..self.pool_size {
                let self_clone = Arc::clone(self);
                tokio::spawn(async move { self_clone.maintain_ws_instance(ws_instance_id).await });
            }
        }
    }

    /// Start (and restart) message handling if this instance should be kept
    /// alive.
    async fn maintain_ws_instance(&self, ws_connection_id: u64) {
        let keep_alive = ws_connection_id < self.min_pool_size;
        loop {
            if !keep_alive {
                // while no incoming messages
                while self.last_get_next_ts.load(Ordering::Relaxed) != u64::MAX {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
                if log::log_enabled!(log::Level::Debug) {
                    log::debug!("Will fire up web socket connection '{ws_connection_id}' shortly.");
                }
            }
            if let Some(ws_connection) = WebSocketConnection::connect(
                &self.url,
                &self.bearer_token_cache.current_as_header_value().await,
                &self.tx.clone(),
            )
            .await
            {
                self.ws_connections
                    .insert(ws_connection_id, Arc::clone(&ws_connection));
                if log::log_enabled!(log::Level::Debug) {
                    log::debug!(
                        "Added web socket connection '{ws_connection_id}'. Connection count is now {}.",
                        self.ws_connections.len()
                    );
                }
                ws_connection
                    .handle_messages(Self::PING_INTERVAL_MICROS)
                    .await;
                // wait for any kind of failure or termination..
                ws_connection.await_termination().await;
            }
            log::info!("Removing web socket connection {ws_connection_id}.");
            self.ws_connections.remove(&ws_connection_id);
            if log::log_enabled!(log::Level::Debug) {
                log::debug!(
                    "Removed web socket connection '{ws_connection_id}'. Connection count esitmate is now {}.",
                    self.ws_connections.len()
                );
            }
            if keep_alive {
                if log::log_enabled!(log::Level::Debug) {
                    log::debug!(
                        "Will try to recover web socket connection '{ws_connection_id}' shortly."
                    );
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
            }
        }
    }

    /// Send command over WebSocket
    pub async fn send(self: &Arc<Self>, command: &SubscriberCommand, flush: bool) {
        if log::log_enabled!(log::Level::Trace) {
            log::trace!("Sending[flush: {flush}]: {command:?}");
        }
        self.get_available_ws_connection()
            .await
            .send(command, flush)
            .await;
        if log::log_enabled!(log::Level::Trace) {
            log::trace!("Sent   [flush: {flush}]: {command:?}");
        }
    }

    /// Return next available [WebSocketConnection].
    async fn get_available_ws_connection(self: &Arc<Self>) -> Arc<WebSocketConnection> {
        self.lazy_init().await;
        let mut count = 0u64;
        loop {
            for _attempt in 0..self.pool_size {
                let next_ws_connection_id =
                    self.rr_counter.fetch_add(1, Ordering::Relaxed) % self.pool_size;
                if let Some(entry) = self.ws_connections.get(&next_ws_connection_id) {
                    if log::log_enabled!(log::Level::Trace) {
                        log::trace!("Found available instance '{next_ws_connection_id}'.");
                    }
                    return Arc::clone(entry.value());
                } else if log::log_enabled!(log::Level::Trace) {
                    log::trace!(
                        "Connection '{next_ws_connection_id}' is not available at this time."
                    );
                }
            }
            if count % 8 == 0 && log::log_enabled!(log::Level::Debug) {
                log::debug!("No available web socket connection yet. Will retry shortly.");
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            count += 1;
        }
    }

    /// If available, get the next SubscriberResponse from any WebSocket
    async fn try_next(self: &Arc<Self>) -> Option<SubscriberResponse> {
        self.lazy_init().await;
        Arc::clone(&self.rx)
            .lock()
            .await
            .try_recv()
            .map_err(|e| {
                match e {
                    mpsc::error::TryRecvError::Empty => {
                        //Ignore;
                    }
                    e => {
                        log::debug!("Unable to recv next message: {e:?}");
                    }
                }
            })
            .ok()
    }

    /// Get the next [SubscriberResponse] from any of the open
    /// [WebSocketConnection]s.
    pub async fn next(self: &Arc<Self>) -> Option<SubscriberResponse> {
        loop {
            let next = self.try_next().await;
            if next.is_some() {
                // Don't bother to check the time while its hot..
                self.last_get_next_ts.store(u64::MAX, Ordering::Relaxed);
                return next;
            }
            let now = crate::time::get_timestamp_micros();
            let last_get_next_ts = self.last_get_next_ts.load(Ordering::Relaxed);
            if last_get_next_ts == u64::MAX {
                self.last_get_next_ts.store(now, Ordering::Relaxed);
                tokio::time::sleep(tokio::time::Duration::from_millis(32)).await;
            } else if last_get_next_ts < now - Self::PING_INTERVAL_MICROS {
                // Terminate another non-keep-alive instance
                for ws_connection_id in self.min_pool_size..self.pool_size {
                    if let Some(entry) = self.ws_connections.get(&ws_connection_id) {
                        let ws_connection = Arc::clone(entry.value());
                        if ws_connection.is_signaled_to_terminate() {
                            continue;
                        }
                        if log::log_enabled!(log::Level::Debug) {
                            log::debug!("Will terminate web socket connection {ws_connection_id}.");
                        }
                        ws_connection.signal_termination();
                        break;
                    }
                }
                // No successful poll in a while.. slow down..
                tokio::time::sleep(tokio::time::Duration::from_millis(128)).await;
            } else {
                tokio::time::sleep(tokio::time::Duration::from_millis(32)).await;
            }
        }
    }
}
