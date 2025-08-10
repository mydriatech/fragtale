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

//! WebSocket connection.

use futures::SinkExt;
use futures::StreamExt;
use futures::stream::SplitSink;
use futures::stream::SplitStream;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::ClientRequestBuilder;
use tokio_tungstenite::tungstenite::http::Uri;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tyst::Tyst;
use tyst::encdec::hex::ToHex;

use super::SubscriberCommand;
use super::SubscriberResponse;

pub struct WebSocketConnection {
    ws_write_stream: Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>,
    ws_read_stream: Arc<Mutex<SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>>>,
    tx: UnboundedSender<SubscriberResponse>,
    termination_semaphore: Semaphore,
    feed_counter: AtomicU64,
}

impl WebSocketConnection {
    /// Try to create and connect a new instance.
    ///
    /// Return `None` if the connection attempt failed.
    pub async fn connect(
        url: &str,
        authorization_header_value: &str,
        tx: &UnboundedSender<SubscriberResponse>,
    ) -> Option<Arc<Self>> {
        let url = if url.starts_with("http") {
            url.replacen("http", "ws", 1)
        } else {
            url.to_owned()
        };
        let uri: Uri = url.parse().unwrap();
        let builder = ClientRequestBuilder::new(uri)
            .with_header("Authorization", authorization_header_value)
            //.with_sub_protocol("fragtale_ws")
            ;
        if let Ok((ws_stream, _res)) = tokio_tungstenite::connect_async_with_config(
            builder,
            Some(WebSocketConfig::default()),
            true,
        )
        .await
        .map_err(|e| {
            log::debug!("Failed to connect to '{url}': {e:?}");
        }) {
            if log::log_enabled!(log::Level::Debug) {
                log::debug!("Opened websocket to '{url}'");
            }
            let (write, read) = ws_stream.split();
            let ws_write_stream = Arc::new(Mutex::new(write));
            let ws_read_stream = Arc::new(Mutex::new(read));
            Some(
                Arc::new(Self {
                    ws_write_stream,
                    ws_read_stream,
                    tx: tx.clone(),
                    termination_semaphore: Semaphore::new(0),
                    feed_counter: AtomicU64::new(0),
                })
                .initialize()
                .await,
            )
        } else {
            None
        }
    }

    /// Start background tasks.
    async fn initialize(self: Arc<Self>) -> Arc<Self> {
        let self_clone = Arc::clone(&self);
        tokio::spawn(async move { self_clone.maintain_flushed_write_channel().await });
        self
    }

    /// Ensure that data written to the web socket is flushed at least every 100
    /// milliseconds.
    async fn maintain_flushed_write_channel(&self) {
        let mut write_counter = 0;
        while !self.is_signaled_to_terminate() {
            let feed_counter = self.feed_counter.load(std::sync::atomic::Ordering::Relaxed);
            if write_counter < feed_counter {
                write_counter = feed_counter;
                {
                    let mut web_socket = self.ws_write_stream.lock().await;
                    if let Err(e) = web_socket.flush().await {
                        if log::log_enabled!(log::Level::Debug) {
                            log::debug!("Flush failed: {e:?}");
                        }
                        self.signal_termination();
                    }
                }
            }
            //tokio::task::yield_now().await;
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    /// Wait for this instance to terminate.
    pub async fn await_termination(&self) {
        let _ = self.termination_semaphore.acquire().await;
    }

    /// Return `true` if this instance is signaled to terminate.
    pub fn is_signaled_to_terminate(&self) -> bool {
        self.termination_semaphore.available_permits() > 0
    }

    /// Signal this instance to terminate.
    pub fn signal_termination(&self) {
        self.termination_semaphore.add_permits(1);
    }

    /// Recieve new messages from the web socket and queue them for the pool
    /// to pick up.
    pub async fn handle_messages(self: &Arc<Self>, ping_interval_micros: u64) {
        if log::log_enabled!(log::Level::Trace) {
            log::trace!("Starting worker to handle incoming messages.");
        }
        // Send ping messages at regular intervals
        let self_clone = Arc::clone(self);
        let ping_task = tokio::spawn(async move {
            // Use an unique ping indetifier for each connection.
            let mut ping_id = [0u8; 32];
            Tyst::instance().prng_fill_with_random(None, &mut ping_id);
            while !self_clone.is_signaled_to_terminate() {
                tokio::time::sleep(tokio::time::Duration::from_micros(ping_interval_micros)).await;
                if let Err(e) = self_clone
                    .ws_write_stream
                    .lock()
                    .await
                    .send(Message::Ping(ping_id.as_slice().to_vec()))
                    .await
                {
                    log::debug!("Sending ping failed: {e:?}");
                    break;
                } else {
                    log::debug!("Sent ping with ping_id '{}'", ping_id.as_slice().to_hex());
                }
            }
        });
        // Recieve new messages from the web socket and queue them for the pool
        // to pick up.
        while !ping_task.is_finished() {
            let res = {
                let mut web_socket_mutex = self.ws_read_stream.lock().await;
                web_socket_mutex.next().await
            };
            match res {
                Some(Ok(Message::Text(text))) => {
                    if log::log_enabled!(log::Level::Trace) {
                        log::trace!("Got text: {text}");
                    }
                    let message = serde_json::from_str(&text).unwrap();
                    if let Err(e) = self.tx.send(message) {
                        log::info!("Unable to write to queue: {e:?}");
                        break;
                    }
                }
                // Respond to ping with pong right away.
                Some(Ok(Message::Ping(text))) => {
                    // Here we should send a pong!
                    if log::log_enabled!(log::Level::Trace) {
                        log::trace!("Got ping: {text:?}");
                    }
                    if let Err(e) = self
                        .ws_write_stream
                        .lock()
                        .await
                        .send(Message::Pong(text))
                        .await
                    {
                        log::debug!("Pong send failed: {e:?}");
                        break;
                    }
                }
                Some(Ok(Message::Pong(text))) => {
                    if log::log_enabled!(log::Level::Trace) {
                        log::trace!("Got pong: {text:?}");
                    }
                }
                None => {
                    log::debug!("None next() message result.");
                    tokio::time::sleep(tokio::time::Duration::from_millis(32)).await;
                }
                Some(Err(e)) => {
                    log::info!("recv_next: {e:?}");
                    break;
                }
                r => {
                    log::info!("Unhandled result: {r:?}");
                    //break;
                    tokio::time::sleep(tokio::time::Duration::from_millis(32)).await;
                }
            }
        }
        if log::log_enabled!(log::Level::Trace) {
            log::trace!("Stopping worker to handle incoming messages.");
        }
        self.signal_termination();
    }

    /// Send all commands to the WebSocket and flush afterwards
    pub async fn send(&self, command: &SubscriberCommand, flush: bool) {
        let msg = Message::Text(serde_json::to_string(&command).unwrap());
        let mut web_socket = self.ws_write_stream.lock().await;
        let res = if flush {
            web_socket.send(msg).await
        } else {
            let res = web_socket.feed(msg).await;
            self.feed_counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            res
        };
        if let Err(e) = res {
            if log::log_enabled!(log::Level::Debug) {
                log::debug!("Send failed: {e:?}");
            }
            self.signal_termination();
        };
    }
}
