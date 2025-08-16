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

//! Monitor local time compared with NTP time source.

use sntpc::NtpContext;
pub use sntpc::NtpResult;
use sntpc::StdTimestampGen;
use sntpc::get_time;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::net::UdpSocket;

/// Monitors local time compared with NTP time source.
pub struct TrustedTime {
    enabled: bool,
    local_time_within_tolerance: Arc<AtomicBool>,
}

impl TrustedTime {
    /// Return a new instance.
    ///
    /// If no `ntp_host` is provided, local time will always be trusted.
    pub async fn new(ntp_host: Option<String>, tolerance_micros: u64) -> Arc<Self> {
        let local_time_within_tolerance = Arc::new(AtomicBool::default());
        if let Some(ntp_host) = ntp_host {
            let ntp_host = if ntp_host.contains(':') {
                ntp_host.to_string()
            } else {
                ntp_host.to_string() + ":123"
            };
            let server_addr = ntp_host
                .to_socket_addrs()
                .expect("Unable to resolve host")
                .next()
                .unwrap();
            let client_socket = UdpSocket::bind("0.0.0.0:0")
                .await
                .expect("Unable to crate UDP socket");
            log::info!(
                "Local NTP UDP listener bound to {:?}.",
                client_socket.local_addr()
            );
            log::debug!("Trusted time will monitor local system clock accuracy.");
            Arc::new(Self {
                enabled: true,
                local_time_within_tolerance,
            })
            .run(server_addr, client_socket, tolerance_micros)
            .await
        } else {
            log::debug!("Trusted time will NOT monitor local system clock accuracy.");
            Arc::new(Self {
                enabled: false,
                local_time_within_tolerance,
            })
        }
    }

    /// Return `true` if local system time was within tolerance during last
    /// check.
    pub fn is_local_time_within_tolerance(&self) -> bool {
        !self.enabled
            || self
                .local_time_within_tolerance
                .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Return microseconds since UNIX epoch if local time is known to be in sync.
    ///
    /// When no NTP-server is configured, this always returns the local time.
    pub fn get_timestamp_micros(&self) -> Option<u64> {
        self.is_local_time_within_tolerance()
            .then_some(fragtale_client::time::get_timestamp_micros())
    }

    async fn run(
        self: Arc<Self>,
        server_addr: SocketAddr,
        client_socket: UdpSocket,
        tolerance_micros: u64,
    ) -> Arc<Self> {
        let self_clone = Arc::clone(&self);
        let interval_micros = tolerance_micros / 2;
        let client_socket = Arc::new(client_socket);
        tokio::spawn(async move {
            loop {
                let client_socket = Arc::clone(&client_socket);
                // Spawn background job and sleep always in main "thread"
                let within_tolerance = self_clone
                    .local_time_within_tolerance
                    .load(Ordering::Relaxed);
                let res = tokio::spawn(async move {
                    let context = NtpContext::new(StdTimestampGen::default());
                    let deadline = tokio::time::Instant::now()
                        + tokio::time::Duration::from_micros(interval_micros);
                    let res_res = tokio::time::timeout_at(
                        deadline,
                        get_time(server_addr, client_socket.as_ref(), context),
                    )
                    .await;
                    match res_res {
                        Err(_e) => {
                            log::warn!("No NTP response within {interval_micros} µs.");
                        }
                        Ok(Err(e)) => {
                            if within_tolerance {
                                log::warn!(
                                    "Failed NTP request. This will be retried. Error: {e:?}"
                                );
                            } else {
                                log::debug!("NTP request failure: {e:?}");
                            }
                        }
                        Ok(Ok(ntp_result)) => {
                            return Some(ntp_result);
                        }
                    }
                    None
                });
                tokio::time::sleep(tokio::time::Duration::from_micros(interval_micros)).await;
                let mut within_tolerance = false;
                if let Ok(Some(ntp_result)) = res.await {
                    let offset_estimate = u64::try_from(ntp_result.offset.abs()).unwrap();
                    let accuracy = Self::get_precision_micros_from_ntp_time(&ntp_result);
                    within_tolerance = offset_estimate + accuracy < tolerance_micros;
                    if log::log_enabled!(log::Level::Trace) {
                        log::trace!(
                            "offset_estimate: {offset_estimate}, accuracy: {accuracy}, tolerance_micros: {tolerance_micros}"
                        );
                    }
                }
                self_clone
                    .local_time_within_tolerance
                    .store(within_tolerance, Ordering::Relaxed);
            }
        });
        self
    }

    /// Convert NTP precision from "power of 2 seconds" to microseconds.
    fn get_precision_micros_from_ntp_time(ntp_time: &NtpResult) -> u64 {
        (2f64.powi(i32::from(ntp_time.precision())) * 1_000_000f64).round() as u64
    }
}
