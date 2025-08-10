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

//! WebSocket API resource for subscribing to events.

use crate::rest_api::AppState;
use crate::rest_api::common::ApiErrorMapper;
use crate::rest_api::common::NextQueryParams;
use actix_web::Error;
use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::get;
use actix_web::rt;
use actix_web::web;
use actix_web::web::Data;
use actix_web::web::Path;
use actix_web::web::Query;
use actix_ws::AggregatedMessage;
use actix_ws::AggregatedMessageStream;
use actix_ws::Session;
use fragtale_client::EventClient;
use fragtale_client::SubscriberResponse;
use fragtale_client::mb::event_descriptor::DescriptorVersion;
use fragtale_core::mb::auth::ClientIdentity;
use futures::StreamExt;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::time::Duration;
use tokio::time::sleep;
use tyst::encdec::hex::ToHex;

/// Open a WebSocket connection for subscribing to new events.
///
/// Consumer identifier is derived from authentication.
#[utoipa::path(
    tag = "web_socket",
    params(
        ("topic_id", description = "Topic identifier."),
        ("from" = Option<u64>, Query, description = "Only consider events newer than this in epoch milliseconds."),
        ("version" = Option<String>, Query, description = "Event Descriptor SemVer that the client prefers (major.minor)."),
    ),
    responses(
        (status = 101, description = "Switching protocols to websocket."),
        (status = 401, description = "Unauthorized: Authentication failure."),
        (status = 400, description = "Bad Request."),
    ),
    security(("bearer_auth" = [])),
)]
#[get("/topics/{topic_id}/subscribe")]
pub async fn subscribe_to_topic(
    http_request: HttpRequest,
    path: Path<String>,
    query: Query<NextQueryParams>,
    app_state: Data<AppState>,
    stream: web::Payload,
) -> Result<HttpResponse, Error> {
    let identity = app_state
        .auth
        .get_identity(&http_request)
        .map_err(ApiErrorMapper::from_message_broker_error)?;
    let consumer_id = identity.identity_string();
    let topic_id = path.into_inner();
    let next_query_params = query.into_inner();
    let baseline_micros = next_query_params.get_from_epoch_micros();
    let descriptor_version = next_query_params.get_descriptor_version()?;
    log::info!("Consumer '{consumer_id}' opened a subscriber connection for topic '{topic_id}'.");
    let (http_upgrade_response, session, stream) = actix_ws::handle(&http_request, stream)?;
    let stream = stream
        .aggregate_continuations()
        // aggregate continuation frames up to 1 MiB
        .max_continuation_size(2_usize.pow(20));
    let last_ping = Arc::new(AtomicU64::new(fragtale_client::time::get_timestamp_micros()));
    let last_ping_clone = Arc::clone(&last_ping);
    // Ship events to this stream
    rt::spawn(async move {
        ship_events_to_stream(
            &identity,
            app_state,
            session,
            last_ping,
            topic_id,
            baseline_micros,
            descriptor_version,
        )
        .await;
    });
    // Pull messages from this steam (none are expected, except pings)
    rt::spawn(async move { pull_messages_from_stream(stream, last_ping_clone).await });
    // Respond immediately with with WebSocket upgrade response
    Ok(http_upgrade_response)
}

/// Ship events to the subscribed consumer.
async fn ship_events_to_stream(
    identity: &ClientIdentity,
    app_state: Data<AppState>,
    mut session: Session,
    last_ping: Arc<AtomicU64>,
    topic_id: String,
    baseline_micros: Option<u64>,
    descriptor_version: Option<DescriptorVersion>,
) {
    let mut counter = 0u64;
    let mut exhausted_ts = None;
    let consumer_id = identity.identity_string();
    loop {
        let start_ts = fragtale_client::time::get_timestamp_micros();
        // Check that last ping was withing acceptable threshold
        if last_ping.load(Ordering::Relaxed)
            < start_ts - (EventClient::PING_INTERVAL_MICROS + 1_000_000)
        {
            if log::log_enabled!(log::Level::Trace) {
                log::trace!("Last ping on this web-socket connection was too old.");
            }
            break;
        }
        let res = app_state
            .mb
            .get_event_by_consumer_and_topic(
                identity,
                &topic_id,
                baseline_micros,
                descriptor_version,
            )
            .await;
        match res {
            Ok(Some((
                encoded_unique_time,
                event_document,
                correlation_token,
                delivery_instance_id,
            ))) => {
                let text = serde_json::to_string(&SubscriberResponse::Next {
                    encoded_unique_time,
                    delivery_instance_id,
                    correlation_token,
                    event_document,
                })
                .unwrap();
                if log::log_enabled!(log::Level::Trace) {
                    log::trace!("Sending text: {text}");
                }
                if let Err(e) = session.text(text).await {
                    if log::log_enabled!(log::Level::Debug) {
                        log::debug!("Send failed with: {e:?}");
                    }
                    // TODO: We could kill the delivery intent here right away to avoid waiting for its redelivery.
                    break;
                }
                /*
                if log::log_enabled!(log::Level::Debug) {
                    if log::log_enabled!(log::Level::Trace) {
                        log::trace!("Sent text.");
                    }
                    let duration = fragtale_client::time::get_timestamp_micros() - start_ts;
                    if duration > 1_000_000 {
                        log::debug!(
                            "get_event_by_consumer_and_topic + session.text took {duration} micros."
                        )
                    }
                    if let Some(exhausted_ts) = exhausted_ts {
                        let exhausted_duration = start_ts - exhausted_ts;
                        if exhausted_duration > 500_000 {
                            log::debug!(
                                "Time since there were no more messages: {exhausted_duration} micros."
                            )
                        }
                    }
                }
                */
                exhausted_ts = None;
            }
            Ok(None) => {
                if exhausted_ts.is_none() {
                    exhausted_ts = Some(start_ts);
                }
                // Only ping when there is no other traffic
                let delay_micros: u64 = 64_000;
                if counter % (EventClient::PING_INTERVAL_MICROS / delay_micros) == 0 {
                    if log::log_enabled!(log::Level::Trace) {
                        log::trace!("Sending ping");
                    }
                    if let Err(e) = session.ping("ping".as_bytes()).await {
                        if log::log_enabled!(log::Level::Debug) {
                            log::debug!("Ping failed with: {e:?}");
                        }
                        break;
                    }
                }
                sleep(Duration::from_micros(delay_micros)).await;
                counter += 1;
            }
            Err(e) => {
                log::info!("Closing connection due to error: {e}");
                break;
            }
        }
    }
    session
        .close(None)
        .await
        .map_err(|e| {
            log::debug!("Failed to close session: {e:?}");
        })
        .ok();
    if log::log_enabled!(log::Level::Debug) {
        log::debug!("Consumer '{consumer_id}' lost a subscriber connection for topic '{topic_id}'");
    }
}

/// Pull messages from this steam (none are expected, except pings)
async fn pull_messages_from_stream(mut stream: AggregatedMessageStream, last_ping: Arc<AtomicU64>) {
    let mut ping_id = None;
    loop {
        match stream.next().await {
            Some(Ok(AggregatedMessage::Text(text))) => {
                // Parse text as "command" and match
                if log::log_enabled!(log::Level::Debug) {
                    log::debug!("Ignoring msg: {text}");
                }
            }
            Some(Ok(AggregatedMessage::Binary(_bin))) => {
                // echo binary message
                if log::log_enabled!(log::Level::Debug) {
                    log::debug!("Ignoring binary message");
                }
                //session.binary(bin).await.unwrap();
            }
            Some(Ok(AggregatedMessage::Ping(msg))) => {
                // respond to PING frame with PONG frame
                if log::log_enabled!(log::Level::Trace) {
                    log::trace!("Got ping message");
                }
                // Allow client to use an unique identifier
                if ping_id.is_none() {
                    ping_id = Some(msg.to_vec());
                    if log::log_enabled!(log::Level::Debug) {
                        log::debug!(
                            "New ping id: {}",
                            ping_id.as_ref().unwrap().as_slice().to_hex()
                        );
                    }
                }
                if ping_id.as_ref().is_some_and(|ping_id| ping_id.eq(&msg)) {
                    let ping_ts = fragtale_client::time::get_timestamp_micros();
                    last_ping.store(ping_ts, Ordering::Relaxed);
                }
                //session.pong(&msg).await.unwrap();
            }
            Some(Ok(AggregatedMessage::Pong(_msg))) => {
                if log::log_enabled!(log::Level::Trace) {
                    log::trace!("Ignoring pong message");
                }
            }
            Some(Err(e)) => {
                if log::log_enabled!(log::Level::Debug) {
                    log::debug!("Failed to get next message: {e:?}");
                }
                break;
            }
            Some(Ok(msg)) => {
                if log::log_enabled!(log::Level::Debug) {
                    log::debug!("Unexpected message: {msg:?}");
                }
            }
            None => {
                if log::log_enabled!(log::Level::Debug) {
                    log::debug!("No message.");
                }
            }
        }
    }
}
