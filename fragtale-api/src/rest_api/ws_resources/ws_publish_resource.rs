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

//! WebSocket API resource for handling event publish messages.

use std::sync::Arc;

use crate::rest_api::AppState;
use crate::rest_api::common::ApiErrorMapper;
use actix_web::Error;
use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::get;
use actix_web::rt;
use actix_web::web::Data;
use actix_web::web::Path;
use actix_web::web::Payload;
use actix_ws::AggregatedMessage;
use actix_ws::AggregatedMessageStream;
use actix_ws::Session;
use fragtale_client::SubscriberCommand;
use fragtale_client::mb::event_descriptor::DescriptorVersion;
use fragtale_core::mb::auth::ClientIdentity;
use futures::StreamExt;

/// Open a WebSocket connection for publishing events.
///
/// Publisher identifier is derived from authentication.
#[utoipa::path(
    tag = "web_socket",
    params(
        ("topic_id", description = "Topic identifier."),
    ),
    responses(
        (status = 101, description = "Switching protocols to websocket."),
        (status = 401, description = "Unauthorized: Authentication failure."),
    ),
    security(("bearer_auth" = [])),
)]
#[get("/topics/{topic_id}/events")]
pub async fn publish_event_to_topic(
    http_request: HttpRequest,
    app_state: Data<AppState>,
    path: Path<String>,
    stream: Payload,
) -> Result<HttpResponse, Error> {
    let identity = app_state
        .auth
        .get_identity(&http_request)
        .map_err(ApiErrorMapper::from_message_broker_error)?;
    let topic_id = path.into_inner();
    log::info!("Publisher '{identity}' opened a publish connection for topic '{topic_id}'.");
    let (http_upgrade_response, session, stream) = actix_ws::handle(&http_request, stream)?;
    let stream = stream
        .aggregate_continuations()
        // aggregate continuation frames up to 4 MiB
        .max_continuation_size(2_usize.pow(22));
    // Pull messages from this steam
    rt::spawn(async move {
        pull_messages_from_stream(identity, app_state, session, stream, topic_id).await;
    });
    // Respond immediately with with WebSocket upgrade response
    Ok(http_upgrade_response)
}

/// Pull messages from this steam
async fn pull_messages_from_stream(
    identity: Arc<ClientIdentity>,
    app_state: Data<AppState>,
    mut session: Session,
    mut stream: AggregatedMessageStream,
    topic_id: String,
) {
    if log::log_enabled!(log::Level::Debug) {
        log::debug!("Starting to process publish messages");
    }
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(AggregatedMessage::Text(text)) => {
                // Parse text as "command" and match
                if log::log_enabled!(log::Level::Trace) {
                    log::trace!("Got msg: {text}");
                }
                match serde_json::from_str(&text) {
                    Ok(SubscriberCommand::Publish {
                        priority,
                        event_document,
                        correlation_token,
                        descriptor_version,
                    }) => {
                        let app_state = app_state.clone();
                        let identity = Arc::clone(&identity);
                        let topic_id = topic_id.to_owned();
                        let descriptor_version =
                            descriptor_version.map(DescriptorVersion::from_encoded);
                        rt::spawn(async move {
                            app_state
                                .mb
                                .publish_event_to_topic(
                                    &identity,
                                    &topic_id,
                                    &event_document,
                                    priority,
                                    descriptor_version,
                                    correlation_token,
                                )
                                .await
                                .map_err(|e| log::info!("Failed to publish event: {e}"))
                                .ok();
                        });
                    }
                    _ => {
                        if log::log_enabled!(log::Level::Debug) {
                            log::debug!("Ignoring text message: {text}");
                        }
                    }
                }
            }
            Ok(AggregatedMessage::Binary(_bin)) => {
                if log::log_enabled!(log::Level::Debug) {
                    log::debug!("Ignoring binary message");
                }
            }
            Ok(AggregatedMessage::Ping(msg)) => {
                // respond to PING frame with PONG frame
                if log::log_enabled!(log::Level::Trace) {
                    log::trace!("Sending pong message");
                }
                session.pong(&msg).await.unwrap();
            }
            Err(e) => {
                if log::log_enabled!(log::Level::Debug) {
                    log::debug!("Publish error: {e:?}");
                }
            }
            r => {
                if log::log_enabled!(log::Level::Debug) {
                    log::debug!("Unknown result: {r:?}");
                }
            }
        }
    }
    if log::log_enabled!(log::Level::Debug) {
        log::debug!("Stopping processing of publish messages");
    }
}
