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

//! API resource for publishing event documents.

use crate::rest_api::AppState;
use crate::rest_api::common::ApiErrorMapper;
use crate::rest_api::common::NextQueryParams;
use actix_web::Error;
use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::error;
use actix_web::http::StatusCode;
use actix_web::put;
use actix_web::web;
use actix_web::web::Data;
use actix_web::web::Path;
use actix_web::web::Payload;
use actix_web::web::Query;
use fragtale_client::mb::event_descriptor::DescriptorVersion;
use fragtale_core::util::LogScopeDuration;
use futures::StreamExt;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct PublishQuery {
    /// Event priority
    priority: Option<u8>,
    /// Event Descriptor SemVer of the event.
    #[serde(rename = "version")]
    event_descriptor_semver: Option<String>,
    /// Effects the topic of the returned `Location` header for retrieving the
    /// result of correlated request.
    #[serde(rename = "target")]
    result_topic_id: Option<String>,
}

impl PublishQuery {
    /// Respect consumers version support to avoid (too new) incompatibel
    /// messages.
    ///
    /// Errors out with HTTP 400 Bad Request if parameters is not in the form
    /// `[number[.number]]`
    pub fn get_descriptor_version(&self) -> Result<Option<DescriptorVersion>, Error> {
        NextQueryParams::as_descriptor_version(&self.event_descriptor_semver)
    }
}

/// Cassandra practical max column size is 5 MiB.
const MAX_DOCUMENT_SIZE: usize = 5 * 1024 * 1024;

/// Publish event document.
///
/// Please note the `correlation-token` and `location` header if you need to
/// find an event from a different topic that is the result of processing this event.
///
/// Publisher identifier is derived from authentication.
#[utoipa::path(
    tag = "http",
    //operation_id = "publish_event_to_topic",
    params(
        ("topic_id", description = "Topic identifier."),
        (
            "priority" = Option<u8>,
            Query,
            description = "Importance of the published event. 0-100 where 100 is most important."
        ),
        (
            "version" = Option<String>,
            Query,
            description = "Event Descriptor SemVer the event is expected to comply to. (E.g. major.minor)."
        ),
        (
            "target" = Option<String>,
            Query,
            description = "Expected target topic of correlated event processing."
        ),
    ),
    responses(
        (
            status = 200,
            description = "Ok. Successfully published event and returning response of correlated request.",
            content_type = "application/json",
        ),
        (
            status = 204,
            description = "No content. Successfully published event.",
            headers(
                (
                    "correlation-token" = String,
                    description = "Opaque token that can be used to correlate events."
                ),
            ),
        ),
        (
            status = 303,
            description = "See other. Correlated result did not appear before the timeout. Poll linked resource to keep trying.",
            links(
                (
                    "Location" = (
                        operation_id = "by_topic_and_correlation_token",
                        parameters(
                            ("topic_id" = "string"),
                            ("correlation_token" = "string")
                        ),
                    )
                ),
            ),
        ),
        (status = 400, description = "Bad Request."),
        (status = 401, description = "Unauthorized: Authentication failure."),
        (status = 403, description = "Forbidden: Authorization failure."),
        (status = 500, description = "Internal server error."),
    ),
    security(("bearer_auth" = [])),
)]
#[put("/topics/{topic_id}/events")]
pub async fn publish_event_to_topic(
    app_state: Data<AppState>,
    path: Path<String>,
    query: Query<PublishQuery>,
    payload: Payload,
    http_request: HttpRequest,
) -> Result<HttpResponse, Error> {
    let _ = LogScopeDuration::new(
        log::Level::Trace,
        module_path!(),
        "publish_event_to_topic",
        0,
    );
    let topic_id = path.into_inner();
    let publish_query = query.into_inner();
    let priority = publish_query.priority;
    let descriptor_version = publish_query.get_descriptor_version()?;
    let http_headers = http_request.headers();
    let identity = app_state
        .auth
        .get_identity(&http_request)
        .map_err(ApiErrorMapper::from_message_broker_error)?;
    let content_length_estimate = assert_declared_content_length(&http_request, MAX_DOCUMENT_SIZE)?;
    let event_document = read_full_body_text(&topic_id, content_length_estimate, payload).await?;
    let correlation_token_opt = http_headers
        .get("correlation-token")
        .and_then(|header_value| header_value.to_str().ok())
        .map(str::to_string);
    let correlation_token_opt_exists = correlation_token_opt.is_some();
    let persisted_correlation_token = app_state
        .mb
        .publish_event_to_topic(
            &identity,
            &topic_id,
            &event_document,
            priority,
            descriptor_version,
            correlation_token_opt,
        )
        .await
        .map_err(ApiErrorMapper::from_message_broker_error)?;
    if let Some(result_topic_id) = publish_query.result_topic_id {
        if let Some(result_document) = app_state
            .mb
            .get_event_by_correlation_token(
                &identity,
                &result_topic_id,
                &persisted_correlation_token,
            )
            .await
            .map_err(ApiErrorMapper::from_message_broker_error)?
        {
            Ok(HttpResponse::build(StatusCode::OK).body(result_document))
        } else {
            let result_poll_url = http_request
                .url_for(
                    "by_topic_and_correlation_token",
                    [result_topic_id, persisted_correlation_token],
                )
                .unwrap();
            // Revert to Post-Redirect-Get pattern if this takes to long
            Ok(HttpResponse::build(StatusCode::SEE_OTHER)
                .append_header(("Location", result_poll_url.as_str()))
                .finish())
        }
    } else {
        let mut builder = HttpResponse::build(StatusCode::NO_CONTENT);
        if !correlation_token_opt_exists {
            // Return the correlation (only really matters first time when it was generated)
            builder.append_header(("correlation-token", persisted_correlation_token));
        }
        Ok(builder.finish())
    }
}

/// Assert that the declared content-length header (if present) is within the
/// max_size limit.
fn assert_declared_content_length(
    http_request: &HttpRequest,
    max_size: usize,
) -> Result<usize, Error> {
    let content_length_estimate = http_request
        .headers()
        .get("content-length")
        .and_then(|header_value| header_value.to_str().ok())
        .and_then(|header_value_str| header_value_str.parse::<usize>().ok())
        .unwrap_or(1024);
    if content_length_estimate > max_size {
        Err(error::ErrorBadRequest("overflow"))?
    } else {
        Ok(content_length_estimate)
    }
}

async fn read_full_body_text(
    topic_id: &str,
    content_length_estimate: usize,
    mut payload: Payload,
) -> Result<String, Error> {
    let mut body = web::BytesMut::with_capacity(content_length_estimate);
    while let Some(chunk) = payload.next().await {
        let chunk = chunk?;
        // limit max size of in-memory payload
        if (body.len() + chunk.len()) > MAX_DOCUMENT_SIZE {
            Err(error::ErrorBadRequest("overflow"))?;
        }
        body.extend_from_slice(&chunk);
    }
    std::str::from_utf8(&body.freeze())
        .map_err(|e| {
            log::info!("Failed to parse document for topic {topic_id}: {e:?}");
            error::ErrorBadRequest("invalid_document")
        })
        .map(str::to_string)
}
