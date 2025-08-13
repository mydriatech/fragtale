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

//! API resource to poll for new events.

use crate::rest_api::AppState;
use crate::rest_api::common::ApiErrorMapper;
use crate::rest_api::common::NextQueryParams;
use actix_web::Error;
use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::error;
use actix_web::get;
use actix_web::http::StatusCode;
use actix_web::web::Data;
use actix_web::web::Path;
use actix_web::web::Query;
use fragtale_core::util::LogScopeDuration;

/// Poll for new events.
///
/// Consumer identifier is derived from authentication.
#[utoipa::path(
    tag = "http",
    //operation_id = "next_event_by_topic_and_consumer",
    params(
        (
            "topic_id",
            description = "Topic identifier."
        ),
        (
            "from" = Option<u64>,
            Query,
            description = "Only consider events newer than this in epoch milliseconds."
        ),
        (
            "version" = Option<String>,
            Query,
            description = "Event Descriptor SemVer that the client prefers (major.minor)."
        ),
    ),
    responses(
        (
            status = 200,
            description = "A new event is delivered in the response body.",
            links(
                (
                    "Location" = (
                        operation_id = "confirm_event_delivery",
                        parameters(
                            ("topic_id" = "string"),
                            ("correlation_token" = "string"),
                            ("unique_time" = "number"),
                            ("instance_id" = "number")
                        ),
                    )
                ),
            ),
        ),
        (status = 204, description = "No new event was found."),
        (status = 400, description = "Bad Request."),
        (status = 401, description = "Unauthorized: Authentication failure."),
        (status = 403, description = "Forbidden: Authorization failure."),
    ),
    security(("bearer_auth" = [])),
)]
#[get("/topics/{topic_id}/next")]
pub async fn next_event_by_topic_and_consumer(
    app_state: Data<AppState>,
    path: Path<String>,
    query: Query<NextQueryParams>,
    http_request: HttpRequest,
) -> Result<HttpResponse, Error> {
    let _ = LogScopeDuration::new(
        log::Level::Trace,
        module_path!(),
        "next_event_by_topic_and_consumer",
        0,
    );
    let identity = app_state
        .auth
        .get_identity(&http_request)
        .map_err(ApiErrorMapper::from_message_broker_error)?;
    let topic_id = path.into_inner();
    let next_query_params = query.into_inner();
    let baseline_micros = next_query_params.get_from_epoch_micros();
    // Respect consumers version support to avoid (too new) incompatibel messages
    let descriptor_version = next_query_params.get_descriptor_version()?;
    let event_opt = app_state
        .mb
        .get_event_by_consumer_and_topic(&identity, &topic_id, baseline_micros, descriptor_version)
        .await
        .map_err(|e| error::ErrorInternalServerError(e.to_string()))?;
    if let Some((unique_time, event_document, correlation_token, instance_id)) = event_opt {
        let confirmation_url = http_request
            .url_for(
                "confirm_event_delivery",
                [topic_id, unique_time.to_string(), instance_id.to_string()],
            )
            .unwrap();
        // TODO: Work-around apparent bug where the 2nd and 3rd path args are dropped.
        let confirmation_url = format!("{confirmation_url}/{unique_time}/{instance_id}");
        Ok(HttpResponse::build(StatusCode::OK)
            .append_header((
                "Link",
                format!(r#"<{confirmation_url}>;rel="confirm-delivery""#),
            ))
            .append_header(("correlation-token", correlation_token))
            .body(event_document))
    } else {
        Ok(HttpResponse::build(StatusCode::NO_CONTENT).finish())
    }
}
