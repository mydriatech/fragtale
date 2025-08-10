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

//! API resource to confirm successful delivery of an event.

use crate::rest_api::AppState;
use crate::rest_api::common::ApiErrorMapper;
use actix_web::Error;
use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::http::StatusCode;
use actix_web::route;
use actix_web::web::Data;
use actix_web::web::Path;
use fragtale_core::util::LogScopeDuration;

/// Confirm successful delivery of an event.
///
/// Consumer identifier is derived from authentication.
#[utoipa::path(
    put,
    path = "/topics/{topic_id}/confirm/{unique_time}/{instance_id}",
    tag = "http",
    //operation_id = "confirm_event_delivery",
    responses(
        (status = 204, description = "Successfully confirmed event delivery."),
        (status = 401, description = "Unauthorized: Authentication failure."),
        (status = 403, description = "Forbidden: Authorization failure."),
        (status = 500, description = "Internal server error."),
    ),
    security(("bearer_auth" = [])),
)]
#[route(
    "/topics/{topic_id}/confirm/{unique_time}/{instance_id}",
    method = "PUT",
    name = "confirm_event_delivery"
)]
pub async fn confirm_event_delivery(
    app_state: Data<AppState>,
    path: Path<(String, u64, u16)>,
    http_request: HttpRequest,
) -> Result<HttpResponse, Error> {
    let _ = LogScopeDuration::new(
        log::Level::Trace,
        module_path!(),
        "confirm_event_delivery",
        0,
    );
    let identity = app_state
        .auth
        .get_identity(&http_request)
        .map_err(ApiErrorMapper::from_message_broker_error)?;
    let (topic_id, encoded_unique_time, instance_id) = path.into_inner();
    app_state
        .mb
        .confirm_event_delivery(&identity, &topic_id, encoded_unique_time, instance_id)
        .await
        .map_err(ApiErrorMapper::from_message_broker_error)?;
    Ok(HttpResponse::build(StatusCode::NO_CONTENT).finish())
}
