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

//! API resource for topic pre-registration.

use crate::rest_api::AppState;
use crate::rest_api::common::ApiErrorMapper;
use actix_web::Error;
use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::http::StatusCode;
use actix_web::put;
use actix_web::web;
use actix_web::web::Data;
use actix_web::web::Path;
use fragtale_client::mb::event_descriptor::EventDescriptor;

/// Upsert topic's event description.
///
/// Use this call to specify an even document schema that will be used for
/// validation or extractors to index values in the event document to enable
/// queries.
///
/// The validation and extraction operates on write (publishing of new events)
/// and has no effect on historic events.
///
/// Publisher identifier is derived from authentication.
#[utoipa::path(
    tag = "http",
    //operation_id = "topic_description_upsert",
    request_body = inline(EventDescriptor),
    responses(
        (
            status = 204,
            description = "Successfully updated topic's event description."
        ),
        (status = 400, description = "Bad Request."),
        (status = 401, description = "Unauthorized: Authentication failure."),
        (status = 403, description = "Forbidden: Authorization failure."),
        (status = 500, description = "Internal server error."),
    ),
    security(("bearer_auth" = [])),
)]
#[put("/topics/{topic_id}/description")]
pub async fn topic_event_description_upsert(
    app_state: Data<AppState>,
    path: Path<String>,
    ted: web::Json<EventDescriptor>,
    http_request: HttpRequest,
) -> Result<HttpResponse, Error> {
    let identity = app_state
        .auth
        .get_identity(&http_request)
        .map_err(ApiErrorMapper::from_message_broker_error)?;
    let topic_id = path.into_inner();
    app_state
        .mb
        .upsert_topic_event_descriptor(&identity, &topic_id, ted.into_inner())
        .await
        .map_err(ApiErrorMapper::from_message_broker_error)
        .map(|_| HttpResponse::build(StatusCode::NO_CONTENT).finish())
}
