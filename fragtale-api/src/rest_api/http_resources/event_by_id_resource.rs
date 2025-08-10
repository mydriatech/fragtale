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

//! API resource for retrieving an event document by its identifier.

use crate::rest_api::AppState;
use crate::rest_api::common::ApiErrorMapper;
use actix_web::Error;
use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::get;
use actix_web::http::StatusCode;
use actix_web::web::Data;
use actix_web::web::Path;

/// Retrieve an event document by its identifier.
///
/// Consumer identifier is derived from authentication.
#[utoipa::path(
    tag = "http",
    //operation_id = "event_by_topic_and_id",
    params(
        ("topic_id", description = "Topic identifier."),
        ("index_name", description = "The name of the index."),
        (
            "index_key",
            description = "The lookup key to use when searching the index."
        ),
    ),
    responses(
        (
            status = 200,
            description = "Return the event document.",
            content_type = "application/json",
        ),
        (status = 401, description = "Unauthorized: Authentication failure."),
        (status = 403, description = "Forbidden: Authorization failure."),
        (
            status = 404,
            description = "No event document with the event identifier was found.",
        ),
        (status = 500, description = "Internal server error."),
    ),
    security(("bearer_auth" = [])),
)]
#[get("/topics/{topic_id}/events/by_event_id/{event_id}")]
pub async fn event_by_topic_and_id(
    app_state: Data<AppState>,
    path: Path<(String, String)>,
    http_request: HttpRequest,
) -> Result<HttpResponse, Error> {
    let identity = app_state
        .auth
        .get_identity(&http_request)
        .map_err(ApiErrorMapper::from_message_broker_error)?;
    let (topic_id, event_id) = path.into_inner();
    let event_document_opt = app_state
        .mb
        .get_event_by_id(&identity, &topic_id, &event_id)
        .await
        .map_err(ApiErrorMapper::from_message_broker_error)?;
    if let Some(event_document) = event_document_opt {
        Ok(HttpResponse::build(StatusCode::OK).body(event_document))
    } else {
        Ok(HttpResponse::build(StatusCode::NOT_FOUND).finish())
    }
}
