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

//! API resource for querying an index for event identifiers.

use crate::rest_api::AppState;
use crate::rest_api::common::ApiErrorMapper;
use actix_web::Error;
use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::get;
use actix_web::http::StatusCode;
use actix_web::web::Data;
use actix_web::web::Path;

/// Query index for event identifiers.
///
/// The index must have been created with an extractor in event descriptor
/// before an event was published for the value to be indexed.
///
/// Consumer identifier is derived from authentication.
#[utoipa::path(
    tag = "http",
    //operation_id = "event_ids_by_topic_and_index",
    params(
        ("topic_id", description = "Topic identifier."),
        ("index_name", description = "The name of the index."),
        ("index_key", description = "The lookup key to use when searching the index."),
    ),
    responses(
        (
            status = 200,
            description = "Array of matching event identifiers.",
            content_type = "application/json",
        ),
        (status = 401, description = "Unauthorized: Authentication failure."),
        (status = 403, description = "Forbidden: Authorization failure."),
        (status = 500, description = "Internal server error."),
    ),
    security(("bearer_auth" = [])),
)]
#[get("/topics/{topic_id}/events/ids_by_index/{index_name}/{index_key}")]
pub async fn event_ids_by_topic_and_index(
    app_state: Data<AppState>,
    path: Path<(String, String, String)>,
    http_request: HttpRequest,
) -> Result<HttpResponse, Error> {
    let identity = app_state
        .auth
        .get_identity(&http_request)
        .map_err(ApiErrorMapper::from_message_broker_error)?;
    let (topic_id, indexed_name, index_key) = path.into_inner();
    let event_ids = app_state
        .mb
        .get_event_ids_by_indexed_column(&identity, &topic_id, &indexed_name, &index_key)
        .await
        .map_err(ApiErrorMapper::from_message_broker_error)?;
    Ok(HttpResponse::build(StatusCode::OK).body(serde_json::to_string_pretty(&event_ids).unwrap()))
}
