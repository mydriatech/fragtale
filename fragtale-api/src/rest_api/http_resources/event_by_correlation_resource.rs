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

//! API resource to retrieve event document related to another event.

use crate::rest_api::AppState;
use crate::rest_api::common::ApiErrorMapper;
use actix_web::Error;
use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::http::StatusCode;
use actix_web::route;
use actix_web::web::Data;
use actix_web::web::Path;

/// Retrieve event document related to another event.
///
/// This enables treating the API as an RPC-style REST API with a PRG twist,
/// pubishing an event can be seen as a request and following the link will
/// return an event from a different topic as a result as long as this happens
/// within a reasonable timeout.
///
/// Consumer identifier is derived from authentication.
#[utoipa::path(
    get,
    path = "/topics/{topic_id}/correlation/{correlation_token}",
    tag = "http",
    //operation_id = "by_topic_and_correlation_token",
    responses(
        (
            status = 200,
            description = "Event document is delivered in the response body.",
            content_type = "application/json",
        ),
        (
            status = 204,
            description = "No correlated event document has appeared within the timeout."
        ),
        (status = 401, description = "Unauthorized: Authentication failure."),
        (status = 403, description = "Forbidden: Authorization failure."),
        (status = 500, description = "Internal server error."),
    ),
    security(("bearer_auth" = [])),
)]
#[route(
    "/topics/{topic_id}/correlation/{correlation_token}",
    method = "GET",
    name = "by_topic_and_correlation_token"
)]
//#[get("/topics/{topic_id}/correlation/{correlation_token}")]
pub async fn by_topic_and_correlation_token(
    app_state: Data<AppState>,
    path: Path<(String, String)>,
    http_request: HttpRequest,
) -> Result<HttpResponse, Error> {
    let identity = app_state
        .auth
        .get_identity(&http_request)
        .map_err(ApiErrorMapper::from_message_broker_error)?;
    let (topic_id, correlation_token_str) = path.into_inner();
    let event_document_opt = app_state
        .mb
        .get_event_by_correlation_token(&identity, &topic_id, &correlation_token_str)
        .await
        .map_err(ApiErrorMapper::from_message_broker_error)?;
    if let Some(event_document) = event_document_opt {
        Ok(HttpResponse::build(StatusCode::OK).body(event_document))
    } else {
        Ok(HttpResponse::build(StatusCode::NO_CONTENT).finish())
    }
}
