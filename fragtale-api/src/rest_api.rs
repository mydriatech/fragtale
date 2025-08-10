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

//! REST API server and resources.
//!
//! API types:
//!
//! 1. REST API with long hanging pull with optional ACK-only payload.
//! 2. Actorless WebSocket duplex channel initiated by client.
//!
//! TODO: WebTransport over HTTP/3 like [wtransport](https://github.com/BiagioFesta/wtransport)
//! when K8s Gateway API w h3 support is standard.

mod http_resources {
    //! API resources

    pub mod confirm_delivery;
    pub mod event_by_correlation_resource;
    pub mod event_by_id_resource;
    pub mod event_description_resource;
    pub mod event_ids_by_index_resource;
    pub mod event_poll_resource;
    pub mod publish_resource;
}
mod common {
    //! Common RESP API resources and utils.

    mod api_error_mapper;
    mod bearer_token_authentication_checker;
    mod next_query_params;
    mod utoipa_security_scheme_modifier;

    pub use api_error_mapper::*;
    pub use bearer_token_authentication_checker::*;
    pub use next_query_params::NextQueryParams;
    pub use utoipa_security_scheme_modifier::*;
}
//mod health_resources;
mod ws_resources {
    //! WebSocket resources.
    //!
    //! Separate end-points avoids starvation of the different message types.
    //!
    //! IP provides 60k ports, so there is still room for many clients using
    //! connection pools with multiple connections per client.

    pub mod ws_confirm_resource;
    pub mod ws_publish_resource;
    pub mod ws_subscribe_resource;
}

use self::common::BearerTokenAuthenticationChecker;
use self::common::UtopiaSecuritySchemeModifier;
use actix_web::App;
use actix_web::HttpResponse;
use actix_web::HttpServer;
use actix_web::Responder;
use actix_web::get;
use actix_web::http::header::ContentType;
use actix_web::web;
use fragtale_core::conf::AppConfig;
use fragtale_core::mb::MessageBroker;
use std::sync::Arc;
use tyst_api_rest_health::AppHealth;
use tyst_api_rest_health::health_resources;
use utoipa::OpenApi;

/// Number of parallel requests the can be served for each assigned CPU core.
const WORKERS_PER_CORE: usize = 1024;

/// Shared state between requests.
#[derive(Clone)]
struct AppState {
    mb: Arc<MessageBroker>,
    auth: Arc<BearerTokenAuthenticationChecker>,
}

/// Simple health check that gets the provider instance.
pub struct MessageBrokerHealth {
    mb: Arc<MessageBroker>,
}
impl MessageBrokerHealth {
    fn with_app(mb: &Arc<MessageBroker>) -> Arc<dyn AppHealth> {
        Arc::new(Self { mb: Arc::clone(mb) })
    }
}
impl AppHealth for MessageBrokerHealth {
    fn is_health_started(&self) -> bool {
        self.mb.is_health_started()
    }
    fn is_health_ready(&self) -> bool {
        self.mb.is_health_ready()
    }
    fn is_health_live(&self) -> bool {
        self.mb.is_health_live()
    }
}

/// Run HTTP server.
pub async fn run_http_server(
    app_config: &Arc<AppConfig>,
    mb: &Arc<MessageBroker>,
) -> Result<(), Box<dyn core::error::Error>> {
    let app_config = Arc::clone(app_config);
    let auth = BearerTokenAuthenticationChecker::new(app_config.api.audience()).await?;
    let workers = app_config.limits.available_parallelism();
    let max_connections = WORKERS_PER_CORE * workers;
    log::info!(
        "API described by http://{}:{}/openapi.json allows {max_connections} concurrent connections.",
        &app_config.api.bind_address(),
        &app_config.api.bind_port(),
    );
    let app_state: AppState = AppState {
        mb: Arc::clone(mb),
        auth,
    };
    let app_data = web::Data::<AppState>::new(app_state);
    let app_health = web::Data::<Arc<dyn AppHealth>>::new(MessageBrokerHealth::with_app(mb));

    HttpServer::new(move || {
        let scope = web::scope("/api/v1")
            .service(get_openapi)
            .service(http_resources::event_description_resource::topic_event_description_upsert)
            .service(http_resources::publish_resource::publish_event_to_topic)
            .service(http_resources::event_poll_resource::next_event_by_topic_and_consumer)
            .service(http_resources::confirm_delivery::confirm_event_delivery)
            .service(http_resources::event_by_correlation_resource::by_topic_and_correlation_token)
            .service(http_resources::event_by_id_resource::event_by_topic_and_id)
            .service(http_resources::event_ids_by_index_resource::event_ids_by_topic_and_index)
            .service(ws_resources::ws_subscribe_resource::subscribe_to_topic)
            .service(ws_resources::ws_confirm_resource::confirm_event_delivery)
            .service(ws_resources::ws_publish_resource::publish_event_to_topic);
        App::new()
            .app_data(app_data.clone())
            .app_data(app_health.clone())
            .service(web::redirect("/openapi", "/api/v1/openapi.json"))
            .service(web::redirect("/openapi.json", "/api/v1/openapi.json"))
            .service(scope)
            .service(health_resources::health)
            .service(health_resources::health_live)
            .service(health_resources::health_ready)
            .service(health_resources::health_started)
            .service(fragtale_metrics::http_metrics_resource::metrics)
    })
    .workers(workers)
    .backlog(u32::try_from(max_connections / 2).unwrap()) // Default is 2048
    .worker_max_blocking_threads(max_connections)
    .max_connections(max_connections)
    .bind_auto_h2c((app_config.api.bind_address(), app_config.api.bind_port()))?
    .disable_signals()
    .shutdown_timeout(5) // Default 30
    .run()
    .await?;
    Ok(())
}

/// Serve Open API documentation.
#[get("/openapi.json")]
async fn get_openapi() -> impl Responder {
    HttpResponse::Ok()
        .content_type(ContentType::json())
        .body(openapi_as_string())
}

/// Get the OpenAPI definition as a pretty JSON String.
pub fn openapi_as_string() -> String {
    #[derive(OpenApi)]
    #[openapi(
        // Use Cargo.toml as source for the "info" section
        modifiers(&UtopiaSecuritySchemeModifier),
        paths(
            http_resources::event_description_resource::topic_event_description_upsert,
            http_resources::publish_resource::publish_event_to_topic,
            http_resources::event_poll_resource::next_event_by_topic_and_consumer,
            http_resources::confirm_delivery::confirm_event_delivery,
            http_resources::event_by_correlation_resource::by_topic_and_correlation_token,
            http_resources::event_by_id_resource::event_by_topic_and_id,
            http_resources::event_ids_by_index_resource::event_ids_by_topic_and_index,
            ws_resources::ws_subscribe_resource::subscribe_to_topic,
            ws_resources::ws_confirm_resource::confirm_event_delivery,
            ws_resources::ws_publish_resource::publish_event_to_topic,
            health_resources::health,
            health_resources::health_live,
            health_resources::health_ready,
            health_resources::health_started,
            fragtale_metrics::http_metrics_resource::metrics,
        )
    )]
    struct ApiDoc;
    ApiDoc::openapi().to_pretty_json().unwrap()
}
