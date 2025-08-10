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

//! Metrics API resource.

use actix_web::HttpResponse;
use actix_web::Responder;
use actix_web::get;

/*
https://prometheus.io/docs/instrumenting/content_negotiation/
```
Protocol                MIME Type                       Parameters
---                     ---                             ---
PrometheusProto         application/vnd.google.protobuf proto=io.prometheus.client.MetricFamily;encoding=delimited
PrometheusText0.0.4     text/plain                      version=0.0.4
PrometheusText1.0.0     text/plain                      version=1.0.0
OpenMetricsText0.0.1    application/openmetrics-text    version=0.0.1
OpenMetricsText1.0.0    application/openmetrics-text    version=1.0.0
```
*/

/// Provides metrics in the `PrometheusText0.0.4` format.
#[utoipa::path(
    responses(
        (
            status = 200,
            description = "Ok. Sending PrometheusText0.0.4.",
            content_type = "text/plain; version=0.0.4",
        ),
        (status = 500, description = "Internal server error"),
    ),
)]
#[get("/metrics")]
pub async fn metrics() -> impl Responder {
    HttpResponse::Ok()
        .content_type("text/plain; version=0.0.4")
        .message_body(
            crate::registry::MetricsProviderRegistry::get_metrics()
                .await
                .as_text(),
        )
}
