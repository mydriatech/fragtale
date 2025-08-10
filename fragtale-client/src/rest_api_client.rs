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

//! Interactions with `fragtale` using the REST API.

use crate::authentication::BearerTokenCache;
use crate::mb::event_descriptor::EventDescriptor;
use reqwest::Client;
use reqwest::ClientBuilder;
use reqwest::Error;
use reqwest::Response;
use reqwest::StatusCode;
use reqwest::header::AUTHORIZATION;
use reqwest::header::CONTENT_TYPE;
use std::sync::Arc;
use tokio::time::Duration;
use tokio::time::sleep;

/// Client for interacting with `fragtale` using the REST API.
pub struct RestApiClient {
    api_base_url: String,
    // Client uses an Arc internally, so it doesn't need Arc<> wrapping here
    client: Client,
    bearer_token_cache: Arc<BearerTokenCache>,
}
impl RestApiClient {
    const MIME_APPLICATION_JSON: &'static str = "application/json";

    /// Return a new instance.
    pub async fn new(
        api_base_url: &str,
        app_name_lowercase: &str,
        app_version: &str,
        pool_size: usize,
    ) -> Self {
        let bearer_token_cache = BearerTokenCache::new().await;
        let user_agent = format!("{app_name_lowercase}/{app_version}");
        log::debug!("user_agent: {user_agent}. pool_size: {pool_size}");
        let client = ClientBuilder::new()
            .user_agent(user_agent)
            .referer(false)
            .brotli(true)
            .pool_max_idle_per_host(pool_size)
            .timeout(core::time::Duration::from_secs(10))
            //.http2_prior_knowledge()
            .build()
            .unwrap();
        Self {
            api_base_url: api_base_url.to_owned(),
            client,
            bearer_token_cache,
        }
    }

    /// Pre-register information about a topic.
    ///
    /// If the topic did not exist, it will be created.
    ///
    /// Only a single producer should "own" the topic and its description.
    pub async fn register_topic(
        &self,
        topic_id: &str,
        topic_description: Option<EventDescriptor>,
    ) -> Option<String> {
        let client = self.client.clone();
        let url = format!("{}/topics/{}/description", self.api_base_url, topic_id);
        let request_json_string = if let Some(event_descriptor) = topic_description {
            event_descriptor.as_string()
        } else {
            // Use the minimal required EventDescriptor.
            r#"{"version": 0}"#.to_string()
        };
        if log::log_enabled!(log::Level::Debug) {
            log::debug!("Sending body: {request_json_string}");
        }
        let res = client
            .put(&url)
            .body(request_json_string)
            .header(&CONTENT_TYPE, Self::MIME_APPLICATION_JSON)
            .header(
                &AUTHORIZATION,
                self.bearer_token_cache
                    .current_as_header_value()
                    .await
                    .as_str(),
            )
            .send()
            .await;
        Self::get_http_20x_response_body_as_string(res, &url).await
        //.and_then(|json| CountRangeResponse::from_json_string(&json).map(|crr| crr.counts()))
    }

    /// Publish a document to a topic.
    ///
    /// Return correlation-token when successful
    pub async fn publish_document(
        &self,
        publish_to_topic_id: &str,
        document: &str,
        correlation_token: &str,
    ) -> Option<String> {
        let client = self.client.clone();
        let url = format!(
            "{}/topics/{}/events?priority=50",
            self.api_base_url, publish_to_topic_id
        );
        let request_json_string = document.to_owned();
        log::trace!("Sending body: {request_json_string}");
        let result = client
            .put(&url)
            .body(request_json_string)
            .header(&CONTENT_TYPE, Self::MIME_APPLICATION_JSON)
            .header(
                &AUTHORIZATION,
                self.bearer_token_cache
                    .current_as_header_value()
                    .await
                    .as_str(),
            )
            .header("correlation-token", correlation_token)
            .send()
            .await;
        Self::handle_response_err(result, &url).and_then(|response| {
            //if response.status() == StatusCode::NO_CONTENT {}
            Self::header_as_string(&response, "correlation-token")
        })
    }

    /// Publish a document to a topic (`publish_to_topic_id`) and wait for a
    /// correlated event to be consumed from another topic
    /// (`consume_from_topic_id`).
    pub async fn publish_and_await_result(
        &self,
        publish_to_topic_id: &str,
        consume_from_topic_id: &str,
        document: &str,
    ) -> Option<String> {
        let client = self.client.clone();
        let url = format!(
            "{}/topics/{}/events?priority=50&target={}",
            self.api_base_url, publish_to_topic_id, consume_from_topic_id,
        );
        let request_json_string = document.to_owned();
        if log::log_enabled!(log::Level::Trace) {
            log::trace!("Sending body: {request_json_string}");
        }
        let result = client
            .put(&url)
            .body(request_json_string)
            .header(&CONTENT_TYPE, Self::MIME_APPLICATION_JSON)
            .header(
                &AUTHORIZATION,
                self.bearer_token_cache
                    .current_as_header_value()
                    .await
                    .as_str(),
            )
            .send()
            .await;
        let mut location_header_content = None;
        if let Ok(response) = result.map_err(|e| {
            log::info!("Failed request to {url}: {:?}", e.without_url());
        }) {
            match response.status() {
                StatusCode::OK => {
                    return response
                        .text()
                        .await
                        .map_err(|e| {
                            log::info!(
                            "Failed request to {url}: Failed to parse response body as text: {:?}",
                            e.without_url()
                        );
                        })
                        .ok();
                }
                StatusCode::SEE_OTHER => {
                    location_header_content = Self::header_as_string(&response, "location");
                    #[allow(clippy::question_mark)]
                    if location_header_content.is_none() {
                        return None;
                    }
                }
                _ => {
                    log::info!("Unhandled response from {url}: {}", response.status());
                    return None;
                }
            }
        }
        if let Some(location_header_content) = location_header_content {
            // Poll for result
            for i in 0..10 {
                let result = client.get(&location_header_content).send().await;
                if let Some(document) =
                    Self::get_http_20x_response_body_as_string(result, &url).await
                {
                    return Some(document);
                }
                // Back off before polling again
                sleep(Duration::from_millis(1000)).await;
                log::info!(
                    "Failed to get any result for correlation_token on topic '{consume_from_topic_id}'. Retry {i}/10 pending."
                );
            }
        }
        log::info!("Failed to get any result on topic '{consume_from_topic_id}'.");
        None
    }

    /// Get the next available document from a topic.
    pub async fn get_next_document(&self, topic_id: &str) -> Option<(String, String, String)> {
        let client = self.client.clone();
        let url = format!("{}/topics/{topic_id}/next?from=0", self.api_base_url);
        let result_res = client
            .get(&url)
            .header(
                &AUTHORIZATION,
                self.bearer_token_cache
                    .current_as_header_value()
                    .await
                    .as_str(),
            )
            .send()
            .await;
        if result_res.is_err() {
            // Back off a little on network failures
            sleep(Duration::from_millis(512)).await;
        }
        let result_opt = result_res
            .map_err(|e| {
                log::info!("Failed request to {url}: {:?}", e.without_url());
            })
            .ok();
        if let Some(response) = result_opt
            && response.status() == StatusCode::OK
        {
            let confirmation_link =
                Self::header_as_string(&response, "link").and_then(|header_value| {
                    log::trace!("Link: {header_value}");
                    header_value
                        .split(';')
                        .next()
                        .map(|link_str| link_str.replace(['<', '>'], "").to_owned())
                });
            let correlation_token = Self::header_as_string(&response, "correlation-token");
            let document = response
                .text()
                .await
                .map_err(|e| {
                    log::info!(
                        "Failed request to {url}: Failed to parse response body as text: {:?}",
                        e.without_url()
                    );
                })
                .ok();
            if let Some(confirmation_link) = confirmation_link
                && let Some(document) = document
                && let Some(correlation_token) = correlation_token
            {
                return Some((document, confirmation_link, correlation_token));
            }
        }
        None
    }

    /// Confirm event delivery.
    pub async fn confirm_delivery(&self, url: &str) {
        if log::log_enabled!(log::Level::Trace) {
            log::trace!("Confirming delivery with PUT '{url}'.");
        }
        self.client
            .clone()
            .put(url)
            .header(
                &AUTHORIZATION,
                self.bearer_token_cache
                    .current_as_header_value()
                    .await
                    .as_str(),
            )
            .send()
            .await
            .map_err(|e| {
                log::info!("Failed request to {url}: {:?}", e.without_url());
            })
            .ok();
    }

    /// Query topic for an event document with the specified event identifier.
    pub async fn event_by_topic_and_event_id(
        &self,
        topic_id: &str,
        event_id: &str,
    ) -> Option<String> {
        let client = self.client.clone();
        let url = format!(
            "{}/topics/{topic_id}/events/by_event_id/{event_id}",
            self.api_base_url
        );
        let result = client
            .get(&url)
            .header(
                &AUTHORIZATION,
                self.bearer_token_cache
                    .current_as_header_value()
                    .await
                    .as_str(),
            )
            .send()
            .await;
        Self::get_http_20x_response_body_as_string(result, &url).await
    }

    /// Get all event identifiers where `index_name` exactly has `index_key`
    /// entries.
    pub async fn event_ids_by_topic_and_index(
        &self,
        topic_id: &str,
        index_name: &str,
        index_key: &str,
    ) -> Vec<String> {
        let client = self.client.clone();
        let url = format!(
            "{}/topics/{topic_id}/events/ids_by_index/{index_name}/{index_key}",
            self.api_base_url
        );
        let result = client
            .get(&url)
            .header(
                &AUTHORIZATION,
                self.bearer_token_cache
                    .current_as_header_value()
                    .await
                    .as_str(),
            )
            .send()
            .await;
        Self::get_http_20x_response_body_as_string(result, &url)
            .await
            .and_then(|content| {
                serde_json::from_str(&content)
                    .map_err(|e| {
                        log::info!("Failed to parse JSON response from '{url}': {e:?}");
                    })
                    .ok()
            })
            .unwrap_or_default()
    }

    /// Return reposonse body as text when present if HTTP status code is 200 or 201.
    async fn get_http_20x_response_body_as_string(
        result: Result<Response, Error>,
        url: &str,
    ) -> Option<String> {
        match Self::handle_response_err(result, url).map(|response| (response.status(), response)) {
            Some((StatusCode::OK, response)) => {
                return response
                    .text()
                    .await
                    .map_err(|e| {
                        log::info!(
                            "Failed request to '{url}': Failed to parse response body as text: {:?}",
                            e.without_url()
                        );
                    })
                    .ok();
            }
            Some((StatusCode::NO_CONTENT, _)) => (),
            Some((status_code, _)) => {
                log::info!("Failed request to {url}: status_code {status_code}.");
            }
            _ => (),
        }
        None
    }

    fn header_as_string(response: &Response, header_key: &str) -> Option<String> {
        response
            .headers()
            .get(header_key)
            .map(|header_value| header_value.to_str().unwrap_or("").to_owned())
    }

    /// Log any error and return the response if present
    fn handle_response_err(result: Result<Response, Error>, url: &str) -> Option<Response> {
        result
            .map_err(|e| {
                log::info!("Failed request to '{url}': {:?}", e.without_url());
            })
            .ok()
    }
}
