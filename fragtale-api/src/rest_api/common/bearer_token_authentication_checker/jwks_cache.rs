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

//! JSON Web Key Set cache.

use super::KubernetesIntegration;
use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::map::Entry;
use fragtale_core::mb::MessageBrokerError;
use fragtale_core::mb::MessageBrokerErrorKind;
use jsonwebtoken::jwk::JwkSet;
use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;

/// The JSON Web Key Set
/// ([RFC 7517 5](https://www.rfc-editor.org/rfc/rfc7517#section-5)) is used to
/// validate bearer tokens in a Kubernetes environment.
pub struct JwksCache {
    iss_and_jwks_cache: SkipMap<(), (String, Arc<JwkSet>)>,
}

impl JwksCache {
    /// Return a new instance.
    pub async fn new() -> Result<Arc<Self>, Box<dyn core::error::Error>> {
        let (iss, jwks) = Self::retrieve_open_id_issuer_and_jwks().await?;
        // Initial poplation of of cached values that fail fast.
        let iss_and_jwks_cache = SkipMap::default();
        iss_and_jwks_cache.insert((), (iss, Arc::new(jwks)));
        Ok(Arc::new(Self { iss_and_jwks_cache }).init().await)
    }

    async fn init(self: Arc<Self>) -> Arc<Self> {
        let self_clone = Arc::clone(&self);
        tokio::spawn(async move { self_clone.background_reload_of_jwks().await });
        self
    }

    /// Get cached `iss` and JWKS
    pub fn get_iss_and_jwks(&self) -> Result<(String, Arc<JwkSet>), MessageBrokerError> {
        self.iss_and_jwks_cache
            .front()
            .as_ref()
            .map(Entry::value)
            .cloned()
            .ok_or_else(|| {
                MessageBrokerErrorKind::AuthenticationFailure
                    .error_with_msg("Unable to get cached JWKS.")
            })
    }

    /// Background reloads of JWKS
    async fn background_reload_of_jwks(&self) {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_micros(60_000_000)).await;
            match Self::retrieve_open_id_issuer_and_jwks().await {
                Ok((iss, jwks)) => {
                    self.iss_and_jwks_cache.insert((), (iss, Arc::new(jwks)));
                }
                Err(e) => {
                    log::warn!(
                        "Failed to reload JWKS (last successfully loaded will be used still): {e}"
                    );
                }
            }
        }
    }

    /// Load JWKS from the Kubernetes API.
    async fn retrieve_open_id_issuer_and_jwks()
    -> Result<(String, JwkSet), Box<dyn core::error::Error>> {
        let k8s_api_client = Self::new_k8s_api_client().await?;
        let api_service_account_token = KubernetesIntegration::read_service_account_token().await?;
        // Don't leak this
        //log::trace!("token:  '{service_account_token}'");
        // Get `https://${KUBERNETES_SERVICE_HOST}:${KUBERNETES_SERVICE_PORT_HTTPS}/.well-known/openid-configuration`
        let openid_config_url = KubernetesIntegration::build_openid_config_url()?;
        let openid_config = Self::http_get(
            &k8s_api_client,
            &api_service_account_token,
            &openid_config_url,
        )
        .await?;
        if log::log_enabled!(log::Level::Trace) {
            log::trace!("openid-configuration at '{openid_config_url}': {openid_config}");
        }
        let json_value = serde_json::from_str::<serde_json::Value>(&openid_config)?;
        // Parse out issuer
        let iss = Self::extract_string_from_json(&json_value, "/issuer")?;
        let jwks_uri = Self::extract_string_from_json(&json_value, "/jwks_uri")?;
        // Get jwks_uri
        let jwks = serde_json::from_str(
            &Self::http_get(&k8s_api_client, &api_service_account_token, &jwks_uri).await?,
        )?;
        if log::log_enabled!(log::Level::Trace) {
            log::trace!("OAuth2 machine tokens will be validated using issuer '{iss}'.");
        }
        Ok((iss, jwks))
    }

    /// Extract String value at JSON Pointer from document.
    fn extract_string_from_json(
        json_value: &Value,
        json_pointer: &str,
    ) -> Result<String, Box<dyn core::error::Error>> {
        Ok(json_value
            .pointer(json_pointer)
            .ok_or("Failed to extract '{json_pointer}' from {json_value}.")?
            .as_str()
            .ok_or("Failed to parse value of '{json_pointer}' from {json_value} as String.")
            .map(str::to_string)?)
    }

    /// Return a new REST API client for talking to the K8s API.
    async fn new_k8s_api_client() -> Result<Client, Box<dyn core::error::Error>> {
        let trust_anchors_pem = KubernetesIntegration::read_trust_anchors_pem().await?;
        if log::log_enabled!(log::Level::Trace) {
            log::trace!("Trust anchors for K8S API calls: {trust_anchors_pem}");
        }
        let mut client_builder = reqwest::ClientBuilder::new()
            .use_rustls_tls()
            .tls_built_in_root_certs(false)
            .https_only(true);
        for cert in reqwest::Certificate::from_pem_bundle(trust_anchors_pem.as_bytes())? {
            client_builder = client_builder.add_root_certificate(cert);
        }
        let res = client_builder
            .user_agent("fragtale/0.0.0")
            .referer(false)
            .redirect(reqwest::redirect::Policy::none())
            .pool_max_idle_per_host(1)
            .timeout(core::time::Duration::from_secs(10))
            .build()?;
        Ok(res)
    }

    /// Make request to the K8s API using client.
    async fn http_get(
        client: &Client,
        service_account_token: &str,
        url: &str,
    ) -> Result<String, Box<dyn core::error::Error>> {
        let response = client
            .get(url)
            .bearer_auth(service_account_token)
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::OK {
            let content = response.text().await?;
            if log::log_enabled!(log::Level::Trace) {
                log::trace!("'{url}' -> '{content}'");
            }
            Ok(content)
        } else {
            Err(format!("Get '{url}' failed: {response:?}").as_str().into())
        }
    }
}
