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

//! Validate authentication with Bearer tokens.

mod jwks_cache;
mod kubernetes_integration;

use self::jwks_cache::JwksCache;
use self::kubernetes_integration::KubernetesIntegration;
use actix_web::HttpRequest;
use actix_web::http::header::HeaderValue;
use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::map::Entry;
use fragtale_core::mb::MessageBrokerError;
use fragtale_core::mb::MessageBrokerErrorKind;
use fragtale_core::mb::auth::ClientIdentity;
use jsonwebtoken::DecodingKey;
use jsonwebtoken::TokenData;
use jsonwebtoken::Validation;
use jsonwebtoken::jwk::AlgorithmParameters;
use jsonwebtoken::jwk::JwkSet;
use serde_json::Value;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

/// Validates authentication using Bearer tokens.
///
/// This is tailored for Kubernetes environments where the other microservice
/// can use a projected service account token to authenticate to fragtale in the
/// same cluster with very little configuration.
pub struct BearerTokenAuthenticationChecker {
    client_identity_by_bearer_token: SkipMap<String, (u64, Arc<ClientIdentity>)>,
    jwks_cache: Arc<JwksCache>,
    aud: String,
    local_service_account_token_sub: String,
}

impl BearerTokenAuthenticationChecker {
    const BEARER_TOKEN: &str = "Bearer";

    pub async fn new(aud: &str) -> Result<Arc<Self>, Box<dyn core::error::Error>> {
        let jwks_cache = JwksCache::new().await?;
        let (iss, jwks) = jwks_cache.get_iss_and_jwks()?;
        let local_service_account_token_sub = Self::get_local_subject(&jwks, &iss, aud).await?;
        let iss_and_jwks_cache = SkipMap::default();
        iss_and_jwks_cache.insert((), (iss, Arc::new(jwks)));
        Ok(Arc::new(Self {
            client_identity_by_bearer_token: SkipMap::default(),
            jwks_cache,
            aud: aud.to_string(),
            local_service_account_token_sub,
        })
        .init()
        .await)
    }

    async fn init(self: Arc<Self>) -> Arc<Self> {
        let self_clone = Arc::clone(&self);
        tokio::spawn(async move { self_clone.purge_expired_cached_client_identities().await });
        self
    }

    /// Purge old and expired bearer tokens from cache.
    ///
    /// Validity is still verified on use and expired tokens in use are removed.
    ///
    /// Note that unused tokens remain in the cache until they expire for now.
    async fn purge_expired_cached_client_identities(&self) {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_micros(60_000_000)).await;
            let now = fragtale_client::time::get_timestamp_micros();
            for entry in self.client_identity_by_bearer_token.iter() {
                if entry.value().0 < now {
                    entry.remove();
                }
            }
        }
    }

    /// Get the local projected volume service account's `sub`.
    async fn get_local_subject(
        jwks: &JwkSet,
        iss: &str,
        aud: &str,
    ) -> Result<String, Box<dyn core::error::Error>> {
        // Parse and validate JWT
        let local_service_account_token =
            KubernetesIntegration::read_service_account_token_projected().await?;
        let local_token_data =
            Self::validate_bearer_token(iss, jwks, aud, &local_service_account_token).map_err(
                |e| {
                    MessageBrokerErrorKind::AuthenticationFailure.error_with_msg(format!(
                        "Local service account bearer token validation failed: {e}"
                    ))
                },
            )?;
        if log::log_enabled!(log::Level::Trace) {
            log::trace!("local_token_data: {local_token_data:?}");
        }
        // Extract sub for checking if token is local later. It would also be
        // possible to just compare the entire file, but then this needs a
        // reload as well as the JWT.
        Ok(Self::extract_sub(&local_token_data)?)
    }

    /// Extract `sub` claim from [TokenData].
    fn extract_sub(
        token_data: &TokenData<HashMap<String, Value>>,
    ) -> Result<String, MessageBrokerError> {
        token_data
            .claims
            .get("sub")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                MessageBrokerErrorKind::MalformedIdentifier
                    .error_with_msg("Missing or non-string 'sub' in bearer token.")
            })
            .map(str::to_string)
    }

    /// Extract `exp` claim from [TokenData].
    fn extract_exp_seconds(
        token_data: &TokenData<HashMap<String, Value>>,
    ) -> Result<u64, MessageBrokerError> {
        token_data
            .claims
            .get("exp")
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                MessageBrokerErrorKind::MalformedIdentifier
                    .error_with_msg("Missing or non-unsigned integer 'exp' in bearer token.")
            })
    }

    /// Return the bearer token's (`iss`,`sub`) or `error::ErrorUnauthorized` (401)
    pub fn get_identity(
        &self,
        http_request: &HttpRequest,
    ) -> Result<Arc<ClientIdentity>, MessageBrokerError> {
        let authorization_header = http_request
            .headers()
            .get(actix_web::http::header::AUTHORIZATION)
            .map(HeaderValue::to_str)
            .map(|res| {
                res.map_err(|e| {
                    MessageBrokerErrorKind::AuthenticationFailure
                        .error_with_msg(format!("Invalid 'Authorization' HTTP header: {e}"))
                })
            })
            .unwrap_or_else(|| {
                Err(MessageBrokerErrorKind::AuthenticationFailure
                    .error_with_msg("Missing 'Authorization' HTTP header."))
            })?
            .trim();
        // Extract Bearer token
        let bearer_token = authorization_header
            .strip_prefix(Self::BEARER_TOKEN)
            .map(|s| s.trim_start_matches(':'))
            .ok_or_else(|| {
                MessageBrokerErrorKind::AuthenticationFailure
                    .error_with_msg("Missing 'Authorization' HTTP header.")
            })?
            .trim();
        if log::log_enabled!(log::Level::Trace) {
            let decoded = bearer_token
                .split('.')
                .map(|part| {
                    String::from_utf8(tyst::encdec::base64::decode_url(part).unwrap_or_default())
                        .unwrap_or_default()
                        + "."
                })
                .collect::<String>();
            log::trace!("Bearer: {bearer_token} -> {decoded}");
        }
        let now_micros = fragtale_client::time::get_timestamp_micros();
        let mut delete_from_cache_on_fail = false;
        if let Some((expires_micros, client_identity)) = self
            .client_identity_by_bearer_token
            .get(bearer_token)
            .as_ref()
            .map(Entry::value)
        {
            if expires_micros < &now_micros {
                delete_from_cache_on_fail = true;
            } else {
                return Ok(Arc::clone(client_identity));
            }
        }
        // Parse and validate JWT
        let (iss, jwks) = self.jwks_cache.get_iss_and_jwks()?;
        let token_data = Self::validate_bearer_token(&iss, &jwks, &self.aud, bearer_token)?;
        if log::log_enabled!(log::Level::Trace) {
            log::trace!("token_data: {token_data:?}");
        }
        let sub = Self::extract_sub(&token_data)?;
        let exp_seconds = Self::extract_exp_seconds(&token_data)?;
        let local_authentication = self.local_service_account_token_sub.eq(&sub);
        let ret = Arc::new(
            ClientIdentity::from_bearer_token_claims(token_data.claims, local_authentication)
                .map_err(|e| {
                    MessageBrokerErrorKind::AuthenticationFailure.error_with_msg(e.to_string())
                })
                .inspect_err(|_e| {
                    if delete_from_cache_on_fail {
                        self.client_identity_by_bearer_token.remove(bearer_token);
                    }
                })?,
        );
        self.client_identity_by_bearer_token.insert(
            bearer_token.to_owned(),
            (exp_seconds * 1_000_000, Arc::clone(&ret)),
        );
        Ok(ret)
    }

    /// Validate the bearer token validity using the provided JSON Web Key set.
    fn validate_bearer_token(
        iss: &str,
        jwks: &JwkSet,
        aud: &str,
        token: &str,
    ) -> Result<TokenData<HashMap<String, serde_json::Value>>, MessageBrokerError> {
        let header = jsonwebtoken::decode_header(token).map_err(|e| {
            MessageBrokerErrorKind::AuthenticationFailure
                .error_with_msg(format!("Failed to decode authorization token: {e}"))
        })?;
        let kid = header.kid.ok_or_else(|| {
            MessageBrokerErrorKind::AuthenticationFailure
                .error_with_msg("Missing 'kid' in token header field")
        })?;
        let jwk = jwks.find(&kid).ok_or_else(|| {
            MessageBrokerErrorKind::AuthenticationFailure
                .error_with_msg(format!("JWKS has no 'kid' with value '{kid}'."))
        })?;
        let decoding_key = match &jwk.algorithm {
            AlgorithmParameters::RSA(rsa) => DecodingKey::from_rsa_components(&rsa.n, &rsa.e)
                .map_err(|e| {
                    MessageBrokerErrorKind::AuthenticationFailure.error_with_msg(format!(
                        "Failed to construct RSA key for bearer token validation: {e}"
                    ))
                })?,
            AlgorithmParameters::EllipticCurve(ec) => DecodingKey::from_ec_components(&ec.x, &ec.y)
                .map_err(|e| {
                    MessageBrokerErrorKind::AuthenticationFailure.error_with_msg(format!(
                        "Failed to construct EC key for bearer token validation: {e}"
                    ))
                })?,
            unsupported => Err(MessageBrokerErrorKind::AuthenticationFailure
                .error_with_msg(format!("Currently '{unsupported:?}' is not supported.")))?,
        };
        let validation = {
            let mut validation = Validation::new(header.alg);
            validation.set_audience(&[aud]);
            validation.validate_exp = true;
            validation.validate_aud = true;
            // Check that we have the expected "iss" (e.g. "https://kubernetes.default.svc")
            validation.iss = Some(HashSet::from_iter([iss.to_string()]));
            validation.required_spec_claims.insert("iss".to_string());
            validation
        };
        jsonwebtoken::decode::<HashMap<String, serde_json::Value>>(
            token,
            &decoding_key,
            &validation,
        )
        .map_err(|e| {
            MessageBrokerErrorKind::AuthenticationFailure.error_with_msg(format!(
                "Decode claims for bearer token validation failed: {e}"
            ))
        })
    }
}
