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

//! Client bearer token parser and cache.

use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::map::Entry;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::time::Duration;
use tokio::time::sleep;

/// Client bearer token parser and cache to populate `Authentication` header
/// in HTTP requests.
///
/// The bearer token is loaded by default loaded from
/// `/var/run/secrets/tokens/service-account`, but this can be controlled by
/// setting the environment variable `BEARER_TOKEN_FILENAME`.
pub struct BearerTokenCache {
    service_account_secret_file: String,
    bearer_token: SkipMap<(), Arc<String>>,
}

impl BearerTokenCache {
    const ENV: &str = "BEARER_TOKEN_FILENAME";
    const DEFAULT: &str = "/var/run/secrets/tokens/service-account";

    /// Return a new instance.
    pub async fn new() -> Arc<Self> {
        let service_account_secret_file = std::env::var(Self::ENV)
            .map_err(|e| log::debug!("Unable to parse environment variable '{}': {e}", Self::ENV))
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| {
                log::debug!(
                    "Using default value '{}' for environment variable '{}'.",
                    Self::DEFAULT,
                    Self::ENV,
                );
                Self::DEFAULT.to_string()
            });
        Arc::new(Self {
            service_account_secret_file,
            bearer_token: SkipMap::default(),
        })
        .init()
        .await
    }

    /// Start background task for reloading bearer token file.
    async fn init(self: Arc<Self>) -> Arc<Self> {
        let ret = Arc::clone(&self);
        self.reload_bearer_token().await;
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_micros(60_000_000)).await;
                self.reload_bearer_token().await;
            }
        });
        ret
    }

    /// Return the `Bearer {bearer_token}` value used to populate the HTTP
    /// `Authorization` header.
    pub async fn current_as_header_value(&self) -> String {
        self.bearer_token
            .get(&())
            .as_ref()
            .map(Entry::value)
            .map(Arc::clone)
            .map(|bearer_token| format!("Bearer {bearer_token}"))
            .unwrap_or_else(|| "None".to_string())
    }

    /// Perform background reload of the cached bearer token.
    async fn reload_bearer_token(&self) -> bool {
        Self::read_file_text(&self.service_account_secret_file)
            .await
            .map_err(|e| {
                log::warn!(
                    "Unable to load bearer token from '{}': {e}",
                    &self.service_account_secret_file
                )
            })
            .ok()
            .map(|service_account_secret| {
                if self
                    .bearer_token
                    .get(&())
                    .is_none_or(|entry| !entry.value().as_ref().eq(&service_account_secret))
                {
                    log::debug!("service_account_secret changed.");
                    // Don't leak this into log!
                    //log::debug!("service_account_secret is now: {service_account_secret}");
                }
                self.bearer_token
                    .insert((), Arc::new(service_account_secret));
            })
            .is_some()
    }

    /// Read full content of a file into a String.
    async fn read_file_text(filename: &str) -> Result<String, Box<dyn core::error::Error>> {
        let mut file = File::open(filename).await?;
        let mut contents = vec![];
        file.read_to_end(&mut contents).await?;
        Ok(std::str::from_utf8(&contents)?.to_string())
    }
}
