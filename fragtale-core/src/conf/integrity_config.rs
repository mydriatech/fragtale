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

//! Parsing of configuration for integrity protection of data at rest.

use super::AppConfigDefaults;
use config::ConfigBuilder;
use config::builder::BuilderState;
use serde::{Deserialize, Serialize};
use tyst::Tyst;

/// Configuration for integrity protection of data at rest.
#[derive(Debug, Deserialize, Serialize)]
pub struct IntegrityConfig {
    correlationsecret: String,
    correlationoid: String,
    currentsecret: String,
    currentoid: String,
    currentsecretts: String,
    previoussecret: String,
    previousoid: String,
    ntphost: Option<String>,
    tolerance: u64,
}

impl AppConfigDefaults for IntegrityConfig {
    /// Provide defaults for this part of the configuration
    fn set_defaults<T: BuilderState>(
        config_builder: ConfigBuilder<T>,
        prefix: &str,
    ) -> ConfigBuilder<T> {
        //let default_protection_oid = tyst::encdec::oid::as_string(tyst::oids::mac::HMAC_SHA3_512);
        config_builder
            .set_default(
                prefix.to_string() + "." + "correlationoid",
                "/secrets/correlation_oid",
            )
            .unwrap()
            .set_default(
                prefix.to_string() + "." + "correlationsecret",
                "/secrets/correlation",
            )
            .unwrap()
            .set_default(
                prefix.to_string() + "." + "currentoid",
                "/secrets/current_oid",
            )
            .unwrap()
            .set_default(
                prefix.to_string() + "." + "currentsecret",
                "/secrets/current",
            )
            .unwrap()
            .set_default(
                prefix.to_string() + "." + "currentsecretts",
                "/secrets/current_ts",
            )
            .unwrap()
            .set_default(
                prefix.to_string() + "." + "previousoid",
                "/secrets/previous_oid",
            )
            .unwrap()
            .set_default(
                prefix.to_string() + "." + "previoussecret",
                "/secrets/previous",
            )
            .unwrap()
            .set_default(prefix.to_string() + "." + "ntphost", "")
            .unwrap()
            .set_default(prefix.to_string() + "." + "tolerance", "1000000")
            .unwrap()
    }
}

impl IntegrityConfig {
    /// Return the correlation token protection OID and secret.
    pub fn correlation_secret(&self) -> (Vec<u32>, Vec<u8>) {
        Self::get_oid_and_secret(&self.correlationoid, &self.correlationsecret)
    }

    /// Return the current protection OID, secret and when the secret was
    /// created in micros.
    pub fn current_secret(&self) -> (Vec<u32>, Vec<u8>, u64) {
        let (oid, secret) = Self::get_oid_and_secret(&self.currentoid, &self.currentsecret);
        let time_stamp_micros = Self::load_text_file(&self.currentsecretts)
            .and_then(|content| {
                content
                    .parse::<u64>()
                    .map_err(|e| {
                        log::warn!(
                            "Unable to parse OID in '{}' (default will be used): {e}",
                            &self.currentsecretts
                        );
                    })
                    .map(|time_stamp_seconds| time_stamp_seconds * 1_000_000)
                    .ok()
            })
            .unwrap_or_default();
        (oid, secret, time_stamp_micros)
    }

    /// Return the previous protection OID and secret.
    pub fn previous_secret(&self) -> (Vec<u32>, Vec<u8>) {
        Self::get_oid_and_secret(&self.previousoid, &self.previoussecret)
    }

    /// NTP host in the form `hostname:port`. An empty string will disable NTP.
    pub fn ntp_host(&self) -> Option<String> {
        if self
            .ntphost
            .as_ref()
            .is_none_or(|ntp_host| ntp_host.is_empty())
        {
            return None;
        }
        self.ntphost.clone()
    }

    /// The worst time local time accuracy that can be tolerated.
    pub fn tolerable_local_accuracy_micros(&self) -> u64 {
        self.tolerance
    }

    /// Return the previous protection OID and secret.
    fn get_oid_and_secret(oid_filename: &str, secret_filename: &str) -> (Vec<u32>, Vec<u8>) {
        let oid = Self::get_oid(oid_filename);
        let raw_secret = Self::get_secret(secret_filename);
        // HMAC optimization: Use configured secret as entropy and produce a key
        // of the right length to avoid this during the MAC operation.
        let suitable_digest_oid = Self::derive_suitable_digest_algos_from_protection(&oid);
        let secret = Tyst::instance()
            .digests()
            .by_oid(&tyst::encdec::oid::as_string(&suitable_digest_oid))
            .map(|mut digest| digest.as_mut().hash(&raw_secret))
            .unwrap_or(raw_secret);
        (oid, secret)
    }

    /// OID
    fn get_oid(oid_filename: &str) -> Vec<u32> {
        if let Some(content) = Self::load_text_file(oid_filename) {
            if log::log_enabled!(log::Level::Trace) {
                log::trace!("get_oid({oid_filename}) -> {content}");
            }
            match tyst::encdec::oid::from_string(&content) {
                Ok(oid) => {
                    return oid;
                }
                Err(e) => {
                    log::warn!(
                        "Unable to parse OID in '{oid_filename}' (default will be used): {e}"
                    );
                }
            }
        }
        tyst::oids::mac::HMAC_SHA3_512.to_vec()
    }

    /// Shared secret
    fn get_secret(secret_filename: &str) -> Vec<u8> {
        if let Some(content) = Self::load_text_file(secret_filename) {
            match tyst::encdec::base64::decode(&content) {
                Ok(secret) => {
                    return secret;
                }
                Err(e) => {
                    log::warn!("Failed to parse '{secret_filename}': {e}");
                }
            }
        }
        log::info!(
            "An ephemeral secret will be generated due to previous error. This is only acceptable for testing."
        );
        Tyst::instance().prng_get_random_bytes(None, 64)
    }

    /// Derive digest algorithms from MAC-like algorithm
    pub fn derive_suitable_digest_algos_from_protection(protection_oid: &[u32]) -> Vec<u32> {
        match protection_oid {
            tyst::oids::mac::HMAC_SHA3_224 => tyst::oids::digest::SHA3_224.to_vec(),
            tyst::oids::mac::HMAC_SHA3_256 => tyst::oids::digest::SHA3_256.to_vec(),
            tyst::oids::mac::HMAC_SHA3_384 => tyst::oids::digest::SHA3_384.to_vec(),
            tyst::oids::mac::HMAC_SHA3_512 => tyst::oids::digest::SHA3_512.to_vec(),
            _ => panic!(),
        }
    }

    fn load_text_file(filename: &str) -> Option<String> {
        let full_filename = std::path::PathBuf::from(filename);
        if log::log_enabled!(log::Level::Trace) {
            log::trace!(
                "Will attempt to load '{}' as text.",
                full_filename.display()
            );
        }
        std::fs::read_to_string(&full_filename)
            .map_err(|e| {
                log::warn!("Failed to parse '{}': {e}", full_filename.display());
            })
            .ok()
    }
}
