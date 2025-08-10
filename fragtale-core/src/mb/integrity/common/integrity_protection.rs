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

//! Protection of data authenticity.

use super::IntegrityError;
use super::IntegrityErrorKind;
use serde::Deserialize;
use serde::Serialize;
use serde_with::base64::Base64;
use serde_with::serde_as;
use serde_with::skip_serializing_none;
use tyst::Tyst;
use tyst::traits::mac::ToMacKey;

/** Protection of data authenticity using shared secrets.

The protected data consists is assumed to be a digest output of reasonable size.
In the current implementation this consists of a `BinaryDigestTree`'s root hash.

The hash is protected with three separate shared secrets (each using MAC-like
algorithm(s)):

* the `current` secret that is current deployment generation of instance are
  using.
* the `previous` secret that is previous deployment generation of instance
  were/are using.
* the `recovery` secret that is mostly constant during the installations
  life-time and by default is not allowed to be used for verification.

*/
#[serde_as]
#[skip_serializing_none]
#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct IntegrityProtection {
    #[serde_as(as = "Base64")]
    #[serde(rename = "protected_hash_b64")]
    protected_hash: Vec<u8>,
    current_algorithm_oid: String,
    #[serde_as(as = "Base64")]
    #[serde(rename = "current_protection_b64")]
    current_protection: Vec<u8>,
    previous_algorithm_oid: String,
    #[serde_as(as = "Base64")]
    #[serde(rename = "previous_protection_b64")]
    previous_protection: Vec<u8>,
}

impl IntegrityProtection {
    /// Return the protected hash.
    pub fn get_protected_hash(&self) -> &[u8] {
        &self.protected_hash
    }

    /// Return [Self] as a JSON serialized String.
    pub fn as_string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    /// Return [Self] from JSON serialized string.
    pub fn from_string<S: AsRef<str>>(s: S) -> Result<Self, IntegrityError> {
        serde_json::from_str(s.as_ref()).map_err(|e| {
            IntegrityErrorKind::Malformed
                .error_with_msg(format!("Failed to parse integrity protection: {e:?}"))
        })
    }

    /// Return a new instance with protection.
    pub fn protect(
        protected_hash: &[u8],
        current_oid: &[u32],
        current_secret: &[u8],
        previous_oid: &[u32],
        previous_secret: &[u8],
    ) -> Result<Self, IntegrityError> {
        let current_protection = if current_oid.is_empty() || current_secret.is_empty() {
            vec![]
        } else {
            Self::protect_with_secret(current_oid, current_secret, protected_hash)?
        };
        let previous_protection = if previous_oid.is_empty() || previous_secret.is_empty() {
            vec![]
        } else {
            Self::protect_with_secret(previous_oid, previous_secret, protected_hash)?
        };
        Ok(Self {
            protected_hash: protected_hash.to_owned(),
            current_algorithm_oid: tyst::encdec::oid::as_string(current_oid),
            current_protection,
            previous_algorithm_oid: tyst::encdec::oid::as_string(previous_oid),
            previous_protection,
        })
    }

    /// Validate the protected data (root hash) using the current secret
    pub fn validate_current(
        &self,
        current_oid: &[u32],
        current_secret: &[u8],
    ) -> Result<(), IntegrityError> {
        let oid = Self::oid_str_to_u32_vec(&self.current_algorithm_oid)?;
        if !oid.eq(current_oid) {
            Err(IntegrityErrorKind::Malformed
                .error_with_msg(format!("OID '{oid:?}' is not allowed for this protection.")))?;
        }
        Self::validate_with_secret(
            &oid,
            current_secret,
            &self.protected_hash,
            &self.current_protection,
        )
    }

    /// Validate the protected data (root hash) using the previous secret
    pub fn validate_previous(
        &self,
        previous_oid: &[u32],
        previous_secret: &[u8],
    ) -> Result<(), IntegrityError> {
        let oid = Self::oid_str_to_u32_vec(&self.previous_algorithm_oid)?;
        if !oid.eq(previous_oid) {
            Err(IntegrityErrorKind::Malformed
                .error_with_msg(format!("OID '{oid:?}' is not allowed for this protection.")))?;
        }
        Self::validate_with_secret(
            &oid,
            previous_secret,
            &self.protected_hash,
            &self.previous_protection,
        )
    }

    fn oid_str_to_u32_vec(oid_str: &str) -> Result<Vec<u32>, IntegrityError> {
        tyst::encdec::oid::from_string(oid_str).map_err(|e| {
            IntegrityErrorKind::Malformed.error_with_msg(format!("Unable to parse OID: '{e:?}'"))
        })
    }

    /// Protect data using requested algorithm and secret.
    fn protect_with_secret(
        protection_oid: &[u32],
        secret: &[u8],
        data: &[u8],
    ) -> Result<Vec<u8>, IntegrityError> {
        match protection_oid {
            tyst::oids::mac::HMAC_SHA3_256
            | tyst::oids::mac::HMAC_SHA3_384
            | tyst::oids::mac::HMAC_SHA3_512 => {
                Self::protect_with_mac(protection_oid, secret, data)
            }
            unsupported_oid => Err(IntegrityErrorKind::Malformed.error_with_msg(format!(
                "Unsupported protection algorithm OID '{unsupported_oid:?}'."
            ))),
        }
    }

    /// Validate `data`'s `protection` using requested algorithm and `secret`.
    fn validate_with_secret(
        protection_oid: &[u32],
        secret: &[u8],
        data: &[u8],
        protection: &[u8],
    ) -> Result<(), IntegrityError> {
        match protection_oid {
            tyst::oids::mac::HMAC_SHA3_256
            | tyst::oids::mac::HMAC_SHA3_384
            | tyst::oids::mac::HMAC_SHA3_512 => {
                Self::validate_with_mac(protection_oid, secret, data, protection)
            }
            unsupported_oid => Err(
                IntegrityErrorKind::ValidationFailure.error_with_msg(format!(
                    "Unsupported protection algorithm OID '{unsupported_oid:?}'."
                )),
            ),
        }
    }

    fn protect_with_mac(
        mac_oid: &[u32],
        secret: &[u8],
        data: &[u8],
    ) -> Result<Vec<u8>, IntegrityError> {
        let oid = &tyst::encdec::oid::as_string(mac_oid);
        Ok(Tyst::instance()
            .macs()
            .by_oid(oid)
            .ok_or_else(|| {
                IntegrityErrorKind::Malformed.error_with_msg(format!(
                    "Unable to get implementation for algorithm '{oid}'."
                ))
            })?
            .mac(secret.to_mac_key().as_ref(), data))
    }

    fn validate_with_mac(
        mac_oid: &[u32],
        secret: &[u8],
        data: &[u8],
        expected: &[u8],
    ) -> Result<(), IntegrityError> {
        let supplied = Self::protect_with_mac(mac_oid, secret, data)?;
        if tyst::util::external_constant_time_equals(expected, &supplied) {
            Ok(())
        } else {
            Err(IntegrityErrorKind::ValidationFailure
                .error_with_msg("Failed to validate protection."))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanity_check_integrity_protection() -> Result<(), IntegrityError> {
        let current_secret = Tyst::instance().prng_get_random_bytes(None, 48);
        let previous_secret = Tyst::instance().prng_get_random_bytes(None, 32);
        let protected_hash = Tyst::instance()
            .digests()
            .by_oid(&tyst::encdec::oid::as_string(tyst::oids::digest::SHA3_512))
            .unwrap()
            .hash(b"Some fingerprinted data.");
        let current_oid = tyst::oids::mac::HMAC_SHA3_384;
        let previous_oid = tyst::oids::mac::HMAC_SHA3_256;
        let serialized = {
            let ip = IntegrityProtection::protect(
                &protected_hash,
                current_oid,
                &current_secret,
                previous_oid,
                &previous_secret,
            )?;
            ip.validate_current(current_oid, &current_secret)?;
            ip.validate_previous(previous_oid, &previous_secret)?;
            ip.as_string()
        };
        let ip2 = IntegrityProtection::from_string(&serialized)?;
        assert_eq!(ip2.get_protected_hash(), protected_hash);
        ip2.validate_current(current_oid, &current_secret)?;
        ip2.validate_previous(previous_oid, &previous_secret)?;
        Ok(())
    }
}
