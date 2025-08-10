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

//! Pointer to proof of protected data authenticity.

use serde::Deserialize;
use serde::Serialize;
use serde_with::base64::Base64;
use serde_with::serde_as;
use serde_with::skip_serializing_none;
use tyst::Tyst;
use tyst::encdec::hex::ToHex;
use tyst::misc::BinaryDigestTree;
use tyst::misc::BinaryDigestTreeProof;

use super::IntegrityError;
use super::IntegrityErrorKind;

/** Pointer to `IntegrityProtection` that can prove that the protected data
here is authentic.

This is implemented as proof of inclusion of hash(protected data) in a
`BinaryDigestTree` and a pointer to the `IntegrityProtection` that protects the
root hash of the `BinaryDigestTree`.

This is String-serializable.
*/
#[serde_as]
#[skip_serializing_none]
#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct IntegrityProtectionReference {
    digest_algorithm_oid: String,
    #[serde_as(as = "Base64")]
    #[serde(rename = "binary_digest_tree_proof_b64")]
    binary_digest_tree_proof: Vec<u8>,
    protection_ts_micros: u64,
}

#[allow(dead_code)]
impl IntegrityProtectionReference {
    /// Return a new instance.
    pub fn new(
        digest_algorithm_oid: &str,
        binary_digest_tree_proof: BinaryDigestTreeProof,
        protection_ts_micros: u64,
    ) -> Self {
        Self {
            digest_algorithm_oid: digest_algorithm_oid.to_owned(),
            binary_digest_tree_proof: binary_digest_tree_proof.get_encoded_proof().to_vec(),
            protection_ts_micros,
        }
    }

    /// Return [Self] as a JSON serialized String.
    pub fn as_string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    /// Return [Self] from JSON serialized string.
    pub fn from_string<S: AsRef<str>>(s: S) -> Self {
        let s_ref = s.as_ref();
        serde_json::from_str(s_ref)
            .map_err(|e| log::error!("Failed to parse string '{s_ref}': {e}"))
            .unwrap()
    }

    /// Return the used digest algorithm
    ///
    /// This is used for both the BDT and hashing the data to protect.
    pub fn get_digest_algorith_oid(&self) -> &str {
        &self.digest_algorithm_oid
    }

    /// Return the time of `IntegrityProtection`
    pub fn get_protection_ts_micros(&self) -> u64 {
        self.protection_ts_micros
    }

    /// Return a reference to the `IntegrityProtection`
    pub fn get_integrity_protection_reference(
        &self,
        member: &[u8],
    ) -> Result<(u64, Vec<u8>), IntegrityError> {
        let mut digest = Tyst::instance()
            .digests()
            .by_oid(&self.digest_algorithm_oid)
            .ok_or_else(|| {
                IntegrityErrorKind::InvalidProof.error_with_msg(format!(
                    "Unable to instantiate digest algorithm '{}'.",
                    self.digest_algorithm_oid
                ))
            })?;
        let proof = BinaryDigestTreeProof::new(
            &tyst::encdec::oid::from_string(&self.digest_algorithm_oid).unwrap(),
            digest.get_digest_size_bits() >> 3,
            &self.binary_digest_tree_proof,
        );
        let root_hash = BinaryDigestTree::root_hash_for_member(digest.as_mut(), &proof, member)
            .ok_or_else(|| {
                let encoded_proof = proof.get_encoded_proof();
                let encoded_proof_hex = if encoded_proof.len() > 9 * 64 {
                    (&encoded_proof[0..(9 * 64)]).to_hex() + "..."
                } else {
                    encoded_proof.to_hex()
                };
                IntegrityErrorKind::InvalidProof.error_with_msg(format!(
                    "Unable to get root hash for member '{}' from proof '{encoded_proof_hex}'.",
                    member.to_hex(),
                ))
            })?;
        Ok((self.protection_ts_micros, root_hash))
    }
}
