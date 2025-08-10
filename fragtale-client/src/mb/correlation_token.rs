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

//! Enable correlation between events in different topics.

use serde::Deserialize;
use serde::Serialize;
use serde_with::base64::Base64;
use serde_with::serde_as;
use tyst::Tyst;
use tyst::encdec::DecodingError;
use tyst::traits::mac::ToMacKey;

/** Enables correlation between events in different topics.

From the event-client perspective this is an opaque token delivered together
with the recieved event and should be forwarded together with any published
event.

This will allow the original publisher of an event to a topic to use a
correlated event in a different topic as result of an RPC-style request.

## Security

Since correlation is based on voluntarely forwarded by cooperating microservices
there is no guarantee that the microservices will behave well. This means that
the correlation token can be lost by poorly implemented microservices or that a
valid correlation token could be attached to unrelated events.

The integrity protection will only ensure that a correlation token was not
tampered with during processing to prevent that a rouge or poorly written client
could fill up the server with waiters for bogus correlation tokens.
*/
#[serde_as]
#[derive(Clone, Deserialize, Serialize)]
pub struct CorrelationToken {
    uid: String,
    timestamp: u64,
    #[serde_as(as = "Base64")]
    integrity: Vec<u8>,
}

impl CorrelationToken {
    /// Return a new instance from the serialized form.
    pub fn from_string<S: AsRef<str>>(value: S) -> Result<Self, DecodingError> {
        let json_string = String::from_utf8(tyst::encdec::base64::decode_url(value.as_ref())?)
            .map_err(|e| DecodingError::with_msg(&e.to_string()))?;
        serde_json::from_str(json_string.as_str())
            .map_err(|e| DecodingError::with_msg(&e.to_string()))
    }

    /// Return the CorrelationToken in String serialized form.
    pub fn as_string(&self) -> String {
        let json_string = serde_json::to_string(self).unwrap();
        tyst::encdec::base64::encode_url(json_string.as_bytes(), false)
    }
}

impl CorrelationToken {
    /// New correlation
    pub fn new(oid: &[u32], secret: &[u8], timestamp: u64) -> Self {
        // UID might be assumed elsewhere to be hard to guess (-> 256 bits)
        let uid = tyst::encdec::base64::encode_url(
            &Tyst::instance().prng_get_random_bytes(None, 32),
            false,
        );
        let integrity = Self::protect(oid, secret, &uid, timestamp);
        Self {
            uid,
            timestamp,
            integrity,
        }
    }

    /// Return the correlation tokens's unique identifier
    pub fn get_uid(&self) -> &str {
        &self.uid
    }

    /// Return the timestamp of the original request
    pub fn get_timestamp_micros(&self) -> u64 {
        self.timestamp
    }

    fn protect(oid: &[u32], secret: &[u8], uid: &str, timestamp: u64) -> Vec<u8> {
        let mut mac = Tyst::instance()
            .macs()
            .by_oid(&tyst::encdec::oid::as_string(oid))
            .unwrap();
        mac.init(secret.to_mac_key().as_ref());
        mac.update(uid.as_bytes());
        mac.update(&u64::to_be_bytes(timestamp));
        let mut out = vec![0u8; mac.get_mac_size_bits() >> 3];
        mac.finalize(&mut out);
        out
    }

    /// Verify the correlation token's integrity protection.
    pub fn verify(&self, oid: &[u32], secret: &[u8]) -> bool {
        let out = Self::protect(oid, secret, &self.uid, self.timestamp);
        tyst::util::external_constant_time_equals(&self.integrity, &out)
    }
}
