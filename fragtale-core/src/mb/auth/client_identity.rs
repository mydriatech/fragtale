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

//! Verified client identity.

use fragtale_dbp::mb::MessageBrokerError;
use fragtale_dbp::mb::MessageBrokerErrorKind;
use serde_json::Value;
use std::collections::HashMap;

/// A client identity verified through authentication.
#[derive(Clone)]
pub enum ClientIdentity {
    /// The identity is the same process.
    Internal,
    /// The identity source is a Bearer token.
    Bearer {
        /// Bearer token claims.
        claims: HashMap<String, serde_json::Value>,
        /// `true` when bearer token is the service account of this Pod.
        local: bool,
        /// Identity in a format that can be used for matching.
        identity_string: String,
    },
}

impl std::fmt::Display for ClientIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.identity_string())
    }
}

impl ClientIdentity {
    /// Return a new instance
    pub fn from_bearer_token_claims(
        claims: HashMap<String, Value>,
        local: bool,
    ) -> Result<Self, MessageBrokerError> {
        let iss = Self::extract_claim("iss", &claims)?;
        let sub = Self::extract_claim("sub", &claims)?;
        if log::log_enabled!(log::Level::Trace) {
            let local_str = if local { "Local" } else { "Remote" };
            log::trace!(
                "{local_str} client identity from bearer token. issuer: '{iss}', subject: '{sub}'"
            );
        }
        let iss = iss.replacen("://", "_", 1).replace(".", "_");
        let identity_string = format!("bearer;{iss};{sub}");
        Ok(Self::Bearer {
            claims,
            local,
            identity_string,
        })
    }

    /// Return `true` when authentication originated from withing this Pod.
    pub fn is_local(&self) -> bool {
        match self {
            ClientIdentity::Internal => true,
            Self::Bearer {
                claims: _,
                local,
                identity_string: _,
            } => *local,
        }
    }

    /// Return the identity in a format that can be used for matching.
    ///
    /// No two different identities should yield the same String.
    pub fn identity_string(&self) -> &str {
        match self {
            ClientIdentity::Internal => "internal;;",
            ClientIdentity::Bearer {
                claims: _,
                local: _,
                identity_string,
            } => identity_string,
        }
    }

    /// Extract a claim from the validated `TokenData`.
    fn extract_claim<'a>(
        claim: &str,
        claims: &'a HashMap<String, serde_json::Value>,
    ) -> Result<&'a str, MessageBrokerError> {
        claims.get(claim).and_then(Value::as_str).ok_or(
            MessageBrokerErrorKind::MalformedIdentifier
                .error_with_msg(format!("Missing or non-string '{claim}' in bearer token.")),
        )
    }
}
