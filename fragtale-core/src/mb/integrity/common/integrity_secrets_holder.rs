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

//! Cache configured secrets used for event integrity protection.

use crate::AppConfig;
use std::sync::Arc;

/// Cache configured secrets used for event integrity protection.
///
/// This also makes generated ephemeral secrets to work during testing.
pub struct IntegritySecretsHolder {
    current_oid: Vec<u32>,
    current_secret: Vec<u8>,
    current_ts_micros: u64,
    previous_oid: Vec<u32>,
    previous_secret: Vec<u8>,
}

impl IntegritySecretsHolder {
    /// Return a new instance.
    pub fn new(app_config: &Arc<AppConfig>) -> Arc<Self> {
        let (current_oid, current_secret, current_ts_micros) =
            app_config.integrity.current_secret();
        let (previous_oid, previous_secret) = app_config.integrity.previous_secret();
        Arc::new(Self {
            current_oid,
            current_secret,
            current_ts_micros,
            previous_oid,
            previous_secret,
        })
    }

    /// Return Object Identifier for the current integrity protection algorithm.
    pub fn get_current_oid(&self) -> &[u32] {
        &self.current_oid
    }

    /// Return current integrity protection secret.
    pub fn get_current_secret(&self) -> &[u8] {
        &self.current_secret
    }

    /// Return time of creation of current integrity protection secret.
    pub fn get_current_ts_micros(&self) -> u64 {
        self.current_ts_micros
    }

    /// Return Object Identifier for the previous integrity protection algorithm.
    pub fn get_previous_oid(&self) -> &[u32] {
        &self.previous_oid
    }

    /// Return previous integrity protection secret.
    pub fn get_previous_secret(&self) -> &[u8] {
        &self.previous_secret
    }
}
