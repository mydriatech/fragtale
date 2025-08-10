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

//! Parsing of configuration for database backend.

use config::ConfigBuilder;
use config::builder::BuilderState;
use serde::Deserialize;
use serde::Serialize;

use super::AppConfigDefaults;

/// Configuration for persistence backend.
#[derive(Deserialize, Serialize)]
pub struct BackendConfig {
    /// Backend implementation
    implementation: String,
    /// Comma separated list of cassandra backends (host:port).
    endpoints: String,
    /// Cassandra username
    username: String,
    /// Cassandra password
    password: String,
    /// Cassandra keyspace for common app tables
    namespace: String,
    /// Cassandra keyspace replication factor
    replfactor: String,
}

impl std::fmt::Debug for BackendConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackendConfig")
            .field("implementation", &self.implementation)
            .field("endpoints", &self.endpoints)
            .field("username", &self.username)
            .field("password", &"*redacted*")
            .field("namespace", &self.namespace)
            .field("replfactor", &self.replfactor)
            .finish()
    }
}

impl AppConfigDefaults for BackendConfig {
    /// Provide defaults for this part of the configuration
    fn set_defaults<T: BuilderState>(
        config_builder: ConfigBuilder<T>,
        prefix: &str,
    ) -> ConfigBuilder<T> {
        config_builder
            .set_default(prefix.to_string() + "." + "implementation", "mem")
            .unwrap()
            .set_default(prefix.to_string() + "." + "endpoints", "")
            .unwrap()
            .set_default(prefix.to_string() + "." + "username", "")
            .unwrap()
            .set_default(prefix.to_string() + "." + "password", "")
            .unwrap()
            .set_default(prefix.to_string() + "." + "namespace", "fragtale")
            .unwrap()
            .set_default(prefix.to_string() + "." + "replfactor", "3")
            .unwrap()
    }
}

impl BackendConfig {
    /// Backend implementation variant
    pub fn implementation(&self) -> &str {
        &self.implementation
    }

    /// Comma separated list of hosts.
    pub fn endpoints(&self) -> Vec<String> {
        let mut ret = Vec::new();
        if !self.endpoints.is_empty() {
            ret = self
                .endpoints
                .split(',')
                .map(|endpoint| endpoint.trim().to_string())
                .collect();
        }
        ret
    }

    /// Cassandra username
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Cassandra password
    pub fn password(&self) -> &str {
        &self.password
    }

    /// Cassandra keyspace for common app tables
    pub fn keyspace(&self) -> &str {
        &self.namespace
    }

    /// Cassandra keyspace replication factor (number of copies of the data)
    pub fn replication_factor(&self) -> usize {
        self.replfactor.parse::<usize>().unwrap_or(3)
    }
}
