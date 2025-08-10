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

//! Parsing of configuration for the application's metrics.

use config::ConfigBuilder;
use config::builder::BuilderState;
use serde::{Deserialize, Serialize};

use super::AppConfigDefaults;

/// Configuration for the application's  metrics collection.
#[derive(Debug, Deserialize, Serialize)]
pub struct MetricsConfig {
    /// See [Self::enabled()].
    enabled: bool,
}

impl AppConfigDefaults for MetricsConfig {
    /// Provide defaults for this part of the configuration
    fn set_defaults<T: BuilderState>(
        config_builder: ConfigBuilder<T>,
        prefix: &str,
    ) -> ConfigBuilder<T> {
        config_builder
            .set_default(prefix.to_string() + "." + "enabled", "true")
            .unwrap()
    }
}

impl MetricsConfig {
    /// Return `true` if metric collection should be enabled.
    pub fn enabled(&self) -> bool {
        self.enabled
    }
}
