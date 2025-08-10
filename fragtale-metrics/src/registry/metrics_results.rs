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

//! Result of scraping all [super::MetricsProvider]s.

use super::MetricsResult;

/// Holds the result of scraping all [super::MetricsProvider]s.
#[derive(Clone, Debug, Default)]
pub struct MetricsResults {
    metrics_results: Vec<MetricsResult>,
}

impl MetricsResults {
    /// Return a new instance.
    pub fn new(metrics_results: Vec<MetricsResult>) -> Self {
        Self { metrics_results }
    }

    /// Provides the metrics in the `PrometheusText0.0.4` format.
    pub fn as_text(&self) -> String {
        let mut ret = String::new();
        for metric_result in self.metrics_results.iter() {
            ret.push_str(metric_result.as_text().as_str());
        }
        ret
    }
}
