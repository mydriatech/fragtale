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

use crate::metric::Metric;

/// Holds the result of scraping a [super::MetricsProvider].
#[derive(Clone, Debug)]
pub struct MetricsResult {
    app_component_name: String,
    metrics: Vec<Metric>,
}

impl Default for MetricsResult {
    fn default() -> Self {
        Self::new("app_default")
    }
}

impl MetricsResult {
    /// Return a new instance.
    ///
    /// The provided `app_component_name` will be used as prefix to the metric
    /// name.
    pub fn new(app_component_name: &str) -> Self {
        Self {
            app_component_name: app_component_name.to_string(),
            metrics: vec![],
        }
    }

    /// Builder style append of a [Metric] to this result.
    pub fn add_metric(mut self, metric: Metric) -> MetricsResult {
        self.metrics.push(metric);
        self
    }

    /// Provides the metric in the `PrometheusText0.0.4` format.
    pub fn as_text(&self) -> String {
        let mut ret = String::new();
        for metric in self.metrics.iter() {
            ret.push_str(metric.as_text(&self.app_component_name).as_str());
            ret.push('\n');
        }
        ret
    }
}

#[allow(unused_imports)]
mod tests {

    use super::*;
    use crate::metric::MetricLabeledValue;
    use crate::metric::MetricType;

    #[test]
    pub fn basic_metrics() {
        let metric_result = MetricsResult::new("test_app_test_component_name")
            .add_metric(
                Metric::from_metric_labeled_values(
                    "basic_test_metric",
                    &[
                        MetricLabeledValue::new(42.00)
                            .add_label("some_label", "a_value".to_owned())
                            .add_label("other_label", "another_value".to_owned()),
                        MetricLabeledValue::new(3.1416)
                            .add_label("some_label", "a_value".to_owned())
                            .add_label("third_label", "third_value".to_owned()),
                    ],
                )
                .set_type(MetricType::Gauge)
                .set_help("This is a test."),
            )
            .add_metric(
                Metric::from_metric_labeled_values(
                    "another_simple_test_metric",
                    &[
                        MetricLabeledValue::new(123456.0)
                            .add_label("method", "post".to_owned())
                            .add_label("code", "200".to_owned())
                            .set_timestamp(1_702_944_000_000),
                        MetricLabeledValue::new(456.0)
                            .add_label("method", "post".to_owned())
                            .add_label("code", "400".to_owned())
                            .set_timestamp(1_702_944_000_000),
                    ],
                )
                .set_type(MetricType::Counter)
                .set_help("This is part two of the a test"),
            );
        log::debug!("**Start**\n{}**End**", metric_result.as_text())
    }
}
