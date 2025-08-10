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

//! Labeled measured value.

pub use super::MetricLabel;

/// A labeled measured value with an optional timestamp.
#[derive(Clone, Debug)]
pub struct MetricLabeledValue {
    metric_labels: Option<Vec<MetricLabel>>,
    metric_value: f64,
    metric_ts: Option<i64>,
}

impl MetricLabeledValue {
    /// Return a new instance with the mandatory measured value.
    pub fn new(metric_value: f64) -> Self {
        Self {
            metric_labels: None,
            metric_value,
            metric_ts: None,
        }
    }

    /// Builder style setting of the optional timestamp.
    ///
    /// The timestamp should be in Unix epoch milliseconds.
    pub fn set_timestamp(mut self, metric_ts_epoch_ms: u64) -> Self {
        self.metric_ts = Some(i64::try_from(metric_ts_epoch_ms).unwrap());
        self
    }

    /// Builder style append of an optional label.
    ///
    /// Multiple labels are allowed.
    pub fn add_label(mut self, name: &'static str, value: String) -> Self {
        self.metric_labels = self.metric_labels.or(Some(Vec::<MetricLabel>::new()));
        if let Some(metric_labels) = &mut self.metric_labels {
            metric_labels.push(MetricLabel::new(name, value))
        }
        self
    }

    /// Get the list of optional labeles.
    pub fn get_metric_labels(&self) -> &Option<Vec<MetricLabel>> {
        &self.metric_labels
    }

    /// Get the measured value.
    pub fn get_metric_value(&self) -> f64 {
        self.metric_value
    }

    /// Get the optional timestamp of the metricy in Unix epoch milliseconds.
    pub fn get_metric_ts(&self) -> &Option<i64> {
        &self.metric_ts
    }
}
