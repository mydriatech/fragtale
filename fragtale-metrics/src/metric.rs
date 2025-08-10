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

//! Metric representation.

mod metric_label;
mod metric_labeled_value;
mod metric_type;

pub use self::metric_label::MetricLabel;
pub use self::metric_labeled_value::MetricLabeledValue;
pub use self::metric_type::MetricType;

/** A metric that can consist of one or more measured values of the same type
with different labels under a single name with an optional description ("help").
 */
#[derive(Clone, Debug)]
pub struct Metric {
    metric_name: &'static str,
    metric_help: Option<&'static str>,
    metric_type: MetricType,
    metric_labeled_values: Vec<MetricLabeledValue>,
}

impl Metric {
    /// Create a new metrics with a single labeled value.
    pub fn from_metric_labeled_value(
        metric_name: &'static str,
        metric_labeled_value: MetricLabeledValue,
    ) -> Self {
        Self::from_metric_labeled_values(metric_name, &[metric_labeled_value])
    }

    /// Create a new metrics with with multiple labeled values.
    pub fn from_metric_labeled_values(
        metric_name: &'static str,
        mlvs: &[MetricLabeledValue],
    ) -> Self {
        debug_assert!(!mlvs.is_empty());
        let mut metric_labeled_values = Vec::new();
        metric_labeled_values.extend(mlvs.iter().cloned());
        Self {
            metric_name,
            metric_help: None,
            // Missing value implies "untyped"
            metric_type: MetricType::Untyped,
            metric_labeled_values,
        }
    }

    /// Set the optional help (description) text of the metric.
    pub fn set_help(mut self, metric_help: &'static str) -> Self {
        self.metric_help = Some(metric_help);
        self
    }

    /// Set the optional type of the metric.
    ///
    /// When no type is provided, this metrics becomes "untyped".
    pub fn set_type(mut self, metric_type: MetricType) -> Self {
        self.metric_type = metric_type;
        self
    }

    /// Convert metric into text format.
    pub fn as_text(&self, app_component_name: &str) -> String {
        let metric_name = app_component_name.to_string() + "_" + self.metric_name;
        let mut ret = String::new();
        if let Some(metric_help) = self.metric_help {
            ret.push_str("# HELP ");
            ret.push_str(&metric_name);
            ret.push(' ');
            ret.push_str(metric_help);
            ret.push('\n');
        }
        match &self.metric_type {
            MetricType::Untyped => {}
            metric_type => {
                ret.push_str("# TYPE ");
                ret.push_str(&metric_name);
                ret.push(' ');
                ret.push_str(metric_type.as_str());
                ret.push('\n');
            }
        }
        for metric_labeled_value in &self.metric_labeled_values {
            ret.push_str(&metric_name);
            if let Some(metric_lables) = metric_labeled_value.get_metric_labels() {
                ret.push('{');
                for (i, metric_label) in metric_lables.iter().enumerate() {
                    ret.push_str(metric_label.get_name());
                    ret.push_str("=\"");
                    ret.push_str(metric_label.get_value());
                    ret.push('\"');
                    if i + 1 < metric_lables.len() {
                        ret.push(',');
                    }
                }
                ret.push('}');
            }
            ret.push(' ');
            ret.push_str(&metric_labeled_value.get_metric_value().to_string());
            if let Some(metric_ts) = *metric_labeled_value.get_metric_ts() {
                ret.push(' ');
                ret.push_str(&metric_ts.to_string());
            }
            ret.push('\n');
        }
        ret
    }
}
