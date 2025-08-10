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

//! The type of metric.

/// The type of metric.
///
/// This dictates how the measured value(s) should be interpreted.
#[derive(Clone, Debug)]
pub enum MetricType {
    /// Single monotonically increasing counter whose value can only increase or
    /// be reset to zero on restart.
    Counter,
    /// A single numerical value that can arbitrarily go up and down.
    Gauge,
    // Unused
    //Histogram,
    // Unused
    //Summary,
    /// No type for the metric is provided.
    Untyped,
}

impl MetricType {
    /// Return the `PrometheusText0.0.4` string representation.
    pub fn as_str(&self) -> &'static str {
        match *self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            //Self::Histogram => "histogram",
            //Self::Summary => "summary",
            Self::Untyped => "untyped",
        }
    }
}
