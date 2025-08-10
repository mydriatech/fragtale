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

/// References scraping function.
use super::MetricsResult;
use core::future::Future;
use futures_util::future::BoxFuture;
use futures_util::future::FutureExt;

/// Holds reference to scraping function provided by [super::MetricsProvider].
pub struct MetricsResultFuture(BoxFuture<'static, MetricsResult>);

impl MetricsResultFuture {
    /// Return a new instance from the provided `Future`.
    pub fn from_future(future: impl Future<Output = MetricsResult> + Send + 'static) -> Self {
        let boxed_future = FutureExt::boxed(future);
        Self(boxed_future)
    }

    /// Get the held future.
    pub fn into_inner(self) -> BoxFuture<'static, MetricsResult> {
        self.0
    }
}
