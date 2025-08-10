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

//! Tracking and scraping of registered [MetricsProvider]s.

mod metrics_provider;
mod metrics_result;
mod metrics_result_future;
mod metrics_results;
pub use self::metrics_provider::MetricsProvider;
pub use self::metrics_result::MetricsResult;
pub use self::metrics_result_future::MetricsResultFuture;
pub use self::metrics_results::MetricsResults;
use core::future::Future;
use crossbeam_skiplist::SkipMap;
use futures::lock::Mutex;
use once_cell::sync::Lazy;
use std::sync::Arc;

static INSTANCE: Lazy<Arc<MetricsProviderRegistry>> = Lazy::new(Arc::default);

/** The registry tracks all registered [MetricsProvider]s and can be used to
scrape these for resulting metrics.
*/
pub struct MetricsProviderRegistry {
    metrics_result: Arc<Mutex<(u64, MetricsResults)>>,
    metrics_instances: SkipMap<String, Arc<dyn MetricsProvider>>,
}

impl Default for MetricsProviderRegistry {
    fn default() -> Self {
        Self {
            metrics_result: Arc::new(Mutex::new((0, MetricsResults::default()))),
            metrics_instances: SkipMap::new(),
        }
    }
}

impl MetricsProviderRegistry {
    /// Register a [MetricsProvider] for scraping.
    pub fn register_metrics(
        app_name: &str,
        component_name: &str,
        observable: Arc<dyn MetricsProvider>,
    ) {
        INSTANCE.register_metrics_internal(&format!("{app_name}_{component_name}"), observable);
    }

    /// Get the result of scraping all [MetricsProvider]s.
    ///
    /// This groups concurrent requests and deliveres a cloned result to all
    /// callers to avoid putting unnessary load on the [MetricsProvider]
    /// implementations.
    pub async fn get_metrics() -> MetricsResults {
        INSTANCE.metrics().await
    }

    /// See [Self::register_metrics].
    fn register_metrics_internal(
        &self,
        app_component_name: &str,
        observable: Arc<dyn MetricsProvider>,
    ) {
        if log::log_enabled!(log::Level::Debug) {
            log::debug!("Registered observable metrics component '{app_component_name}'.");
        }
        self.metrics_instances
            .insert(app_component_name.to_string(), observable);
    }

    /// See [Self::get_metrics].
    async fn metrics(&self) -> MetricsResults {
        self.group_requests(Arc::clone(&self.metrics_result), self.metrics_internal())
            .await
    }

    /// Scrape all [MetricsProvider]s.
    async fn metrics_internal(&self) -> MetricsResults {
        let metrics_results = futures::future::join_all(
            self.metrics_instances
                .iter()
                .map(|entry| (entry.key().to_owned(), Arc::clone(entry.value())))
                .map(|(app_component_name, observable)| {
                    observable
                        .metrics(MetricsResult::new(&app_component_name))
                        .into_inner()
                }),
        )
        .await;
        if log::log_enabled!(log::Level::Trace) {
            log::trace!("metrics_results: {metrics_results:?}");
        }
        MetricsResults::new(metrics_results)
    }

    /// DDoS protection: Serve recent (but slightly stale) result to concurrent requests
    async fn group_requests<T: Clone>(
        &self,
        results: Arc<Mutex<(u64, T)>>,
        future: impl Future<Output = T>,
    ) -> T {
        let result;
        {
            let mut guarded_result = results.lock().await;
            let now_ms = Self::get_timestamp_micros();
            if guarded_result.0 + 1000 < now_ms {
                guarded_result.0 = now_ms;
                guarded_result.1 = future.await;
            }
            result = guarded_result.1.clone();
        }
        result
    }

    /// Microseconds since UNIX epoch
    fn get_timestamp_micros() -> u64 {
        u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_micros(),
        )
        .unwrap()
    }
}
