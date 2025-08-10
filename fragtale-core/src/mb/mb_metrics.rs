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

//! Provide metrics for the [super::MessageBroker].

use crate::AppConfig;
use crossbeam_skiplist::SkipMap;
use fragtale_metrics::metric::Metric;
use fragtale_metrics::metric::MetricLabeledValue;
use fragtale_metrics::metric::MetricType;
use fragtale_metrics::registry::MetricsProvider;
use fragtale_metrics::registry::MetricsProviderRegistry;
use fragtale_metrics::registry::MetricsResult;
use fragtale_metrics::registry::MetricsResultFuture;
use fragtale_metrics::util::AtomicMetricAverage;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

/// Provide metrics for the [super::MessageBroker].
pub struct MessageBrokerMetrics {
    app_version: String,
    published_events: SkipMap<String, AtomicU64>,
    published_bytes: SkipMap<String, AtomicU64>,
    delivered_events: SkipMap<String, AtomicU64>,
    delivered_bytes: SkipMap<String, AtomicU64>,
    correlated_wait_by_topic_max: SkipMap<String, Arc<AtomicU64>>,
    correlated_wait_by_topic_avg: SkipMap<String, AtomicMetricAverage>,
    delivery_latency_by_topic_max: SkipMap<String, Arc<AtomicU64>>,
    delivery_latency_by_topic_avg: SkipMap<String, AtomicMetricAverage>,
}

impl MessageBrokerMetrics {
    const METRIC_COMPONENT_NAME: &str = "mb";
    const METRIC_NAME_DELIVERED_EVENTS: &str = "delivered_events_count";
    const METRIC_NAME_DELIVERED_BYTES: &str = "delivered_bytes_count";
    const METRIC_NAME_PUBLISHED_EVENTS: &str = "published_events_count";
    const METRIC_NAME_PUBLISHED_BYTES: &str = "published_bytes_count";
    const METRIC_NAME_CORRELATED_WAIT_MAX: &str = "correlated_wait_max_micros";
    const METRIC_NAME_CORRELATED_WAIT_AVG: &str = "correlated_wait_avg_millis";
    const METRIC_NAME_DELIVERY_LATENCY_MAX: &str = "delivery_latency_max_micros";
    const METRIC_NAME_DELIVERY_LATENCY_AVG: &str = "delivery_latency_avg_millis";
    const METRIC_NAME_VERSION: &str = "appname_build_info";
    const METRIC_LABEL_TOPIC: &str = "topic";
    const METRIC_LABEL_VERSION: &str = "version";

    /// Return a new instance.
    pub(super) fn new(app_config: &AppConfig) -> Arc<Self> {
        let instance = Arc::new(Self {
            app_version: app_config.app_version().to_owned(),
            published_events: SkipMap::default(),
            published_bytes: SkipMap::default(),
            delivered_events: SkipMap::default(),
            delivered_bytes: SkipMap::default(),
            correlated_wait_by_topic_max: SkipMap::default(),
            correlated_wait_by_topic_avg: SkipMap::default(),
            delivery_latency_by_topic_max: SkipMap::default(),
            delivery_latency_by_topic_avg: SkipMap::default(),
        });
        MetricsProviderRegistry::register_metrics(
            app_config.app_name_lowercase(),
            Self::METRIC_COMPONENT_NAME,
            Arc::clone(&instance) as Arc<dyn MetricsProvider>,
        );
        instance
    }

    /// Increase counter for published events per topic and event document
    /// bytes.
    pub(super) fn inc_published_events(&self, topic_id: &str, event_document_bytes: usize) {
        // Note: Only alloc String when entry is missing during first check.
        self.published_events
            .get(topic_id)
            .unwrap_or_else(|| {
                self.published_events
                    .get_or_insert_with(topic_id.to_string(), AtomicU64::default)
            })
            .value()
            .fetch_add(1, Ordering::Relaxed);
        self.published_bytes
            .get(topic_id)
            .unwrap_or_else(|| {
                self.published_bytes
                    .get_or_insert_with(topic_id.to_string(), AtomicU64::default)
            })
            .value()
            .fetch_add(
                u64::try_from(event_document_bytes).unwrap_or_default(),
                Ordering::Relaxed,
            );
    }

    /// Increase counter for deliviered events per topic.
    pub(super) fn inc_delivered_events(&self, topic_id: &str) {
        self.delivered_events
            .get_or_insert_with(topic_id.to_string(), AtomicU64::default)
            .value()
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Increase counter for delivered event document bytes per topic.
    pub(super) fn inc_delivered_bytes(&self, topic_id: &str, event_document_bytes: usize) {
        self.delivered_bytes
            .get_or_insert_with(topic_id.to_string(), AtomicU64::default)
            .value()
            .fetch_add(
                u64::try_from(event_document_bytes).unwrap_or_default(),
                Ordering::Relaxed,
            );
    }

    /// Track how long the caller has waiting for a result of a correlated
    /// query.
    pub(super) fn report_correlated_wait(&self, topic_id: &str, duration_micros: u64) {
        // Note: Only alloc String when entry is missing during first check.
        {
            self.correlated_wait_by_topic_avg
                .get(topic_id)
                .unwrap_or_else(|| {
                    self.correlated_wait_by_topic_avg
                        .get_or_insert_with(topic_id.to_string(), AtomicMetricAverage::default)
                })
                .value()
                // Convert latency to millis
                .append_with_cap(duration_micros / 1000);
        }
        {
            let value = self
                .correlated_wait_by_topic_max
                .get(topic_id)
                .unwrap_or_else(|| {
                    self.correlated_wait_by_topic_max
                        .get_or_insert_with(topic_id.to_string(), Arc::default)
                })
                .value()
                .clone();
            // Note: This is _not_ atomic as a whole, but good enough for metrics.
            let current = value.load(Ordering::Relaxed);
            if current < duration_micros {
                value.store(duration_micros, Ordering::Relaxed);
            }
        }
    }

    /// Track how long it takes after an event has been published to its
    /// delivery to a waiting topic consumer.
    pub(super) fn report_publish_to_delivery_latency_micros(
        &self,
        topic_id: &str,
        latency_micros: u64,
    ) {
        // Note: Only alloc String when entry is missing during first check.
        {
            self.delivery_latency_by_topic_avg
                .get(topic_id)
                .unwrap_or_else(|| {
                    self.delivery_latency_by_topic_avg
                        .get_or_insert_with(topic_id.to_string(), AtomicMetricAverage::default)
                })
                .value()
                // Convert latency to millis
                .append_with_cap(latency_micros / 1000);
        }
        {
            let value = self
                .delivery_latency_by_topic_max
                .get(topic_id)
                .unwrap_or_else(|| {
                    self.delivery_latency_by_topic_max
                        .get_or_insert_with(topic_id.to_string(), Arc::default)
                })
                .value()
                .clone();
            // Note: This is _not_ atomic as a whole, but good enough for metrics.
            let current = value.load(Ordering::Relaxed);
            if current < latency_micros {
                value.store(latency_micros, Ordering::Relaxed);
            }
        }
    }

    fn mlvs_from_by_topic_count(map: &SkipMap<String, AtomicU64>) -> Vec<MetricLabeledValue> {
        let mut mlvs = vec![];
        for entry in map.iter() {
            let topic_id = entry.key().to_string();
            let metric_value = entry.value().load(Ordering::Relaxed) as f64;
            mlvs.push(
                MetricLabeledValue::new(metric_value).add_label(Self::METRIC_LABEL_TOPIC, topic_id),
            )
        }
        if mlvs.is_empty() {
            mlvs.push(MetricLabeledValue::new(0f64));
        }
        mlvs
    }

    fn mlvs_from_by_topic_gauge_max(
        map: &SkipMap<String, Arc<AtomicU64>>,
    ) -> Vec<MetricLabeledValue> {
        let mut mlvs = vec![];
        for entry in map.iter() {
            let topic_id = entry.key().to_string();
            let metric_value = entry.value().swap(0, Ordering::Relaxed) as f64;
            mlvs.push(
                MetricLabeledValue::new(metric_value).add_label(Self::METRIC_LABEL_TOPIC, topic_id),
            )
        }
        if mlvs.is_empty() {
            mlvs.push(MetricLabeledValue::new(0f64));
        }
        mlvs
    }

    fn mlvs_from_by_topic_gauge_avg(
        map: &SkipMap<String, AtomicMetricAverage>,
    ) -> Vec<MetricLabeledValue> {
        let mut mlvs = vec![];
        for entry in map.iter() {
            let topic_id = entry.key().to_string();
            // Reset value when read
            let metric_value = entry.value().get_and_reset() as f64;
            mlvs.push(
                MetricLabeledValue::new(metric_value).add_label(Self::METRIC_LABEL_TOPIC, topic_id),
            )
        }
        if mlvs.is_empty() {
            mlvs.push(MetricLabeledValue::new(0f64));
        }
        mlvs
    }
}

impl MetricsProvider for MessageBrokerMetrics {
    fn metrics(self: Arc<Self>, template: MetricsResult) -> MetricsResultFuture {
        let self_clone = Arc::clone(&self);
        MetricsResultFuture::from_future(async move {
            template.add_metric(
                Metric::from_metric_labeled_value(
                    Self::METRIC_NAME_VERSION,
                    MetricLabeledValue::new(1.0).add_label(
                        Self::METRIC_LABEL_VERSION,
                        self_clone.app_version.to_owned(),
                    ),
                )
                .set_help("The application version.")
                .set_type(MetricType::Untyped),
            )
            .add_metric(
                Metric::from_metric_labeled_values(
                    Self::METRIC_NAME_PUBLISHED_EVENTS,
                    &Self::mlvs_from_by_topic_count(&self_clone.published_events)
                )
                .set_help("Published events.")
                .set_type(MetricType::Counter),
            )
            .add_metric(
                Metric::from_metric_labeled_values(
                    Self::METRIC_NAME_PUBLISHED_BYTES,
                    &Self::mlvs_from_by_topic_count(&self_clone.published_bytes)
                )
                .set_help("Published events document bytes.")
                .set_type(MetricType::Counter),
            )
            .add_metric(
                Metric::from_metric_labeled_values(
                    Self::METRIC_NAME_DELIVERED_EVENTS,
                     &Self::mlvs_from_by_topic_count(&self_clone.delivered_events)
                 )
                .set_help("Delivered events.")
                .set_type(MetricType::Counter),
            )
            .add_metric(
                Metric::from_metric_labeled_values(
                    Self::METRIC_NAME_DELIVERED_BYTES,
                    &Self::mlvs_from_by_topic_count(&self_clone.delivered_bytes)
                )
                .set_help("Delivered events document bytes.")
                .set_type(MetricType::Counter),
            )
            .add_metric(
                Metric::from_metric_labeled_values(
                    Self::METRIC_NAME_CORRELATED_WAIT_MAX,
                    &Self::mlvs_from_by_topic_gauge_max(&self_clone.correlated_wait_by_topic_max),
                )
                .set_help(
                    "Max latency between publishing of an event and the correlated event response delivery.",
                )
                .set_type(MetricType::Gauge),
            )
            .add_metric(
                Metric::from_metric_labeled_values(
                    Self::METRIC_NAME_CORRELATED_WAIT_AVG,
                    &Self::mlvs_from_by_topic_gauge_avg(&self_clone.correlated_wait_by_topic_avg),
                )
                .set_help("Average wait between publishing of an event the correlated event response delivery.")
                .set_type(MetricType::Gauge),
            )
            .add_metric(
                Metric::from_metric_labeled_values(
                    Self::METRIC_NAME_DELIVERY_LATENCY_MAX,
                    &Self::mlvs_from_by_topic_gauge_max(&self_clone.delivery_latency_by_topic_max),
                )
                .set_help(
                    "Max latency between publishing of an event and start of delivery of the event to a waiting consumer.",
                )
                .set_type(MetricType::Gauge),
            )
            .add_metric(
                Metric::from_metric_labeled_values(
                    Self::METRIC_NAME_DELIVERY_LATENCY_AVG,
                    &Self::mlvs_from_by_topic_gauge_avg(&self_clone.delivery_latency_by_topic_avg),
                )
                .set_help("Average latency between publishing of an event and start of delivery of the event to a waiting consumer.")
                .set_type(MetricType::Gauge),
            )
        })
    }
}
