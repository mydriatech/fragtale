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

//! Message Broker core.

pub mod auth {
    //! Authorization

    mod access_control;
    mod client_identity;

    pub use self::access_control::AccessControl;
    pub use self::client_identity::ClientIdentity;
}
mod consumers;
mod correlation_hotlist;
mod event_descriptor_cache;
mod integrity;
mod mb_metrics;
mod object_count_tracker;
mod pre_storage_processor;
mod unique_time_stamper;

use self::consumers::Consumers;
use self::correlation_hotlist::CorrelationHotlist;
use self::event_descriptor_cache::EventDescriptorCache;
use self::integrity::*;
use self::object_count_tracker::ObjectCountTracker;
use self::pre_storage_processor::PreStorageProcessor;
use self::unique_time_stamper::UniqueTimeStamper;
use crate::conf::AppConfig;
use crate::util::TrustedTime;
use auth::AccessControl;
use auth::ClientIdentity;
use fragtale_client::mb::event_descriptor::DescriptorVersion;
use fragtale_client::mb::event_descriptor::EventDescriptor;
use fragtale_dbp::dbp::DatabaseProvider;
use fragtale_dbp::dbp::facades::DatabaseProviderFacades;
pub use fragtale_dbp::mb::MessageBrokerError;
pub use fragtale_dbp::mb::MessageBrokerErrorKind;
use fragtale_dbp::mb::ObjectCountType;
use fragtale_dbp::mb::TopicEvent;
use fragtale_dbp::mb::UniqueTime;
use fragtale_dbp::mb::consumers::EventDeliveryGist;
use fragtale_dbp_cassandra::CassandraProvider;
use fragtale_dbp_mem::InMemoryDatabaseProvider;
use integrity::common::IntegritySecretsHolder;
use mb_metrics::MessageBrokerMetrics;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::time::sleep;

/** Message Broker.

The Message Broker is responsible for coordinating message reception and timely
delivery while maintaining intgrity.
*/
pub struct MessageBroker {
    /// Thread safe boolean used to indicate application readyness.
    health_ready: AtomicBool,
    /// The database provider
    dbp: Arc<DatabaseProvider>,
    /// The trusted time montor.
    trusted_time: Arc<TrustedTime>,
    /// Producer of true globally unique time-based event identifiers.
    unique_timer_stamper: Arc<UniqueTimeStamper>,
    /// Event schema/meta-data cache.
    event_descriptor_cache: Arc<EventDescriptorCache>,
    // Responsible for creation of event integrity protection.
    integrity_protector: Arc<IntegrityProtector>,
    // Responsible for validation of event integrity protection.
    integrity_validator: Arc<IntegrityValidator>,
    // Tracker of event delivery status changes.
    object_count_tracker: Arc<ObjectCountTracker>,
    // Performs tasks like extracting indexed data before the event is persisted.
    pre_storage_processor: Arc<PreStorageProcessor>,
    // Tracking outcomes of a request event.
    correlation_hotlist: Arc<CorrelationHotlist>,
    // Tracking of consumers and fairly ordered event delivery.
    consumers: Arc<Consumers>,
    // For checking authorization.
    access_control: Arc<AccessControl>,
    // Metrics
    metrics: Option<Arc<MessageBrokerMetrics>>,
}

impl MessageBroker {
    /// Return a new instance.
    pub async fn new(app_config: &Arc<AppConfig>) -> Arc<Self> {
        // Setup persistence from config.
        let dbp = match app_config.backend.implementation() {
            "cassandra" => {
                let cassandra_provider = CassandraProvider::new(
                    app_config.backend.keyspace(),
                    &app_config.backend.endpoints(),
                    app_config.backend.username(),
                    app_config.backend.password(),
                    app_config.backend.replication_factor(),
                )
                .await;
                Arc::new(cassandra_provider.as_database_provider())
            }
            "mem" => {
                let inmem_provider = InMemoryDatabaseProvider::new().await;
                //DatabaseProvider2::new(Box::new(inmem_provider))
                Arc::new(inmem_provider.as_database_provider())
            }
            unknown_provider => panic!("Unkown database provider type '{unknown_provider}'."),
        };
        // Establish a unique instance identifier using the shared database.
        let unique_timer_stamper = UniqueTimeStamper::new(&dbp).await;
        let instance_id = unique_timer_stamper.get_instance_id();
        let instance_start_ts = fragtale_client::time::get_timestamp_micros();
        // Start tracking schema and state of deliveries.
        let event_descriptor_cache = EventDescriptorCache::new(&dbp).await;
        let object_count_tracker = ObjectCountTracker::new(&dbp, instance_id).await;
        let pre_storage_processor = PreStorageProcessor::new(&event_descriptor_cache);
        // Setup time monitoring, integrity protection and consolidation.
        let trusted_time = TrustedTime::new(
            app_config.integrity.ntp_host(),
            app_config.integrity.tolerable_local_accuracy_micros(),
        )
        .await;
        let ish = IntegritySecretsHolder::new(app_config);
        let integrity_protector = IntegrityProtector::new(&ish, &dbp, &unique_timer_stamper);
        let integrity_validator =
            IntegrityValidator::new(&ish, &dbp, instance_start_ts, &unique_timer_stamper);
        IntegrityConsolidationService::new(
            &ish,
            &dbp,
            &integrity_protector,
            &integrity_validator,
            &unique_timer_stamper,
        )
        .await;
        // Setup speedy delivery of correlation requests.
        let correlation_hotlist = CorrelationHotlist::new(app_config, &dbp).await;
        let consumers = Consumers::new(&dbp, &object_count_tracker, instance_id);
        let access_control = AccessControl::new(&dbp).await;
        let metrics = app_config
            .metrics
            .enabled()
            .then(|| MessageBrokerMetrics::new(app_config));
        //let metrics = MessageBrokerMetrics::new(app_config);
        log::info!("Message broker dependencies has have been created.");
        Arc::new(Self {
            health_ready: AtomicBool::new(false),
            dbp,
            trusted_time,
            unique_timer_stamper,
            event_descriptor_cache,
            integrity_protector,
            integrity_validator,
            object_count_tracker,
            pre_storage_processor,
            correlation_hotlist,
            consumers,
            access_control,
            metrics,
        })
        .init(app_config)
    }

    /// Initialize
    fn init(self: Arc<Self>, app_config: &Arc<AppConfig>) -> Arc<Self> {
        let self_clone = Arc::clone(&self);
        let app_config = Arc::clone(app_config);
        tokio::spawn(async move { self_clone.post_init(&app_config).await });
        self
    }

    /// Async tasks to perform after this [MessageBroker] has been started.
    async fn post_init(&self, app_config: &AppConfig) {
        // Wait for local system time to be accurate enough so that we can rely
        // on it for knowing when events happened.
        let mut attempts = 0u64;
        loop {
            let ready = self.trusted_time.is_local_time_within_tolerance();
            if ready {
                break;
            }
            attempts += 1;
            if attempts % 120 == 0 {
                log::info!("Local system time accurracy is still not within allowed tolerance.");
            }
            tokio::time::sleep(tokio::time::Duration::from_micros(500_000)).await;
        }
        let ready_ts_micros = fragtale_client::time::get_timestamp_micros();
        self.health_ready.store(true, Ordering::Relaxed);
        log::info!(
            "Message broker is ready after {} micros.",
            ready_ts_micros - app_config.startup_ts_micros()
        );
    }

    /// Return `true` if the app has started.
    pub fn is_health_started(&self) -> bool {
        self.health_ready.load(Ordering::Relaxed)
    }

    /// Return `true` if the app is ready to recieve requests.
    pub fn is_health_ready(&self) -> bool {
        self.health_ready.load(Ordering::Relaxed) && self.is_health_live()
    }

    /// Return `true` if the app is functioning as expected and `false` if it
    /// needs to be restarted.
    pub fn is_health_live(&self) -> bool {
        self.trusted_time.is_local_time_within_tolerance()
            && self.unique_timer_stamper.is_instance_id_still_valid()
    }

    /// Failsafe that terminates the application if it returns.
    ///
    /// This should kick in if the platform fails to orderly kill the
    /// application despite liveness health-check being raised.
    pub async fn liveness_failsafe(&self) -> Result<(), Box<dyn core::error::Error>> {
        let interval_micros = 10_000_000;
        let mut unhealth_duration = 0;
        loop {
            sleep(tokio::time::Duration::from_micros(interval_micros)).await;
            if self.is_health_live() {
                unhealth_duration = 0;
            } else {
                unhealth_duration += interval_micros;
            }
            if unhealth_duration > 60_000_000 {
                break;
            }
        }
        Err(Box::new(MessageBrokerErrorKind::Unspecified.error_with_msg(
            "Liveness failsafe terminating app. It was expected that the plaform should have killed it already."
        )) as Box<dyn core::error::Error>)
    }

    /// Invoked on graceful shutdowns to allow for some pre-emptive clean-up.
    ///
    /// This is not garanteed to run, so no code can rely on this clean-up to
    /// have happened.
    pub async fn exit_hook(&self) {
        self.unique_timer_stamper.free_instance_id().await
    }

    /// Setup event description with schema validation and indexed value
    /// extraction for a topic.
    pub async fn upsert_topic_event_descriptor(
        &self,
        identity: &ClientIdentity,
        topic_id: &str,
        event_descriptor: EventDescriptor,
    ) -> Result<(), MessageBrokerError> {
        self.access_control
            .assert_allowed_topic_write(identity, topic_id)
            .await?;
        log::info!(
            "Event descriptor update of topic '{topic_id}' by '{}' descriptor: '{event_descriptor:?}'.",
            identity.identity_string()
        );
        self.dbp.topic_facade().ensure_topic_setup(topic_id).await?;
        // Make sue we have the latest version
        self.event_descriptor_cache.reload_for_topic(topic_id).await;
        let latest_opt = self
            .event_descriptor_cache
            .get_event_descriptor_by_topic_latest(topic_id);
        if let Some(latest) = latest_opt.as_deref() {
            if latest.eq(&event_descriptor) {
                log::debug!("Event Descriptor already exists exactly as requested. All good.");
                return Ok(());
            }
            if latest.get_version() <= event_descriptor.get_version() {
                Err(MessageBrokerErrorKind::EvenDescriptorError.error_with_msg(format!(
                    "Prevented upsert of topic '{topic_id}' event descriptor, since version is not newer."
                )))?;
            }
        }
        // Persist new event description
        let inserted = self
            .dbp
            .topic_facade()
            .event_descriptor_persists(
                topic_id,
                event_descriptor.get_version(),
                event_descriptor.get_version_min(),
                &event_descriptor
                    .get_event_schema()
                    .as_ref()
                    .map(|event_schema| event_schema.get_schema_id().to_owned()),
                &event_descriptor.as_string(),
            )
            .await;
        if !inserted {
            Err(
                MessageBrokerErrorKind::EvenDescriptorError.error_with_msg(format!(
                    "Failed to upsert event descriptor for topic '{topic_id}'."
                )),
            )?;
        }
        // Reload cache right away on this instance
        self.event_descriptor_cache.reload_for_topic(topic_id).await;
        // Setup additional columns in 'event_by_id' table
        // Get all Extractors for this topic
        let name_and_type_slice = self
            .dbp
            .topic_facade()
            .event_descriptors_by_topic_id(topic_id, None)
            .await
            .into_iter()
            .map(EventDescriptor::from_string)
            .filter_map(|ed| ed.get_extractors().clone())
            .flatten()
            .map(|extractor| {
                (
                    extractor.get_result_name().to_owned(),
                    extractor.get_result_type().to_owned(),
                )
            })
            .collect::<Vec<_>>();
        self.dbp
            .topic_facade()
            .extraction_setup_searchable(topic_id, &name_and_type_slice)
            .await;
        Ok(())
    }

    /// Publish event to a topic.
    ///
    /// This will also validate event document schema (if any) and extract
    /// indexed values.
    ///
    /// Return `CorrelationToken` in serialized form.
    pub async fn publish_event_to_topic(
        &self,
        identity: &ClientIdentity,
        topic_id: &str,
        event_document: &str,
        priority: Option<u8>,
        descriptor_version: Option<DescriptorVersion>,
        correlation_token_opt: Option<String>,
    ) -> Result<String, MessageBrokerError> {
        self.access_control
            .assert_allowed_topic_write(identity, topic_id)
            .await?;
        let event_ts = self.trusted_time.get_timestamp_micros().ok_or_else(|| {
            MessageBrokerErrorKind::TrustedTimeError.error_with_msg(format!(
                "Refusing to accept published event to '{topic_id}' since time cannot be trusted."
            ))
        })?;
        let correlation_token = self
            .correlation_hotlist
            .validate_or_protect(correlation_token_opt, event_ts);
        self.dbp.topic_facade().ensure_topic_setup(topic_id).await?;
        let priority = priority
            .map(|priority| std::cmp::max(100, priority))
            .unwrap_or(100);
        // Validate schema (if present) and extract data into indexed columns (if available)
        let (additional_columns, event_descriptor_version) = self
            .pre_storage_processor
            .validate_and_extract(topic_id, event_document, descriptor_version)
            .await?;
        let unique_time = self
            .unique_timer_stamper
            .get_unique_timestamp(event_ts, priority);
        // Derive integrity protection
        let protection_ref = self
            .integrity_protector
            .derive_protection(topic_id, event_document, &unique_time)
            .await
            .as_string();
        let ret = self
            .dbp
            .event_facade()
            .event_persist(
                topic_id,
                TopicEvent::new(
                    event_document,
                    priority,
                    &protection_ref,
                    &correlation_token,
                    additional_columns,
                    event_descriptor_version
                        .as_ref()
                        .map(DescriptorVersion::as_encoded),
                    unique_time,
                ),
            )
            .await;
        self.object_count_tracker
            .inc(topic_id, &ObjectCountType::Events);
        if let Some(metrics) = &self.metrics {
            metrics.inc_published_events(topic_id, event_document.len());
        }
        Ok(ret)
    }

    /// Confirm that the delivery of an event has been recieved and should not
    /// be resent again.
    ///
    /// Note that this does not mean that the event has been processed or that
    /// the processing could crash after this confirmation.
    ///
    /// A client application could choose to wait with the confirmation until
    /// after processing is done at the risk of redelivery.
    pub async fn confirm_event_delivery(
        &self,
        identity: &ClientIdentity,
        topic_id: &str,
        encoded_unique_time: u64,
        delivery_instance_id: u16,
    ) -> Result<(), MessageBrokerError> {
        self.access_control
            .assert_allowed_topic_read(identity, topic_id)
            .await?;
        let consumer_id = identity.identity_string();
        if log::log_enabled!(log::Level::Trace) {
            log::trace!(
                "Receiving event confirmation for '{topic_id}/{consumer_id}/{encoded_unique_time}'."
            );
        }
        self.dbp.topic_facade().ensure_topic_setup(topic_id).await?;
        self.dbp
            .consumer_delivery_facade()
            .delivery_intent_mark_done(
                topic_id,
                consumer_id,
                UniqueTime::from(encoded_unique_time),
                delivery_instance_id,
            )
            .await;
        self.object_count_tracker
            .inc(topic_id, &ObjectCountType::DoneDeliveryIntents);
        if let Some(metrics) = &self.metrics {
            metrics.inc_delivered_events(topic_id);
        }
        Ok(())
    }

    /// Get next event to deliver.
    pub async fn get_event_by_consumer_and_topic(
        &self,
        identity: &ClientIdentity,
        topic_id: &str,
        baseline_ts: Option<u64>,
        descriptor_version: Option<DescriptorVersion>,
    ) -> Result<Option<(u64, String, String, u16)>, MessageBrokerError> {
        self.access_control
            .assert_allowed_topic_read(identity, topic_id)
            .await?;
        let consumer_id = identity.identity_string();
        self.dbp.topic_facade().ensure_topic_setup(topic_id).await?;
        let topic_consumer = self
            .consumers
            .by_topic_and_consumer_id(topic_id, consumer_id, baseline_ts, descriptor_version)
            .await?;
        if let Some((unique_time, document, protection_ref, correlation_token)) = topic_consumer
            .reserve_delivery_intent(descriptor_version)
            .await
            .map(EventDeliveryGist::into_parts)
        {
            if log::log_enabled!(log::Level::Trace) {
                log::trace!("Got event_delivery_gist in '{topic_id}'.");
            }
            let delivery_instance_id = self.unique_timer_stamper.get_instance_id();
            if !self
                .integrity_validator
                .validate_protection_ref_of_event(
                    topic_id,
                    &document,
                    &protection_ref,
                    &unique_time,
                )
                .await
            {
                let msg = "Integrity protection validation failed for event in '{topic_id}' with protection_id {protection_ref}.";
                log::warn!("{msg}");
                // This will never be delivered.. make sure it isn't attempted again!
                self.dbp
                    .consumer_delivery_facade()
                    .delivery_intent_mark_done(
                        topic_id,
                        consumer_id,
                        unique_time,
                        delivery_instance_id,
                    )
                    .await;
                self.object_count_tracker
                    .inc(topic_id, &ObjectCountType::DoneDeliveryIntents);
                Err(MessageBrokerErrorKind::IntegrityProtectionError.error_with_msg(msg))?;
            }
            if log::log_enabled!(log::Level::Trace) {
                log::trace!("Validation of event_delivery_gist in '{topic_id}' done.");
            }
            if let Some(metrics) = &self.metrics {
                metrics.inc_delivered_bytes(topic_id, document.len());
                let now = fragtale_client::time::get_timestamp_micros();
                metrics.report_publish_to_delivery_latency_micros(
                    topic_id,
                    now - unique_time.get_time_micros(),
                );
            }
            Ok(Some((
                unique_time.as_encoded(),
                document.to_owned(),
                correlation_token,
                delivery_instance_id,
            )))
        } else {
            Ok(None)
        }
    }

    /// Return an event by the correlation token or `None` if an event has not
    /// appeared before the timeout.
    pub async fn get_event_by_correlation_token(
        &self,
        identity: &ClientIdentity,
        topic_id: &str,
        correlation_token_str: &str,
    ) -> Result<Option<String>, MessageBrokerError> {
        let start_ts = fragtale_client::time::get_timestamp_micros();
        self.access_control
            .assert_allowed_topic_read(identity, topic_id)
            .await?;
        let consumer_id = identity.identity_string();
        // Create topic on the fly, if it did not exist.
        self.dbp.topic_facade().ensure_topic_setup(topic_id).await?;
        // Create a Consumer if it did not exist.
        self.consumers
            .by_topic_and_consumer_id(topic_id, consumer_id, None, None)
            .await?;
        let ret = self
            .correlation_hotlist
            .get_event_by_correlation_token(topic_id, correlation_token_str)
            .await;
        if let Some((unique_time, document, protection_ref, _correlation_token)) =
            ret.map(EventDeliveryGist::into_parts)
        {
            if !self
                .integrity_validator
                .validate_protection_ref_of_event(
                    topic_id,
                    &document,
                    &protection_ref,
                    &unique_time,
                )
                .await
            {
                Err(MessageBrokerErrorKind::IntegrityProtectionError.error())
            } else {
                let event_id = TopicEvent::event_id_from_document(&document);
                if log::log_enabled!(log::Level::Trace) {
                    log::trace!(
                        "correlation token: '{correlation_token_str}' -> event_id: {event_id}"
                    );
                }
                let delivery_instance_id = self.unique_timer_stamper.get_instance_id();
                let descriptor_version = None;
                let intent_ts_micros = fragtale_client::time::get_timestamp_micros();
                self.dbp
                    .consumer_delivery_facade()
                    .delivery_intent_insert_done(
                        topic_id,
                        consumer_id,
                        &event_id,
                        unique_time,
                        delivery_instance_id,
                        &descriptor_version,
                        intent_ts_micros,
                    )
                    .await;
                if let Some(metrics) = &self.metrics {
                    metrics.inc_delivered_events(topic_id);
                    metrics.inc_delivered_bytes(topic_id, document.len());
                    metrics.report_correlated_wait(
                        topic_id,
                        fragtale_client::time::get_timestamp_micros() - start_ts,
                    );
                }
                Ok(Some(document.to_owned()))
            }
        } else {
            Ok(None)
        }
    }

    /// Return the event document by the provided event identifier.
    pub async fn get_event_by_id(
        &self,
        identity: &ClientIdentity,
        topic_id: &str,
        event_id: &str,
    ) -> Result<Option<String>, MessageBrokerError> {
        self.access_control
            .assert_allowed_topic_read(identity, topic_id)
            .await?;
        let consumer_id = identity.identity_string();
        // Create topic on the fly, if it did not exist.
        self.dbp.topic_facade().ensure_topic_setup(topic_id).await?;
        // Create a Consumer if it did not exist.
        self.consumers
            .by_topic_and_consumer_id(topic_id, consumer_id, None, None)
            .await?;
        let ret_opt = self
            .dbp
            .event_facade()
            .event_by_id(topic_id, event_id)
            .await
            .map(EventDeliveryGist::into_parts);
        if let Some((unique_time, document, protection_ref, _correlation_token)) = ret_opt {
            if !self
                .integrity_validator
                .validate_protection_ref_of_event(
                    topic_id,
                    &document,
                    &protection_ref,
                    &unique_time,
                )
                .await
            {
                Err(
                    MessageBrokerErrorKind::IntegrityProtectionError.error_with_msg(format!(
                        "Failed to verify integrity for event with id '{event_id}'."
                    )),
                )
            } else {
                let delivery_instance_id = self.unique_timer_stamper.get_instance_id();
                let descriptor_version = None;
                let intent_ts_micros = fragtale_client::time::get_timestamp_micros();
                self.dbp
                    .consumer_delivery_facade()
                    .delivery_intent_insert_done(
                        topic_id,
                        consumer_id,
                        event_id,
                        unique_time,
                        delivery_instance_id,
                        &descriptor_version,
                        intent_ts_micros,
                    )
                    .await;
                if let Some(metrics) = &self.metrics {
                    metrics.inc_delivered_events(topic_id);
                    metrics.inc_delivered_bytes(topic_id, document.len());
                }
                Ok(Some(document))
            }
        } else {
            Ok(None)
        }
    }

    /// Return event identifiers that match an indexed query.
    pub async fn get_event_ids_by_indexed_column(
        &self,
        identity: &ClientIdentity,
        topic_id: &str,
        index_column: &str,
        index_key: &str,
    ) -> Result<Vec<String>, MessageBrokerError> {
        self.access_control
            .assert_allowed_topic_read(identity, topic_id)
            .await?;
        let consumer_id = identity.identity_string();
        if log::log_enabled!(log::Level::Trace) {
            log::trace!("Consumer '{consumer_id}' queried index {topic_id}.{index_column}.");
        }
        // Create topic on the fly, if it did not exist.
        self.dbp.topic_facade().ensure_topic_setup(topic_id).await?;
        // Create a Consumer if it did not exist.
        self.consumers
            .by_topic_and_consumer_id(topic_id, consumer_id, None, None)
            .await?;

        let ret = self
            .dbp
            .event_facade()
            .event_ids_by_index(topic_id, index_column, index_key)
            .await;
        Ok(ret)
    }
}
