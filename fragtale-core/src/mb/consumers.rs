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

//! Track connected consumers.

pub mod topic_consumer;

pub use self::topic_consumer::TopicConsumer;
use crate::mb::object_count_tracker::ObjectCountTracker;
use crossbeam_skiplist::SkipMap;
use fragtale_client::mb::event_descriptor::DescriptorVersion;
use fragtale_dbp::dbp::DatabaseProvider;
use fragtale_dbp::dbp::facades::DatabaseProviderFacades;
use fragtale_dbp::mb::MessageBrokerError;
use std::sync::Arc;

/// Tracks all connected consumers.
pub struct Consumers {
    dbp: Arc<DatabaseProvider>,
    object_count_tracker: Arc<ObjectCountTracker>,
    consumers: SkipMap<String, Arc<TopicConsumer>>,
    instance_id: u16,
}

impl Consumers {
    /// Return a new instance.
    pub fn new(
        dbp: &Arc<DatabaseProvider>,
        object_count_tracker: &Arc<ObjectCountTracker>,
        instance_id: u16,
    ) -> Arc<Self> {
        Arc::new(Self {
            dbp: Arc::clone(dbp),
            object_count_tracker: Arc::clone(object_count_tracker),
            consumers: SkipMap::new(),
            instance_id,
        })
    }

    /// Returns an existing [TopicConsumer] or a new persisted.
    pub async fn by_topic_and_consumer_id(
        &self,
        topic_id: &str,
        consumer_id: &str,
        baseline_ts: Option<u64>,
        descriptor_version: Option<DescriptorVersion>,
    ) -> Result<Arc<TopicConsumer>, MessageBrokerError> {
        let key = topic_id.to_owned() + "." + consumer_id;
        if let Some(entry) = self.consumers.get(&key) {
            Ok(Arc::clone(entry.value()))
        } else {
            let encoded_descriptor_version = descriptor_version
                .as_ref()
                .map(DescriptorVersion::as_encoded);
            // Does the consumer already exists, but is just not cached in this instance?
            self.dbp
                .consumer_delivery_facade()
                .ensure_consumer_setup(
                    topic_id,
                    consumer_id,
                    baseline_ts,
                    encoded_descriptor_version,
                )
                .await?;
            let entry = self.consumers.get_or_insert_with(key, || {
                TopicConsumer::new(
                    &self.dbp,
                    &self.object_count_tracker,
                    topic_id,
                    consumer_id,
                    self.instance_id,
                )
            });
            Ok(Arc::clone(entry.value()))
        }
    }
}
