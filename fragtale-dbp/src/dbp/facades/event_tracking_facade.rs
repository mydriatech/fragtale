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

//! Database facade for operation related to tracking object counts.

use crate::mb::ObjectCount;
use crate::mb::ObjectCountType;
use crate::mb::correlation::CorrelationResultListener;
use std::sync::Arc;

/// Database facade for operation related to tracking object counts.
#[async_trait::async_trait]
pub trait EventTrackingFacade: Send + Sync {
    /// Persist the local count of the [ObjectCountType]
    async fn object_count_insert(
        &self,
        topic_id: &str,
        object_count_type: &ObjectCountType,
        instance_id: u16,
        value: u64,
    );

    /// Get the recent count of the [ObjectCountType]
    async fn object_count_by_topic_and_type(
        &self,
        topic_id: &str,
        object_count_type: &ObjectCountType,
    ) -> Vec<ObjectCount>;

    /// Notify [CorrelationResultListener] of existing correlation results.
    ///
    /// Return `true` if at least on notification was made.
    async fn track_new_events_in_topic(
        &self,
        topic_id: &str,
        correlation_hotlist: Box<Arc<dyn CorrelationResultListener>>,
        hotlist_duration_micros: u64,
    ) -> bool;
}
