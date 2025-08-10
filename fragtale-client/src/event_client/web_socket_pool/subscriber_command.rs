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

//! WebSocket messages sent from client to server.

use serde::Deserialize;
use serde::Serialize;

/// WebSocket messages sent from client to server.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriberCommand {
    /// Acknowledge (confirm) that an event has been recieved by the client.
    AckDelivery {
        /// UniqueTime of the event.
        encoded_unique_time: u64,
        /// The instance id responsilble for the acknowledged delivery.
        delivery_instance_id: u16,
    },
    /// Publish a new event to the server.
    Publish {
        /// Relative priority of the message. 0-100 (100 is highest priority).
        priority: Option<u8>,
        /// The event document.
        event_document: String,
        /// If this event was created due to recieving an event, the
        /// `correlation_token` of the recieved event must be use here for
        /// for correlation of requests to work.
        correlation_token: Option<String>,
        /// Event descriptor version the event document adheres to.
        descriptor_version: Option<u64>,
    },
}
