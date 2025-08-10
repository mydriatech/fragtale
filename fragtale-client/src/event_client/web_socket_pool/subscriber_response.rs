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

//! WebSocket messages sent from server to client.

use serde::Deserialize;
use serde::Serialize;

/// WebSocket messages sent from server to client.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriberResponse {
    /// Delivery of a new event to the client.
    Next {
        ///  UniqueTime of the event.
        encoded_unique_time: u64,
        /// todo
        event_document: String,
        /// todo
        correlation_token: String,
        /// todo
        delivery_instance_id: u16,
    },
}
