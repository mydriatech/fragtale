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

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

pub(crate) mod authentication {
    mod client_authentication;

    pub use self::client_authentication::*;
}
pub mod mb {
    //! Message broker objects.

    pub mod correlation_token;
    pub mod event_descriptor;
}
mod event_client;
mod rest_api_client;
pub mod time;

pub use event_client::EventClient;
pub use event_client::EventProcessor;
pub use event_client::EventSource;
pub use rest_api_client::RestApiClient;

pub use self::event_client::SubscriberCommand;
pub use self::event_client::SubscriberResponse;
