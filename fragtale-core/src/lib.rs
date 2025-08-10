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

pub mod conf;
pub mod mb;
pub mod util {
    //! Utilities

    mod bdtd_builder;
    mod lockless_caching_filter;
    mod log_scope_duration;
    mod signal_awaiter;
    mod trusted_time;

    pub use self::bdtd_builder::*;
    pub use self::lockless_caching_filter::*;
    pub use self::log_scope_duration::*;
    pub use self::signal_awaiter::*;
    pub use self::trusted_time::*;
}

pub use self::conf::AppConfig;
pub use self::mb::MessageBroker;

pub use fragtale_client::mb::event_descriptor::DescriptorVersion;
pub use fragtale_client::mb::event_descriptor::EventDescriptor;
pub use fragtale_client::mb::event_descriptor::EventSchema;
pub use fragtale_client::mb::event_descriptor::Extractor;
