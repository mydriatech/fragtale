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

pub mod dbp;
pub mod mb {
    //! Message Broker objects.

    pub mod consumers {
        //! Objects related to delivery of events to consumers.

        mod delivery_intent_template;
        mod delivery_intent_template_insertable;
        mod event_delivery_gist;

        pub use self::delivery_intent_template::DeliveryIntentTemplate;
        pub use self::delivery_intent_template_insertable::DeliveryIntentTemplateInsertable;
        pub use self::event_delivery_gist::EventDeliveryGist;
    }
    pub mod correlation {
        //! Tracking outcomes of a request event.

        mod correlation_result_listener;

        pub use self::correlation_result_listener::CorrelationResultListener;
    }
    mod object_count_tracker {
        //! Tracking counts of objects of specific types on instances.

        mod object_count;
        mod object_count_type;

        pub use self::object_count::ObjectCount;
        pub use self::object_count_type::ObjectCountType;
    }
    mod extracted_value;
    mod message_broker_error;
    mod topic_event;
    mod unique_time;

    pub use self::extracted_value::ExtractedValue;
    pub use self::message_broker_error::MessageBrokerError;
    pub use self::message_broker_error::MessageBrokerErrorKind;
    pub use self::object_count_tracker::ObjectCount;
    pub use self::object_count_tracker::ObjectCountType;
    pub use self::topic_event::TopicEvent;
    pub use self::unique_time::UniqueTime;
}
