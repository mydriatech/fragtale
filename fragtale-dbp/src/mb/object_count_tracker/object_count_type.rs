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

//! Type of counted object.

/// Type of counted object.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ObjectCountType {
    /// Events.
    Events,
    /// Intents to deliver.
    ReservedDeliveryIntents,
    /// Finished delivery intents.
    DoneDeliveryIntents,
}

impl ObjectCountType {
    /// Return the name of the type.
    pub fn name(&self) -> &str {
        match self {
            Self::Events => "events",
            Self::ReservedDeliveryIntents => "reserved",
            Self::DoneDeliveryIntents => "done",
        }
    }

    /// Return a new instance from the name of the type.
    ///
    /// Panics if the name is unknown.
    pub fn by_name(name: &str) -> Self {
        match name {
            "events" => Self::Events,
            "reserved" => Self::ReservedDeliveryIntents,
            "done" => Self::DoneDeliveryIntents,
            s => {
                panic!("Unsupported ObjectCountType '{s}'.");
            }
        }
    }
}
