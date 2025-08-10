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

//! Trait for structures that can accept multiple [DeliveryIntentTemplate]s.

use super::DeliveryIntentTemplate;

/// Trait for structures that can accept multiple [DeliveryIntentTemplate]s.
pub trait DeliveryIntentTemplateInsertable: Send + Sync {
    /// Insert of [DeliveryIntentTemplate].
    fn insert(&self, delivery_intent_template: DeliveryIntentTemplate);

    /// Return `true` when structure is estimated to be full and can not longer
    /// accept [DeliveryIntentTemplate]s.
    fn is_full(&self) -> bool;
}
