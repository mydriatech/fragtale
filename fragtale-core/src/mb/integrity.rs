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

//! Event integrity protection.

pub mod common {
    //! Common structs for event integrity protection.

    mod integrity_error;
    mod integrity_protection;
    mod integrity_protection_reference;
    mod integrity_secrets_holder;

    pub use self::integrity_error::*;
    pub use self::integrity_protection::*;
    pub use self::integrity_protection_reference::*;
    pub use self::integrity_secrets_holder::*;
}
pub mod integrity_consolidator;
pub mod integrity_protector;
pub mod integrity_validator;

pub use self::integrity_consolidator::*;
pub use self::integrity_protector::*;
pub use self::integrity_validator::*;
