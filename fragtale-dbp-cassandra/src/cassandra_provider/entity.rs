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

//! Entities for Cassandra implementation.

mod consumer_entity;
mod delivery_intent_entity;
mod event_descriptor_entity;
mod event_entity;
mod event_id_by_unique_time_entity;
mod identity_claim_entity;
mod integrity_by_level_and_time_entity;
mod integrity_by_level_and_time_lookup_entity;
mod integrity_entity;
mod object_count_entity;
mod resource_grant_entity;
mod topic_entity;
mod unique_time_bucket_by_shelf;

pub use self::consumer_entity::ConsumerEntity;
pub use self::delivery_intent_entity::DeliveryIntentEntity;
pub use self::event_descriptor_entity::EventDescriptorEntity;
pub use self::event_entity::EventEntity;
pub use self::event_id_by_unique_time_entity::EventIdByUniqueTimeEntity;
pub use self::identity_claim_entity::IdentityClaimEntity;
pub use self::integrity_by_level_and_time_entity::IntegrityByLevelAndTimeEntity;
pub use self::integrity_by_level_and_time_lookup_entity::IntegrityByLevelAndTimeLookupEntity;
pub use self::integrity_entity::IntegrityEntity;
pub use self::object_count_entity::ObjectCountEntity;
pub use self::resource_grant_entity::ResourceGrantEntity;
pub use self::topic_entity::TopicEntity;
pub use self::unique_time_bucket_by_shelf::UniqueTimeBucketByShelfEntity;

/// Conversion from unsigned to signed primitive.
pub trait FromUnsignedOrDefault<T> {
    fn from_unsigned(value: T) -> Self;
}

impl FromUnsignedOrDefault<u64> for i64 {
    /// Convert `u64` to `i64`. Return 0 on overflow.
    fn from_unsigned(value: u64) -> i64 {
        i64::try_from(value).unwrap_or_default()
    }
}

impl FromUnsignedOrDefault<u32> for i32 {
    /// Convert `u32` to `i32`. Return 0 on overflow.
    fn from_unsigned(value: u32) -> i32 {
        i32::try_from(value).unwrap_or_default()
    }
}

impl FromUnsignedOrDefault<u16> for i16 {
    /// Convert `u16` to `i16`. Return 0 on overflow.
    fn from_unsigned(value: u16) -> i16 {
        i16::try_from(value).unwrap_or_default()
    }
}

impl FromUnsignedOrDefault<u8> for i8 {
    /// Convert `u8` to `i8`. Return 0 on overflow.
    fn from_unsigned(value: u8) -> i8 {
        i8::try_from(value).unwrap_or_default()
    }
}

/// Conversion from signed to unsigned primitive.
pub trait FromSignedOrDefault<T> {
    fn from_signed(value: T) -> Self;
}

impl FromSignedOrDefault<i64> for u64 {
    /// Convert `i64` to `u64`. Return 0 on overflow.
    fn from_signed(value: i64) -> u64 {
        u64::try_from(value).unwrap_or_default()
    }
}

impl FromSignedOrDefault<i32> for u32 {
    /// Convert `i32` to `u32`. Return 0 on overflow.
    fn from_signed(value: i32) -> u32 {
        u32::try_from(value).unwrap_or_default()
    }
}

impl FromSignedOrDefault<i16> for u16 {
    /// Convert `i16` to `u16`. Return 0 on overflow.
    fn from_signed(value: i16) -> u16 {
        u16::try_from(value).unwrap_or_default()
    }
}

impl FromSignedOrDefault<i8> for u8 {
    /// Convert `i8` to `u8`. Return 0 on overflow.
    fn from_signed(value: i8) -> u8 {
        u8::try_from(value).unwrap_or_default()
    }
}
