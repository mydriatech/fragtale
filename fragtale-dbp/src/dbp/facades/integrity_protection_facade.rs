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

//! Database facade for operation related to event integrity protection.

/// Database facade for operation related to event integrity protection.
#[async_trait::async_trait]
pub trait IntegrityProtectionFacade: Send + Sync {
    /// Persist new `protection_data`.
    async fn integrity_protection_persist(
        &self,
        topic_id: &str,
        id: &str,
        protection_data: &str,
        protection_ts_micros: u64,
        level: u8,
    );

    /// Upsert integrirty protection with `protection_ref`.
    async fn integrity_protection_set_protection_ref(
        &self,
        topic_id: &str,
        id: &str,
        protection_ts_micros: u64,
        protection_ref: &str,
    );

    /// Get protection data and ref by `id` and `protection_ts_micros`.
    async fn integrity_protection_by_id_and_ts(
        &self,
        topic_id: &str,
        id: &str,
        protection_ts_micros: u64,
    ) -> Option<(String, Option<String>)>;

    /// Get the next bucket to process or None if there is nothing to do.
    async fn integrity_protection_next_starting_point_to_process(
        &self,
        topic_id: &str,
        level: u8,
        now_micros: u64,
    ) -> Option<u64>;

    /// Returns a vector of (`protection_id`, `protection_ts_micros`,
    /// `protection_data`, `protection_ref`).
    async fn integrity_batch_in_interval_by_level_and_time(
        &self,
        topic_id: &str,
        level: u8,
        from_protections_ts_micros: u64,
        max_results: usize,
    ) -> Vec<(String, u64, String, Option<String>)>;
}
