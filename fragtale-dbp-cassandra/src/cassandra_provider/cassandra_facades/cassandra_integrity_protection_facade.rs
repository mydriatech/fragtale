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

//! Cassandra implementation of [IntegrityProtectionFacade].

use crate::CassandraProvider;
use crate::cassandra_provider::entity::IntegrityByLevelAndTimeEntity;
use crate::cassandra_provider::entity::IntegrityByLevelAndTimeLookupEntity;
use crate::cassandra_provider::entity::IntegrityEntity;
use fragtale_dbp::dbp::facades::IntegrityProtectionFacade;
use std::sync::Arc;

/// Cassandra implementation of [IntegrityProtectionFacade].
pub struct CassandraIntegrityProtectionFacade {
    cassandra_provider: Arc<CassandraProvider>,
}

impl CassandraIntegrityProtectionFacade {
    /// Return a new instance.
    pub fn new(cassandra_provider: &Arc<CassandraProvider>) -> Self {
        Self {
            cassandra_provider: Arc::clone(cassandra_provider),
        }
    }
}

#[async_trait::async_trait]
impl IntegrityProtectionFacade for CassandraIntegrityProtectionFacade {
    async fn integrity_protection_by_id_and_ts(
        &self,
        topic_id: &str,
        id: &str,
        protection_ts_micros: u64,
    ) -> Option<(String, Option<String>)> {
        IntegrityEntity::select_by_protection_id(
            &self.cassandra_provider,
            topic_id,
            protection_ts_micros,
            id,
        )
        .await
        .map(|ipe| {
            (
                ipe.get_protection_data().to_owned(),
                ipe.get_protection_ref().to_owned(),
            )
        })
    }

    async fn integrity_protection_persist(
        &self,
        topic_id: &str,
        id: &str,
        protection_data: &str,
        protection_ts_micros: u64,
        level: u8,
    ) {
        // Persist protection
        IntegrityEntity::new(
            protection_ts_micros,
            id.to_owned(),
            protection_data.to_owned(),
        )
        .insert(&self.cassandra_provider, topic_id)
        .await;
        // Persist protection lookup
        let lookup_ts_bucket =
            IntegrityByLevelAndTimeEntity::to_lookup_ts_bucket(level, protection_ts_micros);
        IntegrityByLevelAndTimeEntity::new(
            level,
            lookup_ts_bucket,
            protection_ts_micros,
            id.to_owned(),
        )
        .insert(&self.cassandra_provider, topic_id)
        .await;
        // Persist protection lookup lookup...
        IntegrityByLevelAndTimeLookupEntity::new(level, lookup_ts_bucket)
            .insert(&self.cassandra_provider, topic_id)
            .await;
    }

    async fn integrity_protection_set_protection_ref(
        &self,
        topic_id: &str,
        id: &str,
        protection_ts_micros: u64,
        protection_ref: &str,
    ) {
        // Upsert protection ref
        IntegrityEntity::upsert_protection_ref(
            &self.cassandra_provider,
            topic_id,
            protection_ts_micros,
            id,
            protection_ref,
        )
        .await;
    }

    /// Return the next buckets starting point
    async fn integrity_protection_next_starting_point_to_process(
        &self,
        topic_id: &str,
        level_in: u8,
        now_micros: u64,
    ) -> Option<u64> {
        let level_out = level_in + 1;
        // Get the latest protection_ts from the output level: get bucket
        if let Some(lookup_ts_bucket) = IntegrityByLevelAndTimeLookupEntity::select_latest_by_level(
            &self.cassandra_provider,
            topic_id,
            level_out,
        )
        .await
        .as_ref()
        .map(IntegrityByLevelAndTimeLookupEntity::get_lookup_ts_bucket)
        {
            if log::log_enabled!(log::Level::Trace) {
                log::trace!(
                    "'{topic_id}' has a latest bucket {lookup_ts_bucket} at level {level_out}."
                );
            }
            // Get the latest protection_ts from the output level: get ts
            if let Some(latest_protection_ts_in_level) =
                IntegrityByLevelAndTimeEntity::select_latest_by_level_and_ts_bucket(
                    &self.cassandra_provider,
                    topic_id,
                    level_out,
                    lookup_ts_bucket,
                )
                .await
                .as_ref()
                .map(IntegrityByLevelAndTimeEntity::get_protection_ts)
            {
                // Transform output levels protection_ts into this levels bucket
                let lookup_ts_bucket_latest = IntegrityByLevelAndTimeEntity::to_lookup_ts_bucket(
                    level_in,
                    latest_protection_ts_in_level,
                );
                // Transform now into this levels bucket
                let lookup_ts_bucket_now = IntegrityByLevelAndTimeEntity::to_lookup_ts_bucket(
                    level_in,
                    // Ensure that there is some time to persist the last protections
                    now_micros + 10_000_000,
                );
                // Get the next bucket at the input level that has entires
                let candidate_ts_bucket =
                    IntegrityByLevelAndTimeLookupEntity::select_next_by_level(
                        &self.cassandra_provider,
                        topic_id,
                        level_in,
                        lookup_ts_bucket_latest,
                    )
                    .await
                    .as_ref()
                    .map(IntegrityByLevelAndTimeLookupEntity::get_lookup_ts_bucket);
                if let Some(candidate_ts_bucket) = candidate_ts_bucket {
                    if lookup_ts_bucket_now > candidate_ts_bucket {
                        // There is something to handle.
                        return Some(candidate_ts_bucket);
                    } else if log::log_enabled!(log::Level::Trace) {
                        log::trace!(
                            "Candidate bucket {candidate_ts_bucket} at level {level_in} in '{topic_id}' was not old enough yet to be consolidated. (now bucket: {lookup_ts_bucket_now})"
                        );
                    }
                } else if log::log_enabled!(log::Level::Trace) {
                    log::trace!(
                        "No newer candidate bucket with entries at level {level_in} in '{topic_id}'. (Already covered lookup_ts_bucket_latest: {lookup_ts_bucket_latest}.)"
                    );
                }
            } else {
                log::warn!(
                    "No latest_protection_ts_in_level. (This should not happen.) topic_id: '{topic_id}', level: {level_out}, lookup_ts_bucket: {lookup_ts_bucket}"
                );
            }
        } else {
            // There is no bucket yet at this level.. find the earlist protection on the previous level and see if it is old enough
            if log::log_enabled!(log::Level::Trace) {
                log::trace!("Topic '{topic_id}' has no bucket yet at level {level_out}.");
            }
            if let Some(candidate_ts_bucket) =
                IntegrityByLevelAndTimeLookupEntity::select_first_by_level(
                    &self.cassandra_provider,
                    topic_id,
                    level_in,
                )
                .await
                .as_ref()
                .map(IntegrityByLevelAndTimeLookupEntity::get_lookup_ts_bucket)
            {
                // Transform now into this levels bucket
                let lookup_ts_bucket_now = IntegrityByLevelAndTimeEntity::to_lookup_ts_bucket(
                    level_in,
                    // Ensure that there is some time to persist the last protections
                    now_micros + 10_000_000,
                );
                if lookup_ts_bucket_now > candidate_ts_bucket {
                    // There is something to handle.
                    return Some(candidate_ts_bucket);
                } else if log::log_enabled!(log::Level::Trace) {
                    log::trace!(
                        "Candidate bucket {candidate_ts_bucket} at level {level_in} in '{topic_id}' was not old enough yet to be consolidated. (now bucket: {lookup_ts_bucket_now})"
                    );
                }
            } else if log::log_enabled!(log::Level::Trace) {
                log::trace!("Topic '{topic_id}' has no bucket yet at level {level_in}.");
            }
        }
        None
    }

    async fn integrity_batch_in_interval_by_level_and_time(
        &self,
        topic_id: &str,
        level: u8,
        from_protections_ts_micros: u64,
        max_results: usize,
    ) -> Vec<(String, u64, String, Option<String>)> {
        let mut pointers = IntegrityByLevelAndTimeEntity::select_next_by_level_and_ts(
            &self.cassandra_provider,
            topic_id,
            level,
            from_protections_ts_micros,
            max_results,
        )
        .await
        .into_iter()
        .map(|iblate| {
            (
                iblate.get_protection_id().to_owned(),
                iblate.get_protection_ts().to_owned(),
            )
        })
        .collect::<Vec<_>>();
        if pointers.is_empty() {
            // If we have exhausted the bucket, go for the next populated one
            if let Some(start_of_next_bucket) =
                IntegrityByLevelAndTimeLookupEntity::select_next_by_level(
                    &self.cassandra_provider,
                    topic_id,
                    level,
                    from_protections_ts_micros,
                )
                .await
                .as_ref()
                .map(IntegrityByLevelAndTimeLookupEntity::get_lookup_ts_bucket)
            {
                pointers = IntegrityByLevelAndTimeEntity::select_next_by_level_and_ts(
                    &self.cassandra_provider,
                    topic_id,
                    level,
                    start_of_next_bucket,
                    max_results,
                )
                .await
                .into_iter()
                .map(|iblate| {
                    (
                        iblate.get_protection_id().to_owned(),
                        iblate.get_protection_ts().to_owned(),
                    )
                })
                .collect::<Vec<_>>();
            }
        }
        let mut ret = Vec::with_capacity(pointers.len());
        for (protection_id, protection_ts_micros) in pointers {
            if let Some(ie) = IntegrityEntity::select_by_protection_id(
                &self.cassandra_provider,
                topic_id,
                protection_ts_micros,
                &protection_id,
            )
            .await
            {
                ret.push((
                    protection_id.to_owned(),
                    protection_ts_micros,
                    ie.get_protection_data().to_owned(),
                    ie.get_protection_ref().to_owned(),
                ));
            }
        }
        ret
    }
}
