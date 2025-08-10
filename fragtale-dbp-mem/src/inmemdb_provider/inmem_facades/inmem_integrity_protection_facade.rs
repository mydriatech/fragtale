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

//! Ephemeral in-memory implementation of [IntegrityProtectionFacade].

use crossbeam_skiplist::SkipMap;
use fragtale_dbp::dbp::facades::IntegrityProtectionFacade;
use std::sync::Arc;

/// Ephemeral in-memory implementation of [IntegrityProtectionFacade].
#[derive(Debug)]
pub struct InMemIntegrityProtection {
    pub protection_ts_micros: u64,
    pub protection_data: String,
    pub protection_ref: Option<String>,
}

#[derive(Default)]
pub struct InMemIntegrityProtectionFacade {
    integrity_protection_by_topic: SkipMap<String, SkipMap<String, Arc<InMemIntegrityProtection>>>,
    ip_processed_by_level_by_topic: SkipMap<String, SkipMap<u8, SkipMap<u64, String>>>,
}

impl InMemIntegrityProtectionFacade {
    fn interval_by_level(level: u8) -> u64 {
        match level {
            // Level 0: Bucket into 4 minute intevals
            0 => 1_000_000u64 * 240,
            // Level 1: Bucket into 7 day intevals
            1 => 1_000_000u64 * 3_600 * 24 * 7,
            // Level 2: Bucket into 365 day intevals
            2 => 1_000_000u64 * 3_600 * 24 * 365,
            unsupported_level => {
                panic!("Bucketing for level {unsupported_level} is not implemented.")
            }
        }
    }
}

#[async_trait::async_trait]
impl IntegrityProtectionFacade for InMemIntegrityProtectionFacade {
    async fn integrity_protection_persist(
        &self,
        topic_id: &str,
        id: &str,
        protection_data: &str,
        protection_ts_micros: u64,
        _level: u8,
    ) {
        self.integrity_protection_by_topic
            .get_or_insert_with(topic_id.to_owned(), SkipMap::default)
            .value()
            .insert(
                id.to_owned(),
                Arc::new(InMemIntegrityProtection {
                    protection_data: protection_data.to_owned(),
                    protection_ts_micros,
                    protection_ref: None,
                }),
            );
    }

    async fn integrity_protection_set_protection_ref(
        &self,
        topic_id: &str,
        id: &str,
        protection_ts_micros: u64,
        protection_ref: &str,
    ) {
        let in_mem_ip = self
            .integrity_protection_by_topic
            .get_or_insert_with(topic_id.to_owned(), SkipMap::default)
            .value()
            .get(id)
            .map(|entry| Arc::clone(entry.value()))
            .unwrap();
        self.integrity_protection_by_topic
            .get_or_insert_with(topic_id.to_owned(), SkipMap::default)
            .value()
            .insert(
                id.to_owned(),
                Arc::new(InMemIntegrityProtection {
                    protection_data: in_mem_ip.protection_data.to_owned(),
                    protection_ts_micros,
                    protection_ref: Some(protection_ref.to_owned()),
                }),
            );
    }

    async fn integrity_protection_by_id_and_ts(
        &self,
        topic_id: &str,
        id: &str,
        protection_ts_micros: u64,
    ) -> Option<(String, Option<String>)> {
        self.integrity_protection_by_topic
            .get_or_insert_with(topic_id.to_owned(), SkipMap::default)
            .value()
            .get(id)
            .map(|entry| Arc::clone(entry.value()))
            .filter(|in_mem_prot| in_mem_prot.protection_ts_micros.eq(&protection_ts_micros))
            .map(|in_mem_prot| {
                (
                    in_mem_prot.protection_data.clone(),
                    in_mem_prot.protection_ref.clone(),
                )
            })
    }

    async fn integrity_protection_next_starting_point_to_process(
        &self,
        topic_id: &str,
        level: u8,
        now_micros: u64,
    ) -> Option<u64> {
        let opt = self
            .ip_processed_by_level_by_topic
            .get_or_insert_with(topic_id.to_owned(), SkipMap::default)
            .value()
            .get_or_insert_with(level, SkipMap::default)
            .value()
            .back()
            .map(|entry| (entry.key().to_owned(), entry.value().to_owned()));
        log::trace!("integrity_protection_next_starting_point_to_process: opt: {opt:?}");
        if let Some((protection_ts_micros, _protection_id)) = opt {
            // Now we have the last consolidated, figure the corresponding bucket
            let interval_micros = Self::interval_by_level(level);
            let bucket = protection_ts_micros - protection_ts_micros % interval_micros;
            let now_bucket = now_micros - now_micros % interval_micros;
            let candidate = bucket + interval_micros;
            log::debug!(
                "integrity_protection_next_starting_point_to_process: level: {level}, now_bucket: {now_bucket}, candidate: {candidate}"
            );
            if now_bucket > candidate {
                return Some(candidate);
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
        // Do a super-inefficient full table scan
        // Keep within the "bucket"
        let interval_micros = Self::interval_by_level(level);
        let bucket = from_protections_ts_micros - from_protections_ts_micros % interval_micros;
        let next_bucket = bucket + interval_micros;
        log::debug!(
            "integrity_batch_in_interval_by_level_and_time: level: {level}, bucket: {bucket}, next_bucket: {next_bucket}"
        );
        let ret = self
            .integrity_protection_by_topic
            .get_or_insert_with(topic_id.to_owned(), SkipMap::default)
            .value()
            .iter()
            .map(|entry| (entry.key().to_owned(), Arc::clone(entry.value())))
            .skip_while(|(_protection_id, imip)| {
                imip.protection_ts_micros < from_protections_ts_micros
            })
            .take(max_results)
            .take_while(|(_protection_id, imip)| imip.protection_ts_micros < next_bucket)
            .map(|(protection_id, imip)| {
                (
                    protection_id,
                    imip.protection_ts_micros,
                    imip.protection_data.to_owned(),
                    imip.protection_ref.to_owned(),
                )
            })
            .collect::<Vec<_>>();
        if let Some((protection_id, protection_ts_micros, _protection_data, _protection_ref)) =
            ret.last()
        {
            log::debug!(
                "last: level: {level}, protection_ts_micros: {protection_ts_micros}, protection_id: {protection_id}"
            );
            // Update "ip_processed_by_level"
            self.ip_processed_by_level_by_topic
                .get_or_insert_with(topic_id.to_owned(), SkipMap::default)
                .value()
                .get_or_insert_with(level, SkipMap::default)
                .value()
                .insert(*protection_ts_micros, protection_id.to_owned());
        }
        ret
    }
}
