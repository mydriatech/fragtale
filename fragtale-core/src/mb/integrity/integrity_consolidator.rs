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

//! Consolidation of event integrity in hierarchys.

use super::IntegrityProtector;
use super::IntegrityValidator;
use super::common::IntegrityProtection;
use super::common::IntegritySecretsHolder;
use crate::mb::unique_time_stamper::UniqueTimeStamper;
use fragtale_dbp::dbp::DatabaseProvider;
use fragtale_dbp::dbp::facades::DatabaseProviderFacades;
use futures::StreamExt;
use std::sync::Arc;
use tyst::encdec::hex::ToHex;

/// Consolidator event integrity in hierarchys.
pub struct IntegrityConsolidationService {
    ish: Arc<IntegritySecretsHolder>,
    dbp: Arc<DatabaseProvider>,
    protector: Arc<IntegrityProtector>,
    validator: Arc<IntegrityValidator>,
    unique_timer_stamper: Arc<UniqueTimeStamper>,
}

impl IntegrityConsolidationService {
    /// Return a new instance.
    pub async fn new(
        integrity_secrets_holder: &Arc<IntegritySecretsHolder>,
        dbp: &Arc<DatabaseProvider>,
        integrity_protector: &Arc<IntegrityProtector>,
        integrity_validator: &Arc<IntegrityValidator>,
        unique_timer_stamper: &Arc<UniqueTimeStamper>,
    ) -> Arc<Self> {
        Arc::new(Self {
            ish: Arc::clone(integrity_secrets_holder),
            dbp: Arc::clone(dbp),
            protector: Arc::clone(integrity_protector),
            validator: Arc::clone(integrity_validator),
            unique_timer_stamper: Arc::clone(unique_timer_stamper),
        })
        .run()
        .await
    }

    async fn run(self: Arc<Self>) -> Arc<Self> {
        let self_clone = Arc::clone(&self);
        tokio::spawn(async move { self_clone.run_update_and_consolidation().await });
        self
    }

    async fn run_update_and_consolidation(&self) {
        // If this is the oldest instance
        //  -> all nodes are using the new secret for new events from now on
        //  -> after the current level 1 interval is over, it is safe to regen secret again
        let start_ts_micros = fragtale_client::time::get_timestamp_micros();
        let mut notified = false;
        let mut has_run_secret_validation = false;
        loop {
            // Is this the lowest claimed instance id?
            if self.unique_timer_stamper.is_oldest_instance().await {
                if log::log_enabled!(log::Level::Trace) {
                    log::trace!("I will be running consolidation service.");
                }
                let mut pre_consolidation_ts_micros = 0;
                let mut from = None;
                loop {
                    let (topics, more) = self.dbp.topic_facade().get_topic_ids(&from).await;
                    if log::log_enabled!(log::Level::Trace) {
                        log::trace!("Topics: {topics:?}");
                    }
                    // Priority number #1 check if current secret has changed and update all if so
                    if !has_run_secret_validation {
                        has_run_secret_validation = true;
                        self.run_integrity_protection_update(&topics).await;
                    }
                    if from.is_none() {
                        pre_consolidation_ts_micros = fragtale_client::time::get_timestamp_micros();
                    }
                    // Priority number #2 Start consolidation
                    for topic_id in &topics {
                        self.run_consolidation_for_topic(topic_id).await
                    }
                    if !more {
                        break;
                    }
                    from = topics.last().cloned();
                }
                // Detect if it is safe to regenerate the secret!
                if !notified
                    && pre_consolidation_ts_micros / (1_000_000 * 240)
                        > start_ts_micros / (1_000_000 * 240)
                {
                    // Consolidation should have protected the level L1 events by now with level L2
                    notified = true;
                    log::info!("It is now safe to regenerate the shared secrets.");
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_micros(10_000_000)).await;
        }
    }

    async fn run_integrity_protection_update(&self, topics: &[String]) {
        // Even if a switch is used in the Helm deploy to regen, there should still
        // be a time limit safety of 5 minutes to avoid having to rewrite level L0.
        //let (current_oid, current_secret, _ts) = self.app_config.integrity.current_secret();
        //let (previous_oid, previous_secret) = self.app_config.integrity.previous_secret();
        let mut update_count = 0;
        for topic_id in topics {
            // for L2, check all buckets
            // for L1, only check buckets not covered by L2
            let mut from_protections_ts_micros = 0;
            for level in (1..=2).rev() {
                let mut result_count = usize::MAX;
                while result_count > 0 {
                    let results = self
                        .dbp
                        .integrity_protection_facade()
                        .integrity_batch_in_interval_by_level_and_time(
                            topic_id,
                            level,
                            from_protections_ts_micros,
                            1024,
                        )
                        .await;
                    result_count = results.len();
                    for (protection_id, protection_ts_micros, protection_data, protection_ref) in
                        results
                    {
                        from_protections_ts_micros = protection_ts_micros;
                        if protection_ref.is_some() {
                            // Skip this, since it apparently had another layer of protection
                            continue;
                        }
                        // Be crash tolerant and don't make assumptions here based on when something was protected
                        if let Ok(ip) = IntegrityProtection::from_string(&protection_data) {
                            if ip.get_protected_hash().to_hex().eq(&protection_id) {
                                // Validate protection_data using current secret
                                if ip
                                    .validate_current(
                                        self.ish.get_current_oid(),
                                        self.ish.get_current_secret(),
                                    )
                                    .is_err()
                                {
                                    //  if validation fails, check with previous secret
                                    if ip
                                        .validate_current(
                                            self.ish.get_previous_oid(),
                                            self.ish.get_previous_secret(),
                                        )
                                        .is_ok()
                                    {
                                        // Reprotect with current secret (only)
                                        let previous_oid = [];
                                        let previous_secret = [];
                                        let new_protection_data = IntegrityProtection::protect(
                                            ip.get_protected_hash(),
                                            self.ish.get_current_oid(),
                                            self.ish.get_current_secret(),
                                            &previous_oid,
                                            &previous_secret,
                                        )
                                        .unwrap()
                                        .as_string();
                                        // Persist
                                        // Keep original protection_ts_micros since it used for bucketing during consolidation.
                                        self.dbp
                                            .integrity_protection_facade()
                                            .integrity_protection_persist(
                                                topic_id,
                                                &protection_id,
                                                &new_protection_data,
                                                protection_ts_micros,
                                                level,
                                            )
                                            .await;
                                        update_count += 1;
                                    } else {
                                        // Err: Not able to verify
                                        log::error!(
                                            "Unable to verify event using current or previous secret. level: {level}, protection_id: '{protection_id}', protection_ts_micros: {protection_ts_micros}"
                                        )
                                    }
                                }
                            } else {
                                // Err
                                log::error!(
                                    "Found protection data is for another protection_id. level: {level}, protection_id: '{protection_id}', protection_ts_micros: {protection_ts_micros}"
                                )
                            }
                        } else {
                            log::error!(
                                "Unable to parse protection data. level: {level}, protection_id: '{protection_id}', protection_ts_micros: {protection_ts_micros}"
                            )
                        }
                    }
                    from_protections_ts_micros += 1;
                }
            }
        }
        log::info!("Updated {update_count} integrity protection with current secret.")
    }

    async fn run_consolidation_for_topic(&self, topic_id: &str) {
        // This grabs stuff in at least 4 min intervals.. → no microsec race condition..
        for level_in in 0..=1 {
            // Grab latest protection of `level_out` by doing lookup on `topic.integrity_blat_lookup` and `topic.integrity_by_level_and_time`.
            if let Some(lookup_ts_bucket) = self
                .dbp
                .integrity_protection_facade()
                .integrity_protection_next_starting_point_to_process(
                    topic_id,
                    level_in,
                    fragtale_client::time::get_timestamp_micros(),
                )
                .await
            {
                if log::log_enabled!(log::Level::Trace) {
                    log::trace!(
                        "There is consolidating to be done in topic {topic_id} bucket {lookup_ts_bucket} at level {level_in}."
                    );
                }
                // Batch grab all (protection_id, protections_ts_micros) into mem
                //  (IntegrityByLevelAndTimeEntity is ordered by `protections_ts_micros`)
                let mut members: Vec<(String, u64)> = vec![];
                let mut from_protections_ts_micros = lookup_ts_bucket;
                let mut result_count = usize::MAX;
                while result_count > 0 {
                    let results = self
                        .dbp
                        .integrity_protection_facade()
                        .integrity_batch_in_interval_by_level_and_time(
                            topic_id,
                            level_in,
                            from_protections_ts_micros,
                            1024,
                        )
                        .await;
                    result_count = results.len();
                    if log::log_enabled!(log::Level::Trace) {
                        log::trace!(
                            "Found {result_count} result in topic_id: {topic_id}, level_in: {level_in}, lookup_ts_bucket: {lookup_ts_bucket}, from_protections_ts_micros: {from_protections_ts_micros}."
                        );
                    }
                    for (protection_id, protection_ts_micros, protection_data, protection_ref) in
                        results
                    {
                        if protection_ref.is_some() {
                            log::warn!(
                                "Trying to consolidate something that already has a integrirty protection reference. (Unexpected)"
                            );
                            // Skip inclusion
                            continue;
                        }
                        // Validate protection before including it
                        if let Ok(ip) =
                            IntegrityProtection::from_string(&protection_data).map_err(|e| {
                                log::warn!("IntegrityProtection::from_string: {e}");
                            })
                        {
                            if self
                                .validator
                                .is_valid_integrity_protection(
                                    protection_ts_micros,
                                    &tyst::encdec::hex::decode(&protection_id).unwrap(),
                                    &ip,
                                )
                                .await
                            {
                                members.push((protection_id, protection_ts_micros));
                            } else {
                                // Skip inclusion
                                log::warn!(
                                    "Validation failed for topic_id: {topic_id}, level_in: {level_in}, lookup_ts_bucket: {lookup_ts_bucket}, from_protections_ts_micros: {from_protections_ts_micros}."
                                );
                            }
                        }
                        from_protections_ts_micros = protection_ts_micros;
                    }
                    from_protections_ts_micros += 1;
                }
                if log::log_enabled!(log::Level::Trace) {
                    log::trace!(
                        "Level {level_in} in '{topic_id}' has {} members in bucket {lookup_ts_bucket}.",
                        members.len()
                    );
                }
                // Build BDT using IntegrityProtector
                let level_out = level_in + 1;
                self.build_bdt(topic_id, level_out, &members).await
            } else if log::log_enabled!(log::Level::Trace) {
                log::trace!(
                    "There is no consolidating to be done in topic '{topic_id}' at level {level_in}."
                );
            }
        }
    }

    /// Build BDT using IntegrityProtector
    async fn build_bdt(&self, topic_id: &str, level_out: u8, members: &[(String, u64)]) {
        let members_bytes = members
            .iter()
            .filter_map(|(protection_id, protection_ts_micros)| {
                tyst::encdec::hex::decode(protection_id)
                    .map_err(|e| {
                        log::warn!(
                            "Excluding protection_id '{protection_id}' that is not valid hex: {e}"
                        )
                    })
                    //.map(|protection_id_bytes|protection_id_bytes.as_slice())
                    .ok()
                    .map(|protection_id_bytes| (protection_id_bytes, *protection_ts_micros))
            })
            .collect::<Vec<_>>();
        self.protector.binary_digest_tree_as_proof_stream(
                &members_bytes
                )
                .enumerate()
                .for_each(|(i,future_ipr)|{
                    async move {
                        let (member, member_protection_ts_micros, ipr) = future_ipr.await;
                        if i == 0 {
                            // First, we should persist the protection of the BDT root hash
                            if let Ok((protection_ts_micros, root_hash)) = ipr.get_integrity_protection_reference(&member)
                                .map_err(|e|{
                                    log::warn!("Failed to validate integrity_protection_reference: {e}")
                                })
                                 {
                                    // Persist root hash protection in `integrity` and lookup helpers (at level_out)
                                    self.protector.create_and_persist_integrity_protection(topic_id, &root_hash, protection_ts_micros, level_out).await;
                                    if log::log_enabled!(log::Level::Debug) {
                                        log::debug!("Consolidated protection at level {level_out}. Protected root hash is '{}' (hex).", root_hash.to_hex());
                                    }
                                }
                        }
                        // Second, Update `topic.integrity.protection` to point to next level for each such integrity
                        let protection_ref = ipr.as_string();
                        let member_protection_id = member.to_hex();
                        self.dbp.integrity_protection_facade().integrity_protection_set_protection_ref(topic_id, &member_protection_id, member_protection_ts_micros, &protection_ref).await;
                    }
                })
                .await;
    }
}
