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

//! Validation of event integrity.

use super::common::IntegritySecretsHolder;
use crate::conf::integrity_config::IntegrityConfig;
use crate::mb::integrity::IntegrityProtector;
use crate::mb::integrity::common::IntegrityProtection;
use crate::mb::integrity::common::IntegrityProtectionReference;
use crate::mb::unique_time_stamper::UniqueTimeStamper;
use crate::util::LocklessCachingFilter;
use fragtale_dbp::dbp::DatabaseProvider;
use fragtale_dbp::dbp::facades::DatabaseProviderFacades;
use fragtale_dbp::mb::UniqueTime;
use std::collections::HashSet;
use std::sync::Arc;
use tyst::encdec::hex::ToHex;

/** Validator of event integrity.

Protected data is grouped using [IntegrityProtectionReference] and then
protected using 1 or more shared secrets ([IntegrityProtection]).

Validation will ensure that the protected data belongs to the group and that
the group protection is valid.

To improve performance, valid group protections will be cached.
*/
pub struct IntegrityValidator {
    dbp: Arc<DatabaseProvider>,
    unique_timer_stamper: Arc<UniqueTimeStamper>,
    caching_filter: Arc<LocklessCachingFilter>,
    instance_start_ts: u64,
    allowed_digest_algorithm_oids: Vec<Vec<u32>>,
    ish: Arc<IntegritySecretsHolder>,
}

impl IntegrityValidator {
    /// Return a new instance.
    pub fn new(
        integrity_secrets_holder: &Arc<IntegritySecretsHolder>,
        dbp: &Arc<DatabaseProvider>,
        instance_start_ts: u64,
        unique_timer_stamper: &Arc<UniqueTimeStamper>,
    ) -> Arc<Self> {
        Arc::new(Self {
            dbp: Arc::clone(dbp),
            unique_timer_stamper: Arc::clone(unique_timer_stamper),
            caching_filter: LocklessCachingFilter::new(100),
            instance_start_ts,
            allowed_digest_algorithm_oids: Self::derive_allowed_digest_algos_from_protection(&[
                integrity_secrets_holder.get_current_oid(),
                integrity_secrets_holder.get_previous_oid(),
            ]),
            ish: Arc::clone(integrity_secrets_holder),
        })
    }

    // Derive digest algorithms from HMAC
    fn derive_allowed_digest_algos_from_protection(protection_oids: &[&[u32]]) -> Vec<Vec<u32>> {
        protection_oids
            .iter()
            .map(|poid| IntegrityConfig::derive_suitable_digest_algos_from_protection(poid))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
    }

    /// Validate the integrity of the event using configured shared secret(s).
    pub async fn validate_protection_ref_of_event(
        &self,
        topic_id: &str,
        document: &str,
        protection_ref: &str,
        unique_time: &UniqueTime,
    ) -> bool {
        let ipr = IntegrityProtectionReference::from_string(protection_ref);
        let digest_algorithm_oid =
            &tyst::encdec::oid::from_string(ipr.get_digest_algorith_oid()).unwrap();
        if !self
            .allowed_digest_algorithm_oids
            .contains(digest_algorithm_oid)
        {
            log::info!(
                "Digest algorithm '{}' used in proof is not allowed.",
                ipr.get_digest_algorith_oid()
            );
            return false;
        }
        let member =
            IntegrityProtector::hash_over_protected(digest_algorithm_oid, document, unique_time);
        let result = self
            .validate_protection_of_member(topic_id, ipr.clone(), &member)
            .await;
        if !result {
            log::info!(
                "Validation of event with unique_time: '{unique_time:?}' in '{topic_id}' failed."
            );
        }
        result
    }

    /// Validate the integrity of the member using configured shared secret(s).
    async fn validate_protection_of_member(
        &self,
        topic_id: &str,
        ipr: IntegrityProtectionReference,
        member: &[u8],
    ) -> bool {
        let mut member = member.to_vec();
        let mut ipr_opt = Some(ipr);
        while let Some(ipr) = ipr_opt {
            if let Ok((protection_ts_micros, root_hash)) =
                ipr.get_integrity_protection_reference(&member).map_err(|e|{
                    log::info!("Failed to get integrity protection reference (root hash) from BDT member (hash over protected): {e}");
                })
            {
                if self.caching_filter.contains(&root_hash) {
                    return true;
                }
                let protecion_id = root_hash.to_hex();
                if let Some((protection_data, protection_ref)) = self
                    .dbp
                    .integrity_protection_facade()
                    .integrity_protection_by_id_and_ts(
                        topic_id,
                        &protecion_id,
                        protection_ts_micros,
                    )
                    .await
                {
                    if let Some(protection_ref) = protection_ref {
                        //  Validate next level.
                        member = root_hash;
                        ipr_opt = Some(IntegrityProtectionReference::from_string(&protection_ref));
                        continue;
                    } else if let Ok(ip) = IntegrityProtection::from_string(&protection_data) {
                        if self
                            // No more levels. Validate here.
                            .is_valid_integrity_protection(protection_ts_micros, &root_hash, &ip)
                            .await
                        {
                            if log::log_enabled!(log::Level::Trace) {
                                log::trace!("Validation of event in '{topic_id}' was successful.");
                            }
                            self.caching_filter.insert(&root_hash).await;
                            return true;
                        } else {
                            log::info!("Validation of event in '{topic_id}' failed.");
                        }
                    }
                } else if log::log_enabled!(log::Level::Debug) {
                    log::debug!(
                        "Validation of event in '{topic_id}' had no IntegrityProtection. protection_ts_micros: {protection_ts_micros}, protection_id: {protecion_id}"
                    );
                }
            }
            break;
        }
        false
    }

    /// Validate [IntegrityProtection] using currently deployed secrets.
    pub async fn is_valid_integrity_protection(
        &self,
        protection_ts_micros: u64,
        root_hash: &[u8],
        ip: &IntegrityProtection,
    ) -> bool {
        let mut valid = false;
        if root_hash.eq(ip.get_protected_hash()) {
            // Is the IntegrityProtection newer than the current secret?
            if protection_ts_micros >= self.ish.get_current_ts_micros() {
                valid |= ip
                    .validate_current(self.ish.get_current_oid(), self.ish.get_current_secret())
                    .is_ok();
                if !valid && log::log_enabled!(log::Level::Debug) {
                    log::debug!("Validate current with current failed.");
                }
            }
            // Is the IntegrityProtection new enough to potentially have been protected by an upgraded instance?
            if !valid && protection_ts_micros >= self.instance_start_ts {
                valid |= ip
                    .validate_previous(self.ish.get_current_oid(), self.ish.get_current_secret())
                    .is_ok();
                if !valid && log::log_enabled!(log::Level::Debug) {
                    log::debug!("Validate previous with current failed.");
                }
            }
            // Was the IntegrityProtection created before the oldest alive node has started?
            if !valid
                && protection_ts_micros
                    <= self
                        .unique_timer_stamper
                        .get_oldest_first_claim_ts_micros()
                        .await
            {
                valid |= ip
                    .validate_previous(self.ish.get_previous_oid(), self.ish.get_previous_secret())
                    .is_ok();
                if !valid && log::log_enabled!(log::Level::Debug) {
                    log::debug!("Validate previous with previous failed.");
                }
            }
        }
        valid
    }
}
