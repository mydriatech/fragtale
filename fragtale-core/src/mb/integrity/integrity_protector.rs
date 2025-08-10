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

//! Protection of event integrity.

use super::common::IntegrityProtection;
use super::common::IntegrityProtectionReference;
use super::common::IntegritySecretsHolder;
use crate::conf::integrity_config::IntegrityConfig;
use crate::mb::unique_time_stamper::UniqueTimeStamper;
use crate::util::BinaryDigestTreeGroupBuilder;
use crossbeam_skiplist::SkipMap;
use fragtale_dbp::dbp::DatabaseProvider;
use fragtale_dbp::dbp::facades::DatabaseProviderFacades;
use fragtale_dbp::mb::UniqueTime;
use std::sync::Arc;
use tyst::Tyst;
use tyst::encdec::hex::ToHex;
use tyst::misc::BinaryDigestTree;

/** Protector of event integrity.

Protected data is grouped using [IntegrityProtectionReference] and then
protected using 1 or more shared secrets ([IntegritySecretsHolder]).

Protection will ensure that the protected data is grouped and that the group
protection is applied.

Events are later protected by layers of binary digest trees to allow performant
scaling (consolidation).
*/
pub struct IntegrityProtector {
    dbp: Arc<DatabaseProvider>,
    unique_timer_stamper: Arc<UniqueTimeStamper>,
    digest_algorithm_oid: Vec<u32>,
    topic_to_protection: SkipMap<String, Arc<BinaryDigestTreeGroupBuilder>>,
    ish: Arc<IntegritySecretsHolder>,
}

impl IntegrityProtector {
    /// Return a new instance.
    pub fn new(
        integrity_secrets_holder: &Arc<IntegritySecretsHolder>,
        dbp: &Arc<DatabaseProvider>,
        unique_timer_stamper: &Arc<UniqueTimeStamper>,
    ) -> Arc<Self> {
        Arc::new(Self {
            dbp: Arc::clone(dbp),
            unique_timer_stamper: Arc::clone(unique_timer_stamper),
            digest_algorithm_oid: IntegrityConfig::derive_suitable_digest_algos_from_protection(
                integrity_secrets_holder.get_current_oid(),
            ),
            topic_to_protection: SkipMap::new(),
            ish: Arc::clone(integrity_secrets_holder),
        })
    }

    /// Create protected hash from event data
    pub fn hash_over_protected(
        digest_algorithm_oid: &[u32],
        document: &str,
        unique_time: &UniqueTime,
    ) -> Vec<u8> {
        Tyst::instance()
            .digests()
            .by_oid(&tyst::encdec::oid::as_string(digest_algorithm_oid))
            .unwrap()
            .hash_many(&[document.as_bytes(), &unique_time.as_bytes()])
    }

    /// Return the protection on the event level.
    ///
    /// The caller is responsible for adding this to the Event row, while this
    /// function takes care of persisting higher levels of protection elsewhere.
    pub async fn derive_protection(
        &self,
        topic_id: &str,
        document: &str,
        unique_time: &UniqueTime,
    ) -> IntegrityProtectionReference {
        let hash_over_protected =
            Self::hash_over_protected(&self.digest_algorithm_oid, document, unique_time);
        let bdtgb = self.get_binary_digest_tree_group_builder(topic_id);
        let (proof, protection_ts_micros) = {
            let (bdt_root_opt, proof, created_ts_micros) =
                bdtgb.get_proof_of_inclusion(hash_over_protected).await;
            if let Some(bdt_root) = bdt_root_opt {
                // Designated committer
                let protected_hash = bdt_root.as_bytes().to_owned();
                self.create_and_persist_integrity_protection(
                    topic_id,
                    &protected_hash,
                    created_ts_micros,
                    0,
                )
                .await;
            }
            (proof, created_ts_micros)
            // ensure bdt_root_opt goes out of scope to release others waiting for semaphore
        };
        IntegrityProtectionReference::new(
            &tyst::encdec::oid::as_string(&self.digest_algorithm_oid),
            proof,
            protection_ts_micros,
        )
    }

    /// Protect the `protected_hash` and perist the result.
    pub async fn create_and_persist_integrity_protection(
        &self,
        topic_id: &str,
        protected_hash: &[u8],
        protection_ts_micros: u64,
        level: u8,
    ) {
        //let current_secret = &self.current_secret as &[u8];
        let mut previous_secret = self.ish.get_previous_secret();
        if self.ish.get_current_ts_micros()
            > self
                .unique_timer_stamper
                .get_oldest_first_claim_ts_micros()
                .await
        {
            // No instance will ever look at the protection by the previous secret anymore
            previous_secret = &[];
        }
        let protection_data = IntegrityProtection::protect(
            protected_hash,
            self.ish.get_current_oid(),
            self.ish.get_current_secret(),
            self.ish.get_previous_oid(),
            previous_secret,
        )
        .unwrap()
        .as_string();
        let protection_id = protected_hash.to_hex();
        // Write integrity protection
        self.dbp
            .integrity_protection_facade()
            .integrity_protection_persist(
                topic_id,
                &protection_id,
                &protection_data,
                protection_ts_micros,
                level,
            )
            .await;
    }

    fn get_binary_digest_tree_group_builder(
        &self,
        topic_id: &str,
    ) -> Arc<BinaryDigestTreeGroupBuilder> {
        Arc::<BinaryDigestTreeGroupBuilder>::clone(
            self.topic_to_protection
                .get_or_insert_with(topic_id.to_owned(), || {
                    Arc::new(BinaryDigestTreeGroupBuilder::new(
                        &self.digest_algorithm_oid,
                        64_000,
                    ))
                })
                .value(),
        )
    }

    /// Create a common [BinaryDigestTree] with all the provided `members` and
    /// return a Stream of proofs that each member belongs to the tree.
    ///
    /// Using a Stream here keeps the memory allocation down compared to
    /// creating an array of all proofs at once.
    pub fn binary_digest_tree_as_proof_stream(
        self: &Arc<Self>,
        members: &[(Vec<u8>, u64)],
    ) -> impl futures::stream::Stream<
        Item = impl Future<Output = (Vec<u8>, u64, IntegrityProtectionReference)>,
    > {
        let members_bytes = members
            .iter()
            .map(|leaf| &leaf.0 as &[u8])
            .collect::<Vec<_>>();
        let self_clone = Arc::clone(self);
        let digest = Tyst::instance()
            .digests()
            .by_oid(&tyst::encdec::oid::as_string(&self.digest_algorithm_oid))
            .unwrap();
        let bdt = Arc::new(BinaryDigestTree::new(digest, &members_bytes));
        let protection_ts_micros = fragtale_client::time::get_timestamp_micros();
        futures::stream::iter(
            members
                .iter()
                .map(move |(member, member_protection_ts_micros)| {
                    let bdt = Arc::clone(&bdt);
                    let member = member.to_owned();
                    let digest_algorithm_oid = self_clone.digest_algorithm_oid.to_owned();
                    async move {
                        let mut digest = Tyst::instance()
                            .digests()
                            .by_oid(&tyst::encdec::oid::as_string(&digest_algorithm_oid))
                            .unwrap();
                        let bdtp = bdt.proof(digest.as_mut(), &member);
                        (
                            member,
                            *member_protection_ts_micros,
                            IntegrityProtectionReference::new(
                                &tyst::encdec::oid::as_string(&digest_algorithm_oid),
                                bdtp,
                                protection_ts_micros,
                            ),
                        )
                    }
                }),
        )
    }
}
