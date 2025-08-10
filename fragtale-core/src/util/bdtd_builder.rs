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

//! [BinaryDigestTree] builder.

use crossbeam_skiplist::SkipMap;
use futures::lock::Mutex;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tyst::Tyst;
use tyst::misc::BinaryDigestTree;
use tyst::misc::BinaryDigestTreeProof;
use tyst::traits::digest::Digest;

/// Populated during [BinaryDigestTree] build.
struct MembershipStaging {
    pub created_ts_micros: u64,
    pub members: Vec<Vec<u8>>,
    pub semaphore: Arc<Semaphore>,
    pub bdt: Arc<SkipMap<(), Arc<BinaryDigestTree>>>,
}

impl Default for MembershipStaging {
    fn default() -> Self {
        Self {
            created_ts_micros: fragtale_client::time::get_timestamp_micros(),
            members: Vec::default(),
            semaphore: Arc::new(Semaphore::new(0)),
            bdt: Arc::default(),
        }
    }
}

/// The root hash of a [BinaryDigestTree].
///
/// All other members of the tree will wait for this object to go out of scope.
pub struct BinaryDigestTreeRoot {
    root_hash: Vec<u8>,
    semaphore: Arc<Semaphore>,
}

impl Drop for BinaryDigestTreeRoot {
    fn drop(&mut self) {
        // Release all other members of the group when `self` goes out of scope.
        self.semaphore.add_permits(Semaphore::MAX_PERMITS);
    }
}

impl BinaryDigestTreeRoot {
    fn new(root_hash: &[u8], semaphore: &Arc<Semaphore>) -> Self {
        Self {
            root_hash: root_hash.to_vec(),
            semaphore: Arc::clone(semaphore),
        }
    }

    /// Return the [BinaryDigestTree] root hash as a byte slice.
    pub fn as_bytes(&self) -> &[u8] {
        &self.root_hash
    }
}

/// Joins multiple callers into the same [BinaryDigestTree] when they join
/// within a common small time frame.
///
/// This is useful to group callers by time with a strict worst case delay.
pub struct BinaryDigestTreeGroupBuilder {
    digest_algorithm_oid: Vec<u32>,
    group_by_micros: u64,
    staging: Arc<Mutex<Option<MembershipStaging>>>,
}

impl Default for BinaryDigestTreeGroupBuilder {
    fn default() -> Self {
        // Default to SHA3-512 and 64 ms
        Self::new(tyst::oids::digest::SHA3_512, 64_000)
    }
}

impl BinaryDigestTreeGroupBuilder {
    /// Return a new instance with a specific digest algorithm and custom time
    /// frame to group calling members by.
    pub fn new(digest_algorithm_oid: &[u32], group_by_micros: u64) -> Self {
        if log::log_enabled!(log::Level::Trace) {
            log::trace!(
                "Instantiated new BinaryDigestTreeGroupBuilder: digest_algorithm_oid: {digest_algorithm_oid:?}, group_by_micros: {group_by_micros}"
            );
        }
        Self {
            digest_algorithm_oid: digest_algorithm_oid.to_vec(),
            group_by_micros,
            staging: Arc::new(Mutex::new(Option::None)),
        }
    }

    /// Return the proof that the member belongs to a tree and one of the
    /// callers in the group will also return [BinaryDigestTreeRoot].
    ///
    /// Members will be grouped into the [BinaryDigestTree] if they arrive
    /// within `group_by_micros` microseconds from the first arrival.
    ///
    /// The [BinaryDigestTreeRoot] can be used to process the tree's root hash
    /// exactly once.
    ///
    /// The returned `created_ts_micros` is not guaranteed to be unique across
    /// all instances.
    pub async fn get_proof_of_inclusion(
        &self,
        member: Vec<u8>,
    ) -> (Option<BinaryDigestTreeRoot>, BinaryDigestTreeProof, u64) {
        let mutex = Arc::clone(&self.staging);
        let (first, semaphore, bdt_holder, created_ts_micros) = {
            let mut staging_opt = mutex.lock().await;
            staging_opt.get_or_insert_with(MembershipStaging::default);
            let staging = staging_opt.as_mut().unwrap();
            staging.members.push(member.to_owned());
            (
                staging.members.len() == 1,
                Arc::clone(&staging.semaphore),
                Arc::clone(&staging.bdt),
                staging.created_ts_micros,
            )
        };
        if first {
            // wait for condition
            tokio::time::sleep(tokio::time::Duration::from_micros(self.group_by_micros)).await;
            let staging = {
                let mut staging = mutex.lock().await;
                staging.take().unwrap()
            };
            // build tree
            let members = staging
                .members
                .iter()
                .map(|leaf| leaf as &[u8])
                .collect::<Vec<_>>();
            let bdt = Arc::new(BinaryDigestTree::new(
                Self::get_digest(&self.digest_algorithm_oid),
                &members,
            ));
            staging.bdt.insert((), bdt);
            // release the other "threads" in the same group
            //staging.semaphore.add_permits(Semaphore::MAX_PERMITS);
        } else {
            let _res_permit = semaphore.acquire().await;
        }
        // return proof
        let bdt = bdt_holder
            .front()
            .as_ref()
            .map(|entry| Arc::clone(entry.value()))
            .unwrap();
        let mut digest = Self::get_digest(&self.digest_algorithm_oid);
        let bdtp = bdt.proof(digest.as_mut(), &member);
        if first {
            (
                Some(BinaryDigestTreeRoot::new(bdtp.get_root_hash(), &semaphore)),
                bdtp,
                created_ts_micros,
            )
        } else {
            (None, bdtp, created_ts_micros)
        }
    }

    fn get_digest(digest_algorithm_oid: &[u32]) -> Box<dyn Digest> {
        Tyst::instance()
            .digests()
            .by_oid(&tyst::encdec::oid::as_string(digest_algorithm_oid))
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_bdt_group_builder() {
        let bdtgb = Arc::new(BinaryDigestTreeGroupBuilder::default());
        let root_count = Arc::new(AtomicUsize::default());
        let mut tasks = vec![];
        for i in 0..255 {
            let member = vec![i; 16];
            let bdtgb = Arc::clone(&bdtgb);
            let root_count = Arc::clone(&root_count);
            tasks.push(tokio::spawn(async move {
                let (root_opt, proof, protection_ts_micros) =
                    bdtgb.get_proof_of_inclusion(member.to_owned()).await;
                let root_hash = BinaryDigestTree::root_hash_for_member(
                    BinaryDigestTreeGroupBuilder::get_digest(proof.get_digest_algorithm_oid())
                        .as_mut(),
                    &proof,
                    &member,
                )
                .expect("Member was not included in tree.");
                if let Some(root) = root_opt {
                    root_count.fetch_add(1, Ordering::Relaxed);
                    assert!(root_hash.eq(&root.as_bytes()));
                }
                protection_ts_micros
            }));
        }
        // All callers should see the same creation time of the tree
        let mut last = None;
        for task in tasks {
            let protection_ts_micros = task.await.unwrap();
            if let Some(last) = last {
                assert_eq!(protection_ts_micros, last);
            } else {
                last = Some(protection_ts_micros);
            }
        }
        assert_eq!(root_count.load(Ordering::Relaxed), 1);
    }
}
