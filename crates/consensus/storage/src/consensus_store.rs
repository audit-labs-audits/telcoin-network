// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::StoreResult;
use narwhal_typed_store::{mem_db::MemDB, traits::multi_insert, Map};
use std::{collections::HashMap, sync::Arc};
use tn_types::{
    AuthorityIdentifier, CommittedSubDag, ConsensusCommit, ConsensusCommitV1, Round, SequenceNumber,
};
use tracing::debug;

/// The persistent storage of the sequencer.
pub struct ConsensusStore {
    /// The latest committed round of each validator.
    last_committed: Arc<dyn Map<AuthorityIdentifier, Round>>,
    /// The global consensus sequence
    committed_sub_dags_by_index_v1: Arc<dyn Map<SequenceNumber, ConsensusCommit>>,
}

impl ConsensusStore {
    /// Create a new consensus store structure by using already loaded maps.
    pub fn new(
        last_committed: Arc<dyn Map<AuthorityIdentifier, Round>>,
        committed_sub_dags_map: Arc<dyn Map<SequenceNumber, ConsensusCommit>>,
    ) -> Self {
        Self { last_committed, committed_sub_dags_by_index_v1: committed_sub_dags_map }
    }

    pub fn new_for_tests() -> Self {
        Self::new(Arc::new(MemDB::open()), Arc::new(MemDB::open()))
    }

    /// Clear the store.
    pub fn clear(&self) -> StoreResult<()> {
        self.last_committed.clear()?;
        self.committed_sub_dags_by_index_v1.clear()?;
        Ok(())
    }

    /// Persist the consensus state.
    pub fn write_consensus_state(
        &self,
        last_committed: &HashMap<AuthorityIdentifier, Round>,
        sub_dag: &CommittedSubDag,
    ) -> eyre::Result<()> {
        // TODO- we lost atomicity here.  Can we even continue in the face of a storage failure?
        let commit = ConsensusCommit::V1(ConsensusCommitV1::from_sub_dag(sub_dag));

        multi_insert(&*self.last_committed, last_committed.iter())?;
        multi_insert(
            &*self.committed_sub_dags_by_index_v1,
            std::iter::once((sub_dag.sub_dag_index, commit)),
        )?;
        Ok(())
    }

    /// Load the last committed round of each validator.
    pub fn read_last_committed(&self) -> HashMap<AuthorityIdentifier, Round> {
        self.last_committed.iter().collect()
    }

    /// Gets the latest sub dag index from the store
    pub fn get_latest_sub_dag_index(&self) -> SequenceNumber {
        self.committed_sub_dags_by_index_v1.last_record().map(|(seq, _)| seq).unwrap_or_default()
    }

    /// Returns thet latest subdag committed. If none is committed yet, then
    /// None is returned instead.
    pub fn get_latest_sub_dag(&self) -> Option<ConsensusCommit> {
        self.committed_sub_dags_by_index_v1.last_record().map(|(_, sub_dag)| sub_dag)
    }

    /// Load all the sub dags committed with sequence number of at least `from`.
    pub fn read_committed_sub_dags_from(
        &self,
        from: &SequenceNumber,
    ) -> StoreResult<Vec<ConsensusCommit>> {
        Ok(self
            .committed_sub_dags_by_index_v1
            .skip_to(from)?
            .map(|(_, sub_dag)| sub_dag)
            .collect::<Vec<ConsensusCommit>>())
    }

    /// Load consensus commit with a given sequence number.
    pub fn read_consensus_commit(
        &self,
        seq: &SequenceNumber,
    ) -> StoreResult<Option<ConsensusCommit>> {
        self.committed_sub_dags_by_index_v1.get(seq)
    }

    /// Reads from storage the latest commit sub dag where its ReputationScores are marked as
    /// "final". If none exists yet then this method will return None.
    pub fn read_latest_commit_with_final_reputation_scores(&self) -> Option<ConsensusCommit> {
        for commit in self.committed_sub_dags_by_index_v1.reverse_iter().map(|(_, sub_dag)| sub_dag)
        {
            // found a final of schedule score, so we'll return that
            if commit.reputation_score().final_of_schedule {
                debug!(
                    "Found latest final reputation scores: {:?} from commit {:?}",
                    commit.reputation_score(),
                    commit.sub_dag_index()
                );
                return Some(commit);
            }
        }
        debug!("No final reputation scores have been found");
        None
    }
}

#[cfg(test)]
mod test {
    use crate::ConsensusStore;
    use std::collections::HashMap;
    use tn_types::{test_utils::CommitteeFixture, Certificate, CommittedSubDag, ReputationScores};

    #[tokio::test]
    async fn test_read_latest_final_reputation_scores() {
        // GIVEN
        let store = ConsensusStore::new_for_tests();
        let fixture = CommitteeFixture::builder().build();
        let committee = fixture.committee();

        // AND we add some commits without any final scores
        for sequence_number in 0..10 {
            let sub_dag = CommittedSubDag::new(
                vec![],
                Certificate::default(),
                sequence_number,
                ReputationScores::new(&committee),
                None,
            );

            store.write_consensus_state(&HashMap::new(), &sub_dag).unwrap();
        }

        // WHEN we try to read the final schedule. The one of sub dag sequence 12 should be returned
        let commit = store.read_latest_commit_with_final_reputation_scores();

        // THEN no commit is returned
        assert!(commit.is_none());

        // AND when adding more commits with some final scores amongst them
        for sequence_number in 10..=20 {
            let mut scores = ReputationScores::new(&committee);

            // we mark the sequence 14 & 20 committed sub dag as with final schedule
            if sequence_number == 14 || sequence_number == 20 {
                scores.final_of_schedule = true;
            }

            let sub_dag =
                CommittedSubDag::new(vec![], Certificate::default(), sequence_number, scores, None);

            store.write_consensus_state(&HashMap::new(), &sub_dag).unwrap();
        }

        // WHEN we try to read the final schedule. The one of sub dag sequence 20 should be returned
        let commit = store.read_latest_commit_with_final_reputation_scores().unwrap();

        assert!(commit.reputation_score().final_of_schedule);
        assert_eq!(commit.sub_dag_index(), 20)
    }
}
