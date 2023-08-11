// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{NodeStorage, StoreResult};
use lattice_typed_store::{
    reopen,
    rocks::{open_cf, DBMap, MetricConf, ReadWriteOptions},
    Map, TypedStoreError,
};
use std::collections::HashMap;
use tn_types::consensus::{
    AuthorityIdentifier, CommittedSubDag, ConsensusCommit,
    ConsensusCommitV1, Round, SequenceNumber,
};

/// The persistent storage of the sequencer.
pub struct ConsensusStore {
    /// The latest committed round of each validator.
    last_committed: DBMap<AuthorityIdentifier, Round>,

    /// The global consensus sequence
    committed_sub_dags_by_index: DBMap<SequenceNumber, ConsensusCommit>,
}

impl ConsensusStore {
    /// Create a new consensus store structure by using already loaded maps.
    pub fn new(
        last_committed: DBMap<AuthorityIdentifier, Round>,
        committed_sub_dags_map: DBMap<SequenceNumber, ConsensusCommit>,
    ) -> Self {
        Self {
            last_committed,
            // committed_sub_dags_by_index: sequence,
            committed_sub_dags_by_index: committed_sub_dags_map,
        }
    }

    pub fn new_for_tests() -> Self {
        let rocksdb = open_cf(
            tempfile::tempdir().unwrap(),
            None,
            MetricConf::default(),
            &[
                NodeStorage::LAST_COMMITTED_CF,
                NodeStorage::SUB_DAG_INDEX_CF,
                NodeStorage::COMMITTED_SUB_DAG_INDEX_CF,
            ],
        )
        .expect("Cannot open database");

        let (last_committed_map, committed_sub_dag_map) = reopen!(&rocksdb,
            NodeStorage::LAST_COMMITTED_CF;<AuthorityIdentifier, Round>,
            NodeStorage::SUB_DAG_INDEX_CF;<SequenceNumber, ConsensusCommit>
        );
        Self::new(last_committed_map, committed_sub_dag_map)
    }

    /// Clear the store.
    pub fn clear(&self) -> StoreResult<()> {
        self.last_committed.clear()?;
        self.committed_sub_dags_by_index.clear()?;
        Ok(())
    }

    /// Persist the consensus state.
    pub fn write_consensus_state(
        &self,
        last_committed: &HashMap<AuthorityIdentifier, Round>,
        sub_dag: &CommittedSubDag,
    ) -> Result<(), TypedStoreError> {
        let commit = ConsensusCommit::V1(ConsensusCommitV1::from_sub_dag(sub_dag));

        let mut write_batch = self.last_committed.batch();
        write_batch.insert_batch(&self.last_committed, last_committed.iter())?;
        write_batch.insert_batch(
            &self.committed_sub_dags_by_index,
            std::iter::once((sub_dag.sub_dag_index, commit)),
        )?;
        write_batch.write()
    }

    /// Load the last committed round of each validator.
    pub fn read_last_committed(&self) -> HashMap<AuthorityIdentifier, Round> {
        self.last_committed.unbounded_iter().collect()
    }

    /// Gets the latest sub dag index from the store
    pub fn get_latest_sub_dag_index(&self) -> SequenceNumber {
        self
            .committed_sub_dags_by_index
            .unbounded_iter()
            .skip_to_last()
            .next()
            .map(|(seq, _)| seq)
            .unwrap_or_default()
    }

    /// Returns thet latest subdag committed. If none is committed yet, then
    /// None is returned instead.
    pub fn get_latest_sub_dag(&self) -> Option<ConsensusCommit> {
        self
            .committed_sub_dags_by_index
            .unbounded_iter()
            .skip_to_last()
            .next()
            .map(|(_, sub_dag)| sub_dag)
    }

    /// Load all the sub dags committed with sequence number of at least `from`.
    pub fn read_committed_sub_dags_from(
        &self,
        from: &SequenceNumber,
    ) -> StoreResult<Vec<ConsensusCommit>> {
        let sub_dags = self.committed_sub_dags_by_index
            .unbounded_iter()
            .skip_to(from)?
            .map(|(_, sub_dag)| sub_dag)
            .collect::<Vec<ConsensusCommit>>();

        Ok(sub_dags)
    }
}

#[cfg(test)]
mod test {
    use crate::ConsensusStore;
    use lattice_typed_store::Map;
    use tn_types::consensus::{
        ConsensusCommit, ConsensusCommitV1, TimestampMs,
    };

    #[tokio::test]
    async fn test_sub_dags() {
        let store = ConsensusStore::new_for_tests();

        // Create few sub dags of V1 and write in the committed_sub_dags_by_index storage
        for i in 0..6 {
            let s = ConsensusCommitV1 {
                certificates: vec![],
                leader: Default::default(),
                leader_round: 2,
                sub_dag_index: i,
                reputation_score: Default::default(),
                commit_timestamp: i,
            };

            store
                .committed_sub_dags_by_index
                .insert(&s.sub_dag_index.clone(), &ConsensusCommit::V1(s))
                .unwrap();
        }

        // Read from index 0, all the sub dags should be returned
        let sub_dags = store.read_committed_sub_dags_from(&0).unwrap();

        assert_eq!(sub_dags.len(), 6);

        for (index, sub_dag) in sub_dags.iter().enumerate() {
            assert_eq!(sub_dag.sub_dag_index(), index as u64);
            assert_eq!(sub_dag.commit_timestamp(), index as TimestampMs);
        }

        // Read the last sub dag, and the sub dag with index 5 should be returned
        let last_sub_dag = store.get_latest_sub_dag();
        assert_eq!(last_sub_dag.unwrap().sub_dag_index(), 5);

        // Read the last sub dag index
        let index = store.get_latest_sub_dag_index();
        assert_eq!(index, 5);
    }
}
