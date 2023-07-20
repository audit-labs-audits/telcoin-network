#![allow(unused)]
//! Test helper impls for generating bodies
use execution_db::{database::Database, tables, transaction::DbTxMut, DatabaseEnv};
use execution_interfaces::{db, p2p::bodies::response::BlockResponse};
use std::collections::HashMap;
use tn_types::execution::{Block, BlockBody, SealedBlock, SealedHeader, H256};

pub(crate) fn zip_blocks<'a>(
    headers: impl Iterator<Item = &'a SealedHeader>,
    bodies: &mut HashMap<H256, BlockBody>,
) -> Vec<BlockResponse> {
    headers
        .into_iter()
        .map(|header| {
            let body = bodies.remove(&header.hash()).expect("body exists");
            if header.is_empty() {
                BlockResponse::Empty(header.clone())
            } else {
                BlockResponse::Full(SealedBlock {
                    header: header.clone(),
                    body: body.transactions,
                    ommers: body.ommers,
                    withdrawals: body.withdrawals,
                })
            }
        })
        .collect()
}

pub(crate) fn create_raw_bodies<'a>(
    headers: impl Iterator<Item = &'a SealedHeader>,
    bodies: &mut HashMap<H256, BlockBody>,
) -> Vec<Block> {
    headers
        .into_iter()
        .map(|header| {
            let body = bodies.remove(&header.hash()).expect("body exists");
            body.create_block(header.as_ref().clone())
        })
        .collect()
}

#[inline]
pub(crate) fn insert_headers(db: &DatabaseEnv, headers: &[SealedHeader]) {
    db.update(|tx| -> Result<(), db::DatabaseError> {
        for header in headers {
            tx.put::<tables::CanonicalHeaders>(header.number, header.hash())?;
            tx.put::<tables::Headers>(header.number, header.clone().unseal())?;
        }
        Ok(())
    })
    .expect("failed to commit")
    .expect("failed to insert headers");
}
