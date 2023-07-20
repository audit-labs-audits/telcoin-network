// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::consensus::{
    worker::batch_serde::Token::NewtypeVariant, Batch, BatchV1, MetadataV1, VersionedMetadata,
};
use serde_test::{assert_tokens, Token};

// TODO: update test with new BatchV1 variant
#[test]
fn test_serde_batch() {
    let tx = || vec![1; 5];

    let batch = Batch::V1(BatchV1 {
        transactions: (0..2).map(|_| tx()).collect(),
        versioned_metadata: VersionedMetadata::V1(MetadataV1 {
            created_at: 1666205365890,
            ..Default::default()
        }),
    });

    assert_tokens(
        &batch,
        &[
            NewtypeVariant { name: "Batch", variant: "V1" },
            Token::Struct { name: "BatchV1", len: 2 },
            Token::Str("transactions"),
            Token::Seq { len: Some(2) },
            Token::Seq { len: Some(5) },
            Token::U8(1),
            Token::U8(1),
            Token::U8(1),
            Token::U8(1),
            Token::U8(1),
            Token::SeqEnd,
            Token::Seq { len: Some(5) },
            Token::U8(1),
            Token::U8(1),
            Token::U8(1),
            Token::U8(1),
            Token::U8(1),
            Token::SeqEnd,
            Token::SeqEnd,
            Token::Str("versioned_metadata"),
            Token::NewtypeVariant { name: "VersionedMetadata", variant: "V1" },
            Token::Str("created_at"),
            Token::U64(1666205365890),
            Token::StructEnd,
            Token::StructEnd,
        ],
    );
}
