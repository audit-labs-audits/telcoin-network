use tn_tracing::init_test_tracing;
mod common;
use common::spawn_test_payload_service;

#[tokio::test(flavor = "multi_thread")]
async fn test_batch_payload_job_genesis() {
    init_test_tracing();
    // TODO: service uses mock transactions which do not work in the EVM
    let service_handle = spawn_test_payload_service().await;
    let batch = service_handle.new_batch().await;
    let batch2 = service_handle.new_batch().await;
    // empty batches result in error
    assert!(batch.is_err());
    assert!(batch2.is_err());
}
