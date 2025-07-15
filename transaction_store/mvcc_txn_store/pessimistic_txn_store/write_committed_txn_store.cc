#include "write_committed_txn_store.h"

namespace MULTI_VERSIONS_NAMESPACE {

WriteCommittedTxnStore::WriteCommittedTxnStore(
    const StoreOptions& store_options,
    const TransactionStoreOptions& txn_store_options,
    const MultiVersionsManagerFactory& multi_versions_mgr_factory,
    const TxnLockManagerFactory& txn_lock_mgr_factory,
    TransactionFactory* txn_factory,
    StagingWriteFactory* staging_write_factory,
    const MVCCWriteBufferFactory& mvcc_write_buffer_factory)
        : PessimisticTxnStore(
              store_options,
              txn_store_options,
              multi_versions_mgr_factory,
              txn_lock_mgr_factory,
              txn_factory,
              staging_write_factory,
              mvcc_write_buffer_factory,
              this->CalcuPrepareQueue(store_options.enable_two_write_queues),
              this->CalcuCommitQueue(store_options.enable_two_write_queues)) {
  assert(std::addressof(commit_queue_) == &first_write_queue_);
  if (store_options.enable_two_write_queues) {
    assert(std::addressof(prepare_queue_) == &second_write_queue_);
  } else {
    assert(std::addressof(prepare_queue_) == &first_write_queue_);
  }
  if (!store_options.enable_two_write_queues) {
    assert(std::addressof(prepare_queue_) == std::addressof(commit_queue_));
  }
  CalcuCommitQueue(store_options.enable_two_write_queues);
}

}   // namespace MULTI_VERSIONS_NAMESPACE
