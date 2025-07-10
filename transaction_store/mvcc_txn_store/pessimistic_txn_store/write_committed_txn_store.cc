#include "write_committed_txn_store.h"

namespace MULTI_VERSIONS_NAMESPACE {

WriteCommittedTxnStore::WriteCommittedTxnStore(
    const StoreOptions& store_options,
    const TransactionStoreOptions& txn_store_options,
    const TxnLockManagerFactory& txn_lock_mgr_factory,
    TransactionFactory* txn_factory)
        : PessimisticTxnStore(
              store_options,
              txn_store_options,
              WriteCommittedMultiVersionsManagerFactory(
                  store_options.enable_two_write_queues),
              txn_lock_mgr_factory,
              txn_factory,
              CalcuPrepareQueue(store_options.enable_two_write_queues),
              CalcuCommitQueue(store_options.enable_two_write_queues)) {
  assert(std::addressof(commit_queue_) == &first_write_queue_);
  if (store_options.enable_two_write_queues) {
    assert(std::addressof(prepare_queue_) == &second_write_queue_);
  } else {
    assert(std::addressof(prepare_queue_) == &first_write_queue_);
  }
  if (!store_options.enable_two_write_queues) {
    assert(std::addressof(prepare_queue_) == std::addressof(commit_queue_));
  }
}

}   // namespace MULTI_VERSIONS_NAMESPACE
