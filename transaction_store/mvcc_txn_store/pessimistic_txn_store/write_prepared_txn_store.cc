#include "write_prepared_txn_store.h"

namespace MULTI_VERSIONS_NAMESPACE {

WritePreparedTxnStore::WritePreparedTxnStore(
    const StoreOptions& store_options,
    const TransactionStoreOptions& txn_store_options,
    const TxnLockManagerFactory& txn_lock_mgr_factory,
    TransactionFactory* txn_factory,
    const CommitTableOptions& commit_table_options)
        : PessimisticTxnStore(
              store_options,
              txn_store_options,
              WritePreparedMultiVersionsManagerFactory(
                  commit_table_options, store_options.enable_two_write_queues),
              txn_lock_mgr_factory,
              txn_factory,
              CalcuPrepareQueue(store_options.enable_two_write_queues),
              CalcuCommitQueue(store_options.enable_two_write_queues)) {
  assert(std::addressof(prepare_queue_) == &first_write_queue_);
  if (store_options.enable_two_write_queues) {
    assert(std::addressof(commit_queue_) == &second_write_queue_);
  } else {
    assert(std::addressof(commit_queue_) == &first_write_queue_);
  }
  if (!store_options.enable_two_write_queues) {
    assert(std::addressof(prepare_queue_) == std::addressof(commit_queue_));
  }

  PostInitializeMultiVersionManager();
}

}   // namespace MULTI_VERSIONS_NAMESPACE
