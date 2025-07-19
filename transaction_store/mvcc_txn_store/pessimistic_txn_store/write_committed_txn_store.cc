#include "write_committed_txn_store.h"

namespace COMPOSITE_STORE_NAMESPACE {

WriteCommittedTxnStore::WriteCommittedTxnStore(
    const StoreOptions& store_options,
    const TransactionStoreOptions& txn_store_options,
    const MVCCTxnStoreCreationParam& creation_param)
        : PessimisticTxnStore(
              store_options,
              txn_store_options,
              creation_param,
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

}   // namespace COMPOSITE_STORE_NAMESPACE
