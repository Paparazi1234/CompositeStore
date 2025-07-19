#include "pessimistic_txn_store.h"

namespace COMPOSITE_STORE_NAMESPACE {

PessimisticTxnStore::PessimisticTxnStore(
    const StoreOptions& store_options,
    const TransactionStoreOptions& txn_store_options,
    const MVCCTxnStoreCreationParam& creation_param,
    WriteQueue& prepare_queue,
    WriteQueue& commit_queue)
      : MVCCTxnStore(store_options, txn_store_options, creation_param),
        enable_two_write_queues_(store_options.enable_two_write_queues),
        prepare_queue_(prepare_queue),
        commit_queue_(commit_queue) {}

}   // namespace COMPOSITE_STORE_NAMESPACE
