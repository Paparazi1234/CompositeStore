#include "pessimistic_txn_store.h"

namespace COMPOSITE_STORE_NAMESPACE {

PessimisticTxnStore::PessimisticTxnStore(
    const StoreOptions& store_options,
    const TransactionStoreOptions& txn_store_options,
    const MultiVersionsManagerFactory& multi_versions_mgr_factory,
    const TxnLockManagerFactory& txn_lock_mgr_factory,
    TransactionFactory* txn_factory,
    StagingWriteFactory* staging_write_factory,
    const MVCCWriteBufferFactory& mvcc_write_buffer_factory,
    WriteQueue& prepare_queue,
    WriteQueue& commit_queue)
      : MVCCTxnStore(store_options,
                     txn_store_options,
                     multi_versions_mgr_factory,
                     txn_lock_mgr_factory,
                     txn_factory,
                     staging_write_factory,
                     mvcc_write_buffer_factory),
        enable_two_write_queues_(store_options.enable_two_write_queues),
        prepare_queue_(prepare_queue),
        commit_queue_(commit_queue) {}

}   // namespace COMPOSITE_STORE_NAMESPACE
