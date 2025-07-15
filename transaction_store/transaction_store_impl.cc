#include "mvcc_txn_store/pessimistic_txn_store/write_committed_txn_store.h"
#include "mvcc_txn_store/pessimistic_txn_store/write_prepared_txn_store.h"

namespace MULTI_VERSIONS_NAMESPACE {

class TxnStoreFactory {
 public:
  virtual ~TxnStoreFactory() {}

  virtual TransactionStore* CreateTxnStore(
      const StoreOptions& store_options,
      const TransactionStoreOptions& txn_store_options,
      const StoreTraits& store_traits) const = 0;
};

struct TxnStoreParam{

};

class MVCCTxnStoreFactory : public TxnStoreFactory {
 public:
  ~MVCCTxnStoreFactory() {}

  TransactionStore* CreateTxnStore(
      const StoreOptions& store_options,
      const TransactionStoreOptions& txn_store_options,
      const StoreTraits& store_traits) const override {
    MVCCTxnStore* txn_store = nullptr;
    EmptyTxnLockManagerFactory txn_lock_mgr_factory;
    SkipListBackedMVCCWriteBufferFactory mvcc_write_buffer_factory;
    switch (store_traits.txn_store_write_policy) {
      case TxnStoreWritePolicy::kWriteCommitted:
        txn_store =
            new WriteCommittedTxnStore(
                store_options,
                txn_store_options,
                WriteCommittedMultiVersionsManagerFactory(
                    store_options.enable_two_write_queues),
                txn_lock_mgr_factory,
                new WriteCommittedTransactionFactory(),
                new OrderedMapBackedStagingWriteFactory(),
                mvcc_write_buffer_factory);
        break;
      case TxnStoreWritePolicy::kWritePrepared:
        txn_store =
            new WritePreparedTxnStore(
                store_options,
                txn_store_options,
                WritePreparedMultiVersionsManagerFactory(
                    store_traits.commit_table_options,
                    store_options.enable_two_write_queues),
                txn_lock_mgr_factory,
                new WritePreparedTransactionFactory(),
                new OrderedMapBackedStagingWriteFactory(),
                mvcc_write_buffer_factory);
        break;
      default:
        txn_store = nullptr;
    }
    return txn_store;
  }
};

Status TransactionStore::Open(const StoreOptions& store_options,
                              const TransactionStoreOptions& txn_store_options,
                              const StoreTraits& store_traits,
                              TransactionStore** txn_store_ptr) {
  assert(txn_store_ptr);
  *txn_store_ptr = nullptr;
  Status s;
  switch (store_traits.txn_store_impl_type) {
    case TxnStoreImplType::kDefault:
    case TxnStoreImplType::kMVCC:
      *txn_store_ptr = MVCCTxnStoreFactory().CreateTxnStore(store_options,
                                                            txn_store_options,
                                                            store_traits);
      break;
    default:
      *txn_store_ptr = nullptr;
  }
  
  if (*txn_store_ptr == nullptr) {
    s = Status::InvalidArgument();
  }
  return s;
}

}   // namespace MULTI_VERSIONS_NAMESPACE
