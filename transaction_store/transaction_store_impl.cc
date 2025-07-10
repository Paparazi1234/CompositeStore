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

class MVCCTxnStoreFactory : public TxnStoreFactory {
 public:
  ~MVCCTxnStoreFactory() {}

  TransactionStore* CreateTxnStore(
      const StoreOptions& store_options,
      const TransactionStoreOptions& txn_store_options,
      const StoreTraits& store_traits) const override {
    MVCCTxnStore* txn_store = nullptr;
    EmptyTxnLockManagerFactory txn_lock_mgr_factory;
    switch (store_traits.txn_write_policy) {
      case WRITE_COMMITTED:
        txn_store =
            new WriteCommittedTxnStore(store_options,
                                       txn_store_options,
                                       txn_lock_mgr_factory,
                                       new WriteCommittedTransactionFactory());
        break;
      case WRITE_PREPARED:
        txn_store =
            new WritePreparedTxnStore(store_options,
                                      txn_store_options,
                                      txn_lock_mgr_factory,
                                      new WritePreparedTransactionFactory(),
                                      store_traits.commit_table_options);
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
  switch (store_traits.backed_type) {
    case kSkipListBacked:
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
