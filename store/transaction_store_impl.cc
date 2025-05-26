#include "skiplist_backed/skiplist_backed_in_memory_txn_store.h"

namespace MULTI_VERSIONS_NAMESPACE {

class TransactionStoreFactory {
 public:
  virtual ~TransactionStoreFactory() {}

  virtual TransactionStore* CreateTransactionStore(
      const StoreOptions& store_options,
      const TransactionStoreOptions& txn_store_options,
      const StoreTraits& store_traits) const = 0;
};

class SkipListBackedInMemoryTxnStoreFactory : public TransactionStoreFactory {
 public:
  ~SkipListBackedInMemoryTxnStoreFactory() {}

  virtual TransactionStore* CreateTransactionStore(
      const StoreOptions& store_options,
      const TransactionStoreOptions& txn_store_options,
      const StoreTraits& store_traits) const override {
    SkipListBackedInMemoryStore* base_store = nullptr;
    switch (store_traits.txn_write_policy) {
      case WRITE_COMMITTED: {
          WCSeqBasedMultiVersionsManagerFactory wcmvm_factory;
          base_store =
              new SkipListBackedInMemoryStore(store_options, wcmvm_factory);
        }
        break;
      case WRITE_PREPARED: {
          WPSeqBasedMultiVersionsManagerFactory wpmvm_factory;
          base_store =
              new SkipListBackedInMemoryStore(store_options, wpmvm_factory);
        }
        break;
      default:
        base_store = nullptr;
    }

    if (!base_store) {
      return nullptr;
    }
    
    TransactionStore* txn_store = nullptr;
    switch (store_traits.txn_lock_manager_type) {
      case kEmptyTxnLoxkManager: {
          EmptyTxnLockManagerFactory tlm_factory;
          txn_store = new WriteCommittedTransactionStore(txn_store_options,
              base_store, tlm_factory);
        }
        break;
      default:
        txn_store = nullptr;
    }
    
    if (!txn_store) {
      delete base_store;
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
      *txn_store_ptr = 
          SkipListBackedInMemoryTxnStoreFactory().CreateTransactionStore(
              store_options, txn_store_options, store_traits);
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
