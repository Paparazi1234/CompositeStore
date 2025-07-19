#include "mvcc_store/mvcc_store.h"

namespace COMPOSITE_STORE_NAMESPACE {

class StoreFactory {
 public:
  virtual ~StoreFactory() {}

  virtual Store* CreateStore(const StoreOptions& store_options) const = 0;
};

namespace {
// just a wrapper of WriteCommittedMultiVersionsManagerFactory and
// enable_two_write_queues == false
class EmptyMultiVersionsManagerFactory :
    public WriteCommittedMultiVersionsManagerFactory {
 public:
  EmptyMultiVersionsManagerFactory() {}
  ~EmptyMultiVersionsManagerFactory() {}
};
}   // anonymous namespace


class MVCCStoreFactory : public StoreFactory {
 public:
  ~MVCCStoreFactory() {}

  Store* CreateStore(const StoreOptions& store_options) const override {
    EmptyMultiVersionsManagerFactory empty_mvm_factory;
    EmptyTxnLockManagerFactory empty_txn_lock_mgr_factory;
    SkipListBackedMVCCWriteBufferFactory skiplist_backed_write_buffer_factory;
    MVCCTxnStoreCreationParam param;
    param.mvm_factory = &empty_mvm_factory;
    param.lock_manager_factory = &empty_txn_lock_mgr_factory;
    param.write_buffer_factory = &skiplist_backed_write_buffer_factory;
    param.transaction_factory = nullptr;
    param.staging_write_factory = new OrderedMapBackedStagingWriteFactory();
    return new MVCCStore(store_options, param);
  }
};

Status Store::Open(const StoreOptions& store_options,
                   const StoreTraits& store_traits,
                   Store** store_ptr) {
  assert(store_ptr);
  *store_ptr = nullptr;
  Status s;
  switch (store_traits.txn_store_impl_type) {
    case TxnStoreImplType::kDefault:
    case TxnStoreImplType::kMVCC:
      *store_ptr = MVCCStoreFactory().CreateStore(store_options);
      break;
    default:
      *store_ptr = nullptr;
  }

  if (*store_ptr == nullptr) {
    s = Status::InvalidArgument();
  }
  return s;
}

}   // namespace COMPOSITE_STORE_NAMESPACE
