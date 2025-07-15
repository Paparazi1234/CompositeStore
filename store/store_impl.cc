#include "mvcc_store/mvcc_store.h"

namespace MULTI_VERSIONS_NAMESPACE {

class StoreFactory {
 public:
  virtual ~StoreFactory() {}

  virtual Store* CreateStore(const StoreOptions& store_options) const = 0;
};

class MVCCStoreFactory : public StoreFactory {
 public:
  ~MVCCStoreFactory() {}

  Store* CreateStore(const StoreOptions& store_options) const override {
    return new MVCCStore(store_options);
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

}   // namespace MULTI_VERSIONS_NAMESPACE
