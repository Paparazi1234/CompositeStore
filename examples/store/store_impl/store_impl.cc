#include "skiplist_backed/skliplist_backed_in_memory_store.h"

namespace MULTI_VERSIONS_NAMESPACE {

class StoreFactory {
 public:
  virtual ~StoreFactory() {}

  virtual Store* CreateStore() = 0;
};

class SkipListBackedInMemoryStoreFactory : public StoreFactory {
 public:
  ~SkipListBackedInMemoryStoreFactory() {}

  virtual Store* CreateStore() override {
    EmptyMultiVersionsManagerFactory factory;
    return new SkipListBackedInMemoryStore(factory);
  }
};

Status Store::Open(const StoreOptions& options, Store** store_ptr) {
  assert(store_ptr);
  *store_ptr == nullptr;
  Status s;
  switch (options.store_backed_type) {
    case kTypeSkipList :
      if (!options.enable_txn_if_supported) {
        *store_ptr = SkipListBackedInMemoryStoreFactory().CreateStore();
        s = Status::OK();
      }
      break;
    default:
      s = Status::InvalidArgument();
  }
  return s;
}

}   // namespace MULTI_VERSIONS_NAMESPACE
