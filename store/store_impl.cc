#include "skiplist_backed/skiplist_backed_in_memory_store.h"

namespace MULTI_VERSIONS_NAMESPACE {

class StoreFactory {
 public:
  virtual ~StoreFactory() {}

  virtual Store* CreateStore(const StoreOptions& store_options) const = 0;
};

class SkipListBackedInMemoryStoreFactory : public StoreFactory {
 public:
  ~SkipListBackedInMemoryStoreFactory() {}

  virtual Store* CreateStore(const StoreOptions& store_options) const override {
    EmptyMultiVersionsManagerFactory factory;
    return new SkipListBackedInMemoryStore(store_options, factory);
  }
};

Status Store::Open(const StoreOptions& store_options,
                   const StoreTraits& store_traits,
                   Store** store_ptr) {
  assert(store_ptr);
  *store_ptr = nullptr;
  Status s;
  switch (store_traits.backed_type) {
    case kSkipListBacked :
      *store_ptr =
          SkipListBackedInMemoryStoreFactory().CreateStore(store_options);
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
