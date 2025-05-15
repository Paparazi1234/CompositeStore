#pragma once

#include "./skiplist_backed/skliplist_backed_in_memory_store.h"
#include "store.h"

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
    return new SkipListBackedInMemoryStore();
  }
};

Status Store::Open(const StoreOptions& options, Store** store_ptr) {
  SkipListBackedInMemoryStoreFactory slbimsf;
  *store_ptr = slbimsf.CreateStore();
  return Status::OK();
}

}   // namespace MULTI_VERSIONS_NAMESPACE
