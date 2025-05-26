#pragma once

#include "include/multi_versions.h"
#include "options.h"
#include "store_traits.h"

namespace MULTI_VERSIONS_NAMESPACE {

class Store {
 public:
  static Status Open(const StoreOptions& store_options,
                     const StoreTraits& store_traits,
                     Store** store_ptr);
  // No copying allowed
  Store(const Store&) = delete;
  Store& operator=(const Store&) = delete;

  Store() {}
  virtual ~Store() {}

  virtual Status Put(const WriteOptions& write_options,
                     const std::string& key, const std::string& value) = 0;
  virtual Status Delete(const WriteOptions& write_options,
                        const std::string& key) = 0;
  virtual Status Get(const ReadOptions& read_options,
                     const std::string& key, std::string* value) = 0;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
