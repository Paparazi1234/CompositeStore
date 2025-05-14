#pragma once

#include "store.h"
#include "multi_versions.h"

namespace MULTI_VERSIONS_NAMESPACE {

class SkipListBackedInMemoryStore : public Store {
 public:
  // No copying allowed
  SkipListBackedInMemoryStore(const SkipListBackedInMemoryStore&) = delete;
  SkipListBackedInMemoryStore& operator=(
      const SkipListBackedInMemoryStore&) = delete;

  SkipListBackedInMemoryStore();
  ~SkipListBackedInMemoryStore() {}

  Status Put(const WriteOptions& write_options,
             const std::string& key, const std::string& value) override;
  Status Delete(const WriteOptions& write_options,
                const std::string& key) override;
  Status Get(const ReadOptions& read_options,
             const std::string& key, std::string* value) override;

 private:

};

}   // namespace MULTI_VERSIONS_NAMESPACE
