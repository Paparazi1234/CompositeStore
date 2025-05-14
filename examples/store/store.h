#pragma once

#include <string>

#include "multi_versions_namespace.h"
#include "status.h"

namespace MULTI_VERSIONS_NAMESPACE {

struct ReadOptions {

};

struct WriteOptions {

};

class Store {
 public:
  // No copying allowed
  Store(const Store&) = delete;
  Store& operator=(const Store&) = delete;

  virtual ~Store() {}

  virtual Status Put(const WriteOptions& write_options,
                     const std::string& key, const std::string& value) = 0;
  virtual Status Delete(const WriteOptions& write_options,
                        const std::string& key) = 0;
  virtual Status Get(const ReadOptions& read_options,
                     const std::string& key, std::string* value) = 0;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
