#pragma once

#include <map>
#include <string>
#include <assert.h>

#include "include/format.h"
#include "include/status.h"

namespace MULTI_VERSIONS_NAMESPACE {

class WriteBatch {
 public:
  WriteBatch() {}
  ~WriteBatch() {}

  enum GetReault : unsigned char {
    kFound = 0x0,
    kDeleted,
    kNotFound
  };

  class BufferedWrite {
   public:
    BufferedWrite(const std::string& value, ValueType type)
        : value_(value), type_(type) {} 

    const std::string& Value() const {
      return value_;
    }

    ValueType Type() const {
      return type_;
    }

   private:
    std::string value_;
    ValueType type_;
  };

  class Handler {
   public:
    virtual ~Handler() {}
    virtual Status Put(const std::string& key, const std::string& value) = 0;
    virtual Status Delete(const std::string& key) = 0;
  };

  void Put(const std::string& key, const std::string& value) {
    buffered_writes_.insert_or_assign(key, BufferedWrite(value, kTypeValue));
  }

  void Delete(const std::string& key) {
    buffered_writes_.insert_or_assign(key, BufferedWrite("", kTypeDeletion));
  }

  GetReault Get(const std::string& key, std::string* value) {
    BufferedWrites::iterator iter = buffered_writes_.find(key);
    if (iter == buffered_writes_.end()) {
      return kNotFound;
    }
    if (iter->second.Type() == kTypeDeletion) {
      return kDeleted;
    }
    assert(iter->second.Type() == kTypeValue);
    *value = iter->second.Value();
    return kFound;
  }

  void Clear() {
    buffered_writes_.clear();
  }

  Status Iterate(Handler* handler);

  bool IsEmpty() const {
    return buffered_writes_.empty();
  }

  uint64_t Count() const {
    return buffered_writes_.size();
  }

 private:
  // since write a write_batch to store is all or nothing, it's ok that we 
  // don't keep the ordering of the writes to a write_batch and use a map is
  // good with it
  using BufferedWrites = std::map<std::string, BufferedWrite>;
  BufferedWrites buffered_writes_;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
