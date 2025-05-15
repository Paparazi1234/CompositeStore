#pragma once

#include <string>
#include <deque>
#include <assert.h>

#include "format.h"

namespace MULTI_VERSIONS_NAMESPACE {

class WriteBatch {
 public:
  ~WriteBatch() {}

  class BufferedWrite {
   public:
    BufferedWrite(const std::string& key,
                  const std::string& value, ValueType value_type)
        : key_(key),
          value_(value),
          value_type_(value_type) {} 

    const std::string& Key() const {
      return key_;
    }

    const std::string& Value() const {
      return value_;
    }

    ValueType Type() const {
      return value_type_;
    }

   private:
    const std::string key_;
    const std::string value_;
    const ValueType value_type_;
  };

  void Put(const std::string& key, const std::string& value) {
    buffered_writes_.push_back(
        std::move(BufferedWrite(key, value, ValueType::kTypeValue)));
  }

  void Delete(const std::string& key) {
    buffered_writes_.push_back(
        std::move(BufferedWrite(key, "", ValueType::kTypeDeletion)));
  }

  const BufferedWrite& GetOneWrite(size_t i) {
    assert(0 <= i && i < buffered_writes_.size());
    return buffered_writes_.at(i);
  }

  bool IsEmpty() const {
    return buffered_writes_.empty();
  }

  size_t Count() const {
    return buffered_writes_.size();
  }

 private:
  using BufferedWrites = std::deque<BufferedWrite>;
  BufferedWrites buffered_writes_;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
