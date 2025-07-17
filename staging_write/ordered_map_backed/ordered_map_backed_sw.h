#pragma once

#include <map>
#include <assert.h>

#include "include/staging_write.h"
#include "include/format.h"

namespace COMPOSITE_STORE_NAMESPACE {

class OrderedMapBackedStagingWrite : public StagingWrite {
 public:
  // No copying allowed
  OrderedMapBackedStagingWrite(const OrderedMapBackedStagingWrite&) = delete;
  OrderedMapBackedStagingWrite& operator=(
      const OrderedMapBackedStagingWrite&) = delete;

  OrderedMapBackedStagingWrite() {}
  virtual ~OrderedMapBackedStagingWrite() {}

  Status Put(const std::string& key, const std::string& value) override {
    buffered_writes_.insert_or_assign(
        key, OneBufferedWrite(value, ValueType::kTypeValue));
    return Status::OK();
  }

  Status Delete(const std::string& key) override {
    buffered_writes_.insert_or_assign(
        key, OneBufferedWrite("", ValueType::kTypeDeletion));
    return Status::OK();
  }

  GetReault Get(const std::string& key, std::string* value) override {
    BufferedWrites::iterator iter = buffered_writes_.find(key);
    if (iter == buffered_writes_.end()) {
      return kNotFound;
    }
    if (iter->second.Type() == ValueType::kTypeDeletion) {
      return kDeleted;
    }
    assert(iter->second.Type() == ValueType::kTypeValue);
    *value = iter->second.Value();
    return kFound;
  }

  Status Iterate(Handler* handler) override;

  void Clear() override {
    buffered_writes_.clear();
  }

  bool IsEmpty() const override {
    return buffered_writes_.empty();
  }

  uint64_t Count() const override {
    return buffered_writes_.size();
  }

  void EncodeTo(std::string* dest) const override {
    assert(dest);
    dest->clear();
  }

 private:
  class OneBufferedWrite {
   public:
    OneBufferedWrite(const std::string& value, ValueType type)
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

  // since committing of a StagingWrite to store is all or nothing, it's ok that
  // we don't keep the inserting order info of the writes to a StagingWrite and
  // use a map is good with it
  using BufferedWrites = std::map<std::string, OneBufferedWrite>;
  BufferedWrites buffered_writes_;
};

}   // namespace COMPOSITE_STORE_NAMESPACE
