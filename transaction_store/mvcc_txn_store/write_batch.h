#pragma once

#include <map>
#include <string>
#include <assert.h>

#include "skiplist_rep.h"

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

class SkipListInsertHandler : public WriteBatch::Handler {
 public:
  SkipListInsertHandler(SkipListBackedRep* skiplist_backed_rep,
      Version* started_version, uint64_t version_inc)
      : skiplist_backed_rep_(skiplist_backed_rep),
        started_version_(started_version),
        version_inc_(version_inc) {
    assert(started_version_);
    assert(version_inc_ > 0);
  }
  ~SkipListInsertHandler() {}

  virtual Status Put(const std::string& key, const std::string& value) override;
  virtual Status Delete(const std::string& key) override;

 private:
  void MaybeAdvanceVersion() {
    if (!is_first_iterated_entry_ && version_inc_ > 1) {
      started_version_->IncreaseByOne();
    }
  }
  SkipListBackedRep* skiplist_backed_rep_;
  Version* started_version_;
  uint64_t version_inc_;
  bool is_first_iterated_entry_ = true;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
