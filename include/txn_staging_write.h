#pragma once

#include <string>

#include "status.h"

namespace MULTI_VERSIONS_NAMESPACE {

class TxnStagingWrite {
 public:
  // No copying allowed
  TxnStagingWrite(const TxnStagingWrite&) = delete;
  TxnStagingWrite& operator=(const TxnStagingWrite&) = delete;

  TxnStagingWrite() {}
  virtual ~TxnStagingWrite() {}

  virtual void Put(const std::string& key, const std::string& value) = 0;
  virtual void Delete(const std::string& key) = 0;

  enum GetReault : unsigned char {
    kFound = 0x0,
    kDeleted,
    kNotFound
  };
  virtual GetReault Get(const std::string& key, std::string* value) = 0;

  class Handler {
   public:
    virtual ~Handler() {}
    virtual Status Put(const std::string& key, const std::string& value) = 0;
    virtual Status Delete(const std::string& key) = 0;
  };
  virtual Status Iterate(Handler* handler) = 0;

  virtual void Clear() = 0;
  virtual bool IsEmpty() const = 0;
  virtual uint64_t Count() const = 0;

  virtual void EncodeTo(std::string* dest) const = 0;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
