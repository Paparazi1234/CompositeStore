#pragma once

#include "include/transaction_store.h"

namespace MULTI_VERSIONS_NAMESPACE {

class TxnStoreBench {
 public:
  TxnStoreBench() {}
  ~TxnStoreBench() {}

  void ReadModifyWrite();
  void ReadOnly();
  void WriteOnly();

 private:
  
};

}   // namespace MULTI_VERSIONS_NAMESPACE
