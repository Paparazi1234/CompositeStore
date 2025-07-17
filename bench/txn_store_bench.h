#pragma once

#include "include/transaction_store.h"

namespace COMPOSITE_STORE_NAMESPACE {

class TxnStoreBench {
 public:
  TxnStoreBench() {}
  ~TxnStoreBench() {}

  void ReadModifyWrite();
  void ReadOnly();
  void WriteOnly();

 private:
  
};

}   // namespace COMPOSITE_STORE_NAMESPACE
