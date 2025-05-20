#pragma once

#include "../../include/snapshot.h"

namespace MULTI_VERSIONS_NAMESPACE {

struct ReadOptions {
  const Snapshot* snapshot = nullptr;
};

struct WriteOptions {

};

enum StoreBackedType : unsigned char {
  kTypeSkipList = 0x0,
  kMaxStoreBackedType
};

enum TxnStoreWritePolicy : unsigned char {
  WRITE_COMMITTED = 0x0,
  WRITE_PREPARED = 0x1
};

struct StoreOptions {
  StoreBackedType store_backed_type = StoreBackedType::kTypeSkipList;
  bool enable_txn_if_supported = false;
  TxnStoreWritePolicy write_policy = WRITE_COMMITTED;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
