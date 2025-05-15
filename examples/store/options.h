#pragma once

namespace MULTI_VERSIONS_NAMESPACE {

struct ReadOptions {

};

struct WriteOptions {

};

enum StoreBackedType : unsigned char {
  kTypeSkipList = 0x0,
  kMaxStoreBackedType
};

struct StoreOptions {
  StoreBackedType store_backed_type = StoreBackedType::kTypeSkipList;
  bool enable_txn_if_supported = false;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
