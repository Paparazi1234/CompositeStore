#pragma once

#include "multi_versions.h"

namespace MULTI_VERSIONS_NAMESPACE {

struct ReadOptions {
  const Snapshot* snapshot = nullptr;
};

struct WriteOptions {

};

struct StoreOptions {
  bool enable_two_write_queues = false;
};

struct TransactionStoreOptions : public StoreOptions {

};

}   // namespace MULTI_VERSIONS_NAMESPACE
