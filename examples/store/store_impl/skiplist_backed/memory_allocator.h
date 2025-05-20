#pragma once

#include "../../../../third-party/rocksdb/allocator.h"

namespace MULTI_VERSIONS_NAMESPACE {

class MemoryAllocator : public ROCKSDB_NAMESPACE::Allocator {
 public:
  MemoryAllocator() {}
  ~MemoryAllocator() {}

  virtual char* Allocate(size_t bytes) override;
  virtual char* AllocateAligned(size_t bytes) override;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
