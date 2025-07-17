#pragma once

#include <deque>
#include <memory>
#include <cstddef>

#include "include/composite_store_namespace.h"

namespace COMPOSITE_STORE_NAMESPACE {

class MemoryAllocator {
 public:
  MemoryAllocator() {
    active_block_remaining_bytes_ = sizeof(inline_block_);
    aligned_alloc_ptr_ = inline_block_;
    unaligned_alloc_ptr = inline_block_ + active_block_remaining_bytes_;
    if (kBlockSize % kAlignUnit != 0) {
      kBlockSize = (1 + kBlockSize / kAlignUnit) * kAlignUnit;
    }
  }
  ~MemoryAllocator() {}

  char* Allocate(size_t bytes);
  char* AllocateAligned(size_t bytes);

 private:
  char* AllocateFallback(size_t bytes, bool aligned);
  char* AllocateNewBlock(size_t block_bytes);

  static constexpr size_t kInlineBlockSize = 2048;
  static constexpr unsigned kAlignUnit = alignof(std::max_align_t);
  static_assert((kAlignUnit & (kAlignUnit - 1)) == 0,
                "Pointer size should be power of 2");
  size_t kBlockSize = 8192;

  alignas(std::max_align_t) char inline_block_[kInlineBlockSize];
  std::deque<std::unique_ptr<char[]>> allocated_blocks_;

  char* aligned_alloc_ptr_ = nullptr;
  char* unaligned_alloc_ptr = nullptr;
  size_t active_block_remaining_bytes_ = 0;
};

}   // namespace COMPOSITE_STORE_NAMESPACE
