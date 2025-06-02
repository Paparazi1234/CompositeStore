#include "memory_allocator.h"

#include <assert.h>

namespace MULTI_VERSIONS_NAMESPACE {

char* MemoryAllocator::Allocate(size_t bytes) {
  assert(bytes > 0);
  if (bytes <= active_block_remaining_bytes_) {
    unaligned_alloc_ptr -= bytes;
    active_block_remaining_bytes_ -= bytes;
    return unaligned_alloc_ptr;
  }
  return AllocateFallback(bytes, false);
}

char* MemoryAllocator::AllocateAligned(size_t bytes) {
  size_t current_mod =
      reinterpret_cast<uintptr_t>(aligned_alloc_ptr_) & (kAlignUnit - 1);
  size_t slop = (current_mod == 0 ? 0 : kAlignUnit - current_mod);
  size_t needed = bytes + slop;
  char* result;
  if (needed <= active_block_remaining_bytes_) {
    result = aligned_alloc_ptr_ + slop;
    aligned_alloc_ptr_ += needed;
    active_block_remaining_bytes_ -= needed;
  } else {
    result = AllocateFallback(bytes, true);
  }
  assert((reinterpret_cast<uintptr_t>(result) & (kAlignUnit - 1)) == 0);
  return result;
}

char* MemoryAllocator::AllocateFallback(size_t bytes, bool aligned) {
  // if required size is more than 1/4 kBlockSize, then allocate a separate
  // block for this memory requirement
  if (bytes > kBlockSize / 4) {
    return AllocateNewBlock(bytes);
  }

  // required size less than or equal 1/4 kBlockSize and active block can't
  // offer it, then switch to a new block and waste remaining bytes in active 
  // block
  size_t block_size = kBlockSize;
  char* new_block_head = AllocateNewBlock(block_size);
  active_block_remaining_bytes_ = block_size - bytes;
  if (aligned) {
    aligned_alloc_ptr_ = new_block_head + bytes;
    unaligned_alloc_ptr = new_block_head + block_size;
    return new_block_head;
  } else {
    aligned_alloc_ptr_ = new_block_head;
    unaligned_alloc_ptr = new_block_head + block_size - bytes;
    return unaligned_alloc_ptr;
  }
}

char* MemoryAllocator::AllocateNewBlock(size_t block_bytes) {
  char* new_block = new char[block_bytes];
  allocated_blocks_.push_back(std::unique_ptr<char[]>(new_block));
  return new_block;
}

}   // namespace MULTI_VERSIONS_NAMESPACE
