#pragma once

#include <mutex>

#include "include/multi_versions_namespace.h"

namespace MULTI_VERSIONS_NAMESPACE {

class WriteLock {
 public:
  WriteLock() {}
  ~WriteLock() {}

  void Lock() {
    mutex_.lock();
  }

  void Unlock() {
    mutex_.unlock();
  }
 private:
  std::mutex mutex_;
};

class ManagedWriteLock {
 public:
  ManagedWriteLock(WriteLock* write_lock) : write_lock_(write_lock) {
    write_lock_->Lock();
  }
  ~ManagedWriteLock() { write_lock_->Unlock(); }

 private:
  WriteLock* const write_lock_;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
