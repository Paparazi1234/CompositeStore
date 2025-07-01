#pragma once

#include <assert.h>

#include "include/multi_versions.h"
#include "port/port.h"

namespace MULTI_VERSIONS_NAMESPACE {

class WriteQueue {
 public:
  WriteQueue(MultiVersionsManager* multi_versions_manager)
      : multi_versions_manager_(multi_versions_manager) {}
  ~WriteQueue() {}

  void Enter() {
    mutex_.Lock();
  }

  void Exit() {
    mutex_.Unlock();
  }

  Version* VersionForInsert() {
    if (version_for_insert_.get() == nullptr) {
      version_for_insert_.reset(multi_versions_manager_->CreateVersion());
    }
    assert(version_for_insert_.get() != nullptr);
    return version_for_insert_.get();
  }

 private:
  port::Mutex mutex_;
  MultiVersionsManager *const multi_versions_manager_;
  // used to assign version for inserting entries in write path, lazy initialize
  // protected by the WriteQueue self
	std::unique_ptr<Version> version_for_insert_ = nullptr;
};

class ManagedWriteQueue {
 public:
  ManagedWriteQueue(WriteQueue* write_queue) : write_queue_(write_queue) {
    write_queue_->Enter();
  }
  ~ManagedWriteQueue() { write_queue_->Exit(); }

 private:
  WriteQueue *const write_queue_;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
