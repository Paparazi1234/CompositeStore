#pragma once

#include "port/port.h"

namespace MULTI_VERSIONS_NAMESPACE {

class WriteQueue {
 public:
  WriteQueue() {}
  ~WriteQueue() {}

  void Enter() {
    mutex_.Lock();
  }

  void Exit() {
    mutex_.Unlock();
  }
 private:
  port::Mutex mutex_;
};

class ManagedWriteQueue {
 public:
  ManagedWriteQueue(WriteQueue* write_queue) : write_queue_(write_queue) {
    write_queue_->Enter();
  }
  ~ManagedWriteQueue() { write_queue_->Exit(); }

 private:
  WriteQueue* const write_queue_;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
