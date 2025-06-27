#pragma once

#include "port/port.h"

namespace MULTI_VERSIONS_NAMESPACE {

// Helper class that locks a mutex on construction and unlocks the mutex when
// the destructor of the MutexLock object is invoked.
//
// Typical usage:
//
//   void MyClass::MyMethod() {
//     MutexLock l(&mu_);       // mu_ is an instance variable
//     ... some complex code, possibly with multiple return paths ...
//   }

class MutexLock {
 public:
  explicit MutexLock(port::Mutex *mu) : mu_(mu) { this->mu_->Lock(); }
  // No copying allowed
  MutexLock(const MutexLock &) = delete;
  void operator=(const MutexLock &) = delete;

  ~MutexLock() { this->mu_->Unlock(); }

 private:
  port::Mutex *const mu_;
};

//
// Acquire a ReadLock on the specified RWMutex.
// The Lock will be automatically released when the
// object goes out of scope.
//
class ReadLock {
 public:
  explicit ReadLock(port::SharedMutex *mu) : mu_(mu) { this->mu_->ReadLock(); }
  // No copying allowed
  ReadLock(const ReadLock &) = delete;
  void operator=(const ReadLock &) = delete;

  ~ReadLock() { this->mu_->ReadUnlock(); }

 private:
  port::SharedMutex *const mu_;
};

//
// Acquire a WriteLock on the specified RWMutex.
// The Lock will be automatically released then the
// object goes out of scope.
//
class WriteLock {
 public:
  explicit WriteLock(port::SharedMutex *mu) : mu_(mu) { this->mu_->WriteLock(); }
  // No copying allowed
  WriteLock(const WriteLock &) = delete;
  void operator=(const WriteLock &) = delete;

  ~WriteLock() { this->mu_->WriteUnlock(); }

 private:
  port::SharedMutex *const mu_;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
