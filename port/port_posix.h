#pragma once

#include <thread>

#include "include/multi_versions_namespace.h"

namespace MULTI_VERSIONS_NAMESPACE {

namespace port {

class CondVar;

class Mutex {
 public:
  Mutex();
  // No copying
  Mutex(const Mutex&) = delete;
  void operator=(const Mutex&) = delete;

  ~Mutex();

  void Lock();
  void Unlock();

  bool TryLock();

  // this will assert if the mutex is not locked
  // it does NOT verify that mutex is held by a calling thread
  void AssertHeld();

  // Also implement std Lockable
  inline void lock() { Lock(); }
  inline void unlock() { Unlock(); }
  inline bool try_lock() { return TryLock(); }

 private:
  friend class CondVar;
  pthread_mutex_t mu_;
#ifndef NDEBUG
  bool locked_ = false;
#endif
};

class SharedMutex {
 public:
  SharedMutex();
  // No copying allowed
  SharedMutex(const SharedMutex&) = delete;
  void operator=(const SharedMutex&) = delete;

  ~SharedMutex();

  void ReadLock();
  void WriteLock();
  void ReadUnlock();
  void WriteUnlock();
  void AssertHeld() {}

 private:
  pthread_rwlock_t mu_;  // the underlying platform mutex
};

class CondVar {
 public:
  explicit CondVar(Mutex* mu);
  ~CondVar();
  void Wait();
  // Timed condition wait.  Returns true if timeout occurred.
  bool TimedWait(uint64_t abs_time_us);
  void Signal();
  void SignalAll();

 private:
  pthread_cond_t cv_;
  Mutex* mu_;
};

}   // namespace port
}   // namespace MULTI_VERSIONS_NAMESPACE
