#if !defined(OS_WIN)

#include <assert.h>
#include <cstdio>
#include <cstdlib>

#include "port_posix.h"

namespace COMPOSITE_STORE_NAMESPACE {
namespace port {

static int PthreadCall(const char* label, int result) {
  if (result != 0 && result != ETIMEDOUT && result != EBUSY) {
    fprintf(stderr, "pthread: errno %s: %d\n", label, result);
    abort();
  }
  return result;
}

Mutex::Mutex() {
  PthreadCall("init mutex", pthread_mutex_init(&mu_, nullptr));
}

Mutex::~Mutex() { PthreadCall("destroy mutex", pthread_mutex_destroy(&mu_)); }

void Mutex::Lock() {
  PthreadCall("lock", pthread_mutex_lock(&mu_));
#ifndef NDEBUG
  locked_ = true;
#endif
}

void Mutex::Unlock() {
#ifndef NDEBUG
  locked_ = false;
#endif
  PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}

bool Mutex::TryLock() {
  bool ret = PthreadCall("trylock", pthread_mutex_trylock(&mu_)) == 0;
#ifndef NDEBUG
  if (ret) {
    locked_ = true;
  }
#endif
  return ret;
}

bool Mutex::TryLockFor(uint64_t timeout_time_us) {
#ifdef PTHREAD_MUTEX_TIMEDLOCK_AVAILABLE
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  ts.tv_nsec += timeout_time_us * 1000;
  bool ret = PthreadCall("trylockfor", pthread_mutex_timedlock(&mu_, &ts)) == 0;
#ifndef NDEBUG
  if (ret) {
    locked_ = true;
  }
#endif
  return ret;
#else   // if pthread_mutex_timedlock not available, then use pthread_mutex_lock
  PthreadCall("lock", pthread_mutex_lock(&mu_));
#ifndef NDEBUG
  locked_ = true;
#endif
  return true;
#endif  // PTHREAD_MUTEX_TIMEDLOCK_AVAILABLE
}

void Mutex::AssertHeld() {
#ifndef NDEBUG
  assert(locked_);
#endif
}

SharedMutex::SharedMutex() {
  PthreadCall("init mutex", pthread_rwlock_init(&mu_, nullptr));
}

SharedMutex::~SharedMutex() {
  PthreadCall("destroy mutex", pthread_rwlock_destroy(&mu_));
}

void SharedMutex::ReadLock() {
  PthreadCall("read lock", pthread_rwlock_rdlock(&mu_));
}

void SharedMutex::WriteLock() {
  PthreadCall("write lock", pthread_rwlock_wrlock(&mu_));
}

void SharedMutex::ReadUnlock() {
  PthreadCall("read unlock", pthread_rwlock_unlock(&mu_));
}

void SharedMutex::WriteUnlock() {
  PthreadCall("write unlock", pthread_rwlock_unlock(&mu_));
}

CondVar::CondVar(Mutex* mu) : mu_(mu) {
  PthreadCall("init cv", pthread_cond_init(&cv_, nullptr));
}

CondVar::~CondVar() { PthreadCall("destroy cv", pthread_cond_destroy(&cv_)); }

void CondVar::Wait() {
#ifndef NDEBUG
  mu_->locked_ = false;
#endif
  PthreadCall("wait", pthread_cond_wait(&cv_, &mu_->mu_));
#ifndef NDEBUG
  mu_->locked_ = true;
#endif
}

bool CondVar::TimedWait(uint64_t abs_time_us) {
  struct timespec ts;
  ts.tv_sec = static_cast<time_t>(abs_time_us / 1000000);
  ts.tv_nsec = static_cast<suseconds_t>((abs_time_us % 1000000) * 1000);

#ifndef NDEBUG
  mu_->locked_ = false;
#endif  
  int err = pthread_cond_timedwait(&cv_, &mu_->mu_, &ts);
#ifndef NDEBUG
  mu_->locked_ = true;
#endif
  if (err == ETIMEDOUT) {
    return true;
  }
  if (err != 0) {
    PthreadCall("timedwait", err);
  }
  return false;
}

void CondVar::Signal() { PthreadCall("signal", pthread_cond_signal(&cv_)); }

void CondVar::SignalAll() {
  PthreadCall("broadcast", pthread_cond_broadcast(&cv_));
}

}   // namespace port
}   // namespace COMPOSITE_STORE_NAMESPACE

#endif
