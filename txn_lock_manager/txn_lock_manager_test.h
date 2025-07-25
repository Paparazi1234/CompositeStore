#pragma once

#include <unordered_set>

#include "include/txn_lock_manager.h"
#include "third-party/gtest/gtest.h"
#include "port/port.h"

namespace COMPOSITE_STORE_NAMESPACE {

class CommonTxnLockManagerTests {
 public:
  CommonTxnLockManagerTests(TxnLockTrackerFactory* lock_tracker_factory,
                            TxnLockManagerFactory* lock_manager_factory)
      : lock_tracker_factory_(lock_tracker_factory),
        lock_manager_factory_(lock_manager_factory) {}

  ~CommonTxnLockManagerTests() {
    delete lock_tracker_factory_;
    delete lock_manager_factory_;
  }

  void ReentrantExclusiveLock();
  void ReentrantSharedLock();
  void LockUpgrade();
  void LockDowngrade();
  void LockConflict();
  void SharedLocks();
  void Deadlock();

  void UnlockKeyTracker();
  void ExceedNumLocksLimit();

  void MultiThreadedLocking();

 private:
  TxnLockTrackerFactory* lock_tracker_factory_;
  TxnLockManagerFactory* lock_manager_factory_;

  const int64_t WONT_TIMEOUE = -1;
  const int64_t TIMEOUT_TIME_3MS = 3;
};

class CommonTxnLockTrackerTests {
 public:
  CommonTxnLockTrackerTests(TxnLockTrackerFactory* lock_tracker_factory)
      : lock_tracker_factory_(lock_tracker_factory) {}

  ~CommonTxnLockTrackerTests() {
    delete lock_tracker_factory_;
  }

  void BasicTest();
  void TrackedKeyIteratorTest();

 private:
  TxnLockTrackerFactory* lock_tracker_factory_;
};

void CommonTxnLockManagerTests::ReentrantExclusiveLock() {
  const uint64_t txn_id = 1;
  TxnLockManager* lock_manager =
      lock_manager_factory_->CreateTxnLockManager(SystemClock::GetSingleton());

  Status s = lock_manager->TryLock(txn_id, "foo", true, WONT_TIMEOUE);
  ASSERT_TRUE(s.IsOK());
  s = lock_manager->TryLock(txn_id, "foo", true, WONT_TIMEOUE);
  ASSERT_TRUE(s.IsOK());
  ASSERT_EQ(lock_manager->NumLocks(), 1ull);

  // cleanup
  lock_manager->UnLock(txn_id, "foo");
  ASSERT_EQ(lock_manager->NumLocks(), 0ull);

  delete lock_manager;
}

void CommonTxnLockManagerTests::ReentrantSharedLock() {
  const uint64_t txn_id = 1;
  TxnLockManager* lock_manager =
      lock_manager_factory_->CreateTxnLockManager(SystemClock::GetSingleton());

  Status s = lock_manager->TryLock(txn_id, "foo", false, WONT_TIMEOUE);
  ASSERT_TRUE(s.IsOK());
  s = lock_manager->TryLock(txn_id, "foo", false, WONT_TIMEOUE);
  ASSERT_TRUE(s.IsOK());
  ASSERT_EQ(lock_manager->NumLocks(), 1ull);

  // cleanup
  lock_manager->UnLock(txn_id, "foo");
  ASSERT_EQ(lock_manager->NumLocks(), 0ull);

  delete lock_manager;
}

void CommonTxnLockManagerTests::LockUpgrade() {
  const uint64_t txn_id1 = 1;
  const uint64_t txn_id2 = 2;
  TxnLockManager* lock_manager =
      lock_manager_factory_->CreateTxnLockManager(SystemClock::GetSingleton());

  // first shared by txn1 and txn2
  Status s = lock_manager->TryLock(txn_id1, "foo", false, WONT_TIMEOUE);
  ASSERT_TRUE(s.IsOK());
  s = lock_manager->TryLock(txn_id2, "foo", false, WONT_TIMEOUE);
  ASSERT_TRUE(s.IsOK());
  ASSERT_EQ(lock_manager->NumLocks(), 1ull);

  // upgrade by txn1 failed
  s = lock_manager->TryLock(txn_id1, "foo", true, TIMEOUT_TIME_3MS);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(lock_manager->NumLocks(), 1ull);

  // txn2 unlock
  lock_manager->UnLock(txn_id2, "foo");
  ASSERT_EQ(lock_manager->NumLocks(), 1ull);

  // upgraded by txn1 succeed
  s = lock_manager->TryLock(txn_id1, "foo", true, WONT_TIMEOUE);
  ASSERT_TRUE(s.IsOK());
  ASSERT_EQ(lock_manager->NumLocks(), 1ull);

  // txn2 re-lock failed
  s = lock_manager->TryLock(txn_id2, "foo", true, TIMEOUT_TIME_3MS);
  ASSERT_TRUE(s.IsTimedOut());
  s = lock_manager->TryLock(txn_id2, "foo", false, TIMEOUT_TIME_3MS);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(lock_manager->NumLocks(), 1ull);

  // cleanup
  lock_manager->UnLock(txn_id1, "foo");
  ASSERT_EQ(lock_manager->NumLocks(), 0ull);

  delete lock_manager;
}

void CommonTxnLockManagerTests::LockDowngrade() {
  const uint64_t txn_id1 = 1;
  const uint64_t txn_id2 = 2;
  TxnLockManager* lock_manager =
      lock_manager_factory_->CreateTxnLockManager(SystemClock::GetSingleton());

  // first exclusive locked by txn1
  Status s = lock_manager->TryLock(txn_id1, "foo", true, WONT_TIMEOUE);
  ASSERT_TRUE(s.IsOK());
  ASSERT_EQ(lock_manager->NumLocks(), 1ull);

  // txn2 lock failed
  s = lock_manager->TryLock(txn_id2, "foo", true, TIMEOUT_TIME_3MS);
  ASSERT_TRUE(s.IsTimedOut());
  s = lock_manager->TryLock(txn_id2, "foo", false, TIMEOUT_TIME_3MS);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(lock_manager->NumLocks(), 1ull);

  // downgraded by txn1
  s = lock_manager->TryLock(txn_id1, "foo", false, WONT_TIMEOUE);
  ASSERT_TRUE(s.IsOK());
  ASSERT_EQ(lock_manager->NumLocks(), 1ull);

  // txn2 shared succeed
  s = lock_manager->TryLock(txn_id2, "foo", false, WONT_TIMEOUE);
  ASSERT_TRUE(s.IsOK());
  ASSERT_EQ(lock_manager->NumLocks(), 1ull);

  // cleanup
  lock_manager->UnLock(txn_id2, "foo");
  ASSERT_EQ(lock_manager->NumLocks(), 1ull);

  lock_manager->UnLock(txn_id1, "foo");
  ASSERT_EQ(lock_manager->NumLocks(), 0ull);

  delete lock_manager;
}

void CommonTxnLockManagerTests::LockConflict() {
  const uint64_t txn_id1 = 1;
  const uint64_t txn_id2 = 2;
  TxnLockManager* lock_manager =
      lock_manager_factory_->CreateTxnLockManager(SystemClock::GetSingleton());

  // exclusive-exclusive conflict.
  Status s = lock_manager->TryLock(txn_id1, "foo", true, WONT_TIMEOUE);
  ASSERT_TRUE(s.IsOK());
  s = lock_manager->TryLock(txn_id2, "foo", true, TIMEOUT_TIME_3MS);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(lock_manager->NumLocks(), 1ull);

  // exclusive-shared conflict.
  s = lock_manager->TryLock(txn_id1, "foo1", true, WONT_TIMEOUE);
  ASSERT_TRUE(s.IsOK());
  s = lock_manager->TryLock(txn_id2, "foo1", false, TIMEOUT_TIME_3MS);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(lock_manager->NumLocks(), 2ull);

  // shared-exclusive conflict.
  s = lock_manager->TryLock(txn_id1, "foo2", false, WONT_TIMEOUE);
  ASSERT_TRUE(s.IsOK());
  s = lock_manager->TryLock(txn_id2, "foo2", true, TIMEOUT_TIME_3MS);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(lock_manager->NumLocks(), 3ull);

  // cleanup
  lock_manager->UnLock(txn_id1, "foo2");
  ASSERT_EQ(lock_manager->NumLocks(), 2ull);

  lock_manager->UnLock(txn_id1, "foo1");
  ASSERT_EQ(lock_manager->NumLocks(), 1ull);

  lock_manager->UnLock(txn_id1, "foo");
  ASSERT_EQ(lock_manager->NumLocks(), 0ull);

  delete lock_manager;
}

void CommonTxnLockManagerTests::SharedLocks() {
  const uint64_t txn_id1 = 1;
  const uint64_t txn_id2 = 2;
  TxnLockManager* lock_manager =
      lock_manager_factory_->CreateTxnLockManager(SystemClock::GetSingleton());

  Status s = lock_manager->TryLock(txn_id1, "foo", false, WONT_TIMEOUE);
  ASSERT_TRUE(s.IsOK());
  s = lock_manager->TryLock(txn_id2, "foo", false, WONT_TIMEOUE);
  ASSERT_TRUE(s.IsOK());
  ASSERT_EQ(lock_manager->NumLocks(), 1ull);

  // cleanup
  lock_manager->UnLock(txn_id2, "foo");
  ASSERT_EQ(lock_manager->NumLocks(), 1ull);

  lock_manager->UnLock(txn_id1, "foo");
  ASSERT_EQ(lock_manager->NumLocks(), 0ull);

  delete lock_manager;
}

void CommonTxnLockManagerTests::Deadlock() {
  // Todo: impl
}

void CommonTxnLockManagerTests::UnlockKeyTracker() {
  const uint64_t txn_id1 = 1;
  const uint64_t txn_id2 = 2;
  TxnLockManager* lock_manager =
      lock_manager_factory_->CreateTxnLockManager(SystemClock::GetSingleton());
  TxnLockTracker* lock_tracker1 = lock_tracker_factory_->CreateTxnLockTracker();
  TxnLockTracker* lock_tracker2 = lock_tracker_factory_->CreateTxnLockTracker();

  Status s = lock_manager->TryLock(txn_id1, "foo", true, WONT_TIMEOUE);
  ASSERT_TRUE(s.IsOK());
  lock_tracker1->TrackKey("foo", true);
  s = lock_manager->TryLock(txn_id2, "bar", true, WONT_TIMEOUE);
  ASSERT_TRUE(s.IsOK());
  lock_tracker2->TrackKey("bar", true);
  ASSERT_EQ(lock_manager->NumLocks(), 2ull);

  s = lock_manager->TryLock(txn_id1, "foo1", false, WONT_TIMEOUE);
  ASSERT_TRUE(s.IsOK());
  lock_tracker1->TrackKey("foo1", false);
  s = lock_manager->TryLock(txn_id2, "foo1", false, WONT_TIMEOUE);
  ASSERT_TRUE(s.IsOK());
  lock_tracker2->TrackKey("foo1", false);
  ASSERT_EQ(lock_manager->NumLocks(), 3ull);

  s = lock_manager->TryLock(txn_id1, "foo2", true, WONT_TIMEOUE);
  ASSERT_TRUE(s.IsOK());
  lock_tracker1->TrackKey("foo2", true);
  s = lock_manager->TryLock(txn_id2, "bar2", true, WONT_TIMEOUE);
  ASSERT_TRUE(s.IsOK());
  lock_tracker2->TrackKey("bar2", true);
  ASSERT_EQ(lock_manager->NumLocks(), 5ull);

  // cleanup
  lock_manager->UnLock(txn_id1, *lock_tracker1);
  ASSERT_EQ(lock_manager->NumLocks(), lock_tracker2->NumTrackedKeys());
  lock_manager->UnLock(txn_id2, *lock_tracker2);
  ASSERT_EQ(lock_manager->NumLocks(), 0ull);

  delete lock_tracker2;
  delete lock_tracker1;
  delete lock_manager;
}

void CommonTxnLockManagerTests::ExceedNumLocksLimit() {
  const uint64_t txn_id = 1;
  const int64_t NUM_LOCKS_LIMIT = 2;
  TxnLockManager* lock_manager = lock_manager_factory_->CreateTxnLockManager(
      SystemClock::GetSingleton(), NUM_LOCKS_LIMIT);

  Status s = lock_manager->TryLock(txn_id, "foo", true, WONT_TIMEOUE);
  ASSERT_TRUE(s.IsOK());
  s = lock_manager->TryLock(txn_id, "foo1", false, WONT_TIMEOUE);
  ASSERT_TRUE(s.IsOK());

  s = lock_manager->TryLock(txn_id, "foo2", true, TIMEOUT_TIME_3MS);
  ASSERT_TRUE(s.IsBusy());
  s = lock_manager->TryLock(txn_id, "foo3", true, TIMEOUT_TIME_3MS);
  ASSERT_TRUE(s.IsBusy());

  ASSERT_EQ(lock_manager->NumLocks(), uint64_t(NUM_LOCKS_LIMIT));

  // cleanup
  lock_manager->UnLock(txn_id, "foo");
  lock_manager->UnLock(txn_id, "foo1");
  ASSERT_EQ(lock_manager->NumLocks(), 0ull);

  delete lock_manager;
}

void LockFunc(TxnLockManager* lock_manager, uint64_t txn_id,
              const std::string& key, int64_t timeout_time_ms){
  Status s = lock_manager->TryLock(txn_id, key, true, timeout_time_ms);
  ASSERT_TRUE(s.IsOK());

  SystemClock::GetSingleton()->SleepForMicroseconds(100);

  // cleanup
  lock_manager->UnLock(txn_id, key);
}

void CommonTxnLockManagerTests::MultiThreadedLocking() {
  const uint64_t txn_id1 = 1;
  const uint64_t txn_id2 = 2;
  const uint64_t txn_id3 = 3;
  TxnLockManager* lock_manager =
      lock_manager_factory_->CreateTxnLockManager(SystemClock::GetSingleton());

  port::Thread thread1(LockFunc, lock_manager, txn_id1, "foo", WONT_TIMEOUE);
  port::Thread thread2(LockFunc, lock_manager, txn_id2, "foo", WONT_TIMEOUE);
  port::Thread thread3(LockFunc, lock_manager, txn_id3, "foo", WONT_TIMEOUE);

  thread1.join();
  thread2.join();
  thread3.join();

  // cleanup
  delete lock_manager;
}

void CommonTxnLockTrackerTests::BasicTest() {
  TxnLockTracker* lock_tracker = lock_tracker_factory_->CreateTxnLockTracker();

  lock_tracker->TrackKey("foo", false);
  lock_tracker->TrackKey("foo1", true);
  ASSERT_EQ(lock_tracker->NumTrackedKeys(), 2ull);

  // upgrade exclusive
  lock_tracker->TrackKey("foo", true);
  bool exclusive;
  bool tracked = lock_tracker->IsKeyAlreadyTracked("foo", &exclusive);
  ASSERT_TRUE(tracked && exclusive == true);
  ASSERT_EQ(lock_tracker->NumTrackedKeys(), 2ull);

  // won't downgrade
  lock_tracker->TrackKey("foo", false);
  tracked = lock_tracker->IsKeyAlreadyTracked("foo", &exclusive);
  ASSERT_TRUE(tracked && exclusive == true);
  ASSERT_EQ(lock_tracker->NumTrackedKeys(), 2ull);

  // cleanup
  lock_tracker->Clear();
  ASSERT_EQ(lock_tracker->NumTrackedKeys(), 0ull);

  delete lock_tracker;
}

void CommonTxnLockTrackerTests::TrackedKeyIteratorTest() {
  TxnLockTracker* lock_tracker = lock_tracker_factory_->CreateTxnLockTracker();

  std::unordered_set<std::string> keys = {"foo", "bar", "foo1", "bar1"};
  for (auto& key : keys) {
    lock_tracker->TrackKey(key, true);
  }
  ASSERT_EQ(lock_tracker->NumTrackedKeys(), keys.size());

  TxnLockTracker::TrackedKeyIterator* iter =
      lock_tracker->GetTrackedKeyIterator();
  std::unordered_set<std::string> keys_in_tracker;
  while (iter->HasNext()) {
    keys_in_tracker.insert(iter->Next());
  }
  
  for (auto& key : keys) {
    ASSERT_TRUE(keys_in_tracker.find(key) != keys_in_tracker.end());
  }

  // cleanup
  lock_tracker->Clear();
  ASSERT_EQ(lock_tracker->NumTrackedKeys(), 0ull);

  delete iter;
  delete lock_tracker;
}

}   // namespace COMPOSITE_STORE_NAMESPACE
