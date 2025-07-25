#include "txn_lock_manager_test.h"

namespace COMPOSITE_STORE_NAMESPACE {

class CommonTxnLockManagerTest : public testing::Test {
 public:
  CommonTxnLockManagerTest() {}
  ~CommonTxnLockManagerTest() {}

  void TestTxnLockManagerFunc(void (CommonTxnLockManagerTests::*func)()) {
    TxnLockTrackerFactory* lock_tracker_factory =
        new OrdinaryTxnLockTrackerFactory();
    TxnLockManagerFactory* lock_manager_factory =
        new OrdinaryTxnLockManagerFactory();
    CommonTxnLockManagerTests tests(lock_tracker_factory, lock_manager_factory);
    (tests.*func)();
  }
};

TEST_F(CommonTxnLockManagerTest, ReentrantExclusiveLock) {
  TestTxnLockManagerFunc(&CommonTxnLockManagerTests::ReentrantExclusiveLock);
}

TEST_F(CommonTxnLockManagerTest, ReentrantSharedLock) {
  TestTxnLockManagerFunc(&CommonTxnLockManagerTests::ReentrantSharedLock);
}

TEST_F(CommonTxnLockManagerTest, LockUpgrade) {
  TestTxnLockManagerFunc(&CommonTxnLockManagerTests::LockUpgrade);
}

TEST_F(CommonTxnLockManagerTest, LockDowngrade) {
  TestTxnLockManagerFunc(&CommonTxnLockManagerTests::LockDowngrade);
}

TEST_F(CommonTxnLockManagerTest, LockConflict) {
  TestTxnLockManagerFunc(&CommonTxnLockManagerTests::LockConflict);
}

TEST_F(CommonTxnLockManagerTest, SharedLocks) {
  TestTxnLockManagerFunc(&CommonTxnLockManagerTests::SharedLocks);
}

TEST_F(CommonTxnLockManagerTest, Deadlock) {
  TestTxnLockManagerFunc(&CommonTxnLockManagerTests::Deadlock);
}

TEST_F(CommonTxnLockManagerTest, UnlockKeyTracker) {
  TestTxnLockManagerFunc(&CommonTxnLockManagerTests::UnlockKeyTracker);
}

TEST_F(CommonTxnLockManagerTest, ExceedNumLocksLimit) {
  TestTxnLockManagerFunc(&CommonTxnLockManagerTests::ExceedNumLocksLimit);
}

TEST_F(CommonTxnLockManagerTest, MultiThreadedLocking) {
  TestTxnLockManagerFunc(&CommonTxnLockManagerTests::MultiThreadedLocking);
}

class CommonTxnLockTrackerTest : public testing::Test {
 public:
  CommonTxnLockTrackerTest() {}
  ~CommonTxnLockTrackerTest() {}

  void TestTxnLockTrackerFunc(void (CommonTxnLockTrackerTests::*func)()) {
    TxnLockTrackerFactory* lock_tracker_factory =
        new OrdinaryTxnLockTrackerFactory();
    CommonTxnLockTrackerTests tests(lock_tracker_factory);
    (tests.*func)();
  }
};

TEST_F(CommonTxnLockTrackerTest, BasicTest) {
  TestTxnLockTrackerFunc(&CommonTxnLockTrackerTests::BasicTest);
}

TEST_F(CommonTxnLockTrackerTest, TrackedKeyIteratorTest) {
  TestTxnLockTrackerFunc(&CommonTxnLockTrackerTests::TrackedKeyIteratorTest);
}

}   // namespace COMPOSITE_STORE_NAMESPACE

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
