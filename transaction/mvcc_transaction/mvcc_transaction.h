#pragma once

#include <atomic>

#include "transaction_store/mvcc_txn_store/mvcc_txn_store.h"

namespace COMPOSITE_STORE_NAMESPACE {

class MVCCTransaction : public Transaction {
 public:
  // No copying allowed
  MVCCTransaction(const MVCCTransaction&) = delete;
  MVCCTransaction& operator=(const MVCCTransaction&) = delete;

  MVCCTransaction(TransactionStore* txn_store,
                  const WriteOptions& write_options,
                  const TransactionOptions& txn_options);
  virtual ~MVCCTransaction() {
    if (!GetTxnStore()->IsTestCrashEnabled()) {
      assert(txn_lock_tracker_->NumTrackedKeys() == 0);
    }
  }

  virtual Status Put(const std::string& key, const std::string& value) override; 
  virtual Status Delete(const std::string& key) override;

  virtual Status Get(const ReadOptions& read_options, const std::string& key,
                     std::string* value) override;

  virtual void SetSnapshot() override;

  // this func only invoked when we reuse a txn, it needs to not only re-init
  // current class's member variables and then do the cleanup of current class
  // but it also needs to invoke it's direct parent class's Reinitialize()
  // explicitly, like Parent::Reinitialize() when this func is overriden,
  // here is the funcs invoked sequence:
  // assume we have classes as follow for example:
  //    class Parant;
  //    class Child : public Parent;     // Child derived from Parant
  //    class GrandChild : public Child; // GrandChild derived from Child
  // then each class's Reinitialize() impl will be:
  //    Parant::Reinitialize() {
  //      // do current class's re-initialization of it's own member variables
  //      // and it's own resources cleanup
  //      ...
  //    }
  //
  //    Child::Reinitialize() {
  //      // first do current class's re-initialization of it's own member
  //      // variables and it's own resources cleanup
  //      ...
  //      // then invoke it's direct parent class's Reinitialize() explicitly
  //      Parant::Reinitialize();
  //    }
  //
  //    GrandChild::Reinitialize() {
  //      // first do current class's re-initialization of it's own member
  //      // variables and it's own resources cleanup
  //      ...
  //      // then invoke it's direct parent class's Reinitialize() explicitly
  //      Child::Reinitialize();
  //    }
  //
  // when we have code like this:
  //    Parent* ptr = new GrandChild();
  //    // do some job to *ptr
  //    ...
  //    // then reuse *ptr after job done
  //    ptr->Reinitialize();
  //
  // we will have following sequences of re-initialization calls
  //    GrandChild's own re-initialization jobs and resources cleanup done
  //    Child's own re-initialization jobs and resources cleanup done
  //    Parant's own re-initialization jobs and resources cleanup done
  // this seems a kind of ideal sequences of re-initialization jobs and
  // resources cleanup done and we please stick to this principle
  virtual void Reinitialize(TransactionStore* txn_store,
                            const WriteOptions& write_options,
                            const TransactionOptions& txn_options);

  MVCCTxnStore* GetTxnStore() const {
    return txn_store_;
  }

 protected:
  enum TransactionState : unsigned char {
    STAGE_WRITING = 0x0,
    STAGE_PREPARING = 0x1,
    STAGE_PREPARED = 0x2,
    STAGE_COMMITTING = 0x3,
    STAGE_COMMITTED = 0x4,
    STAGE_ROLLBACKING = 0x5,
    STAGE_ROLLBACKED = 0x6
  };

  // let derived class impl the locking detail, because different concurrency
  // control policy has different locking logic, like Pessimistic txn and
  // Optimistic txn.
  virtual Status TryLock(const std::string& key, bool exclusive,
                         int64_t timeout_time_ms) = 0;

  // this func only invoked in the ctor, and derived class's overridens of this
  // func only need to do their own member variables initialization jobs and
  // invokeed in their own ctor(if needed)
  virtual void Initialize(TransactionStore* txn_store,
                          const WriteOptions& write_options,
                          const TransactionOptions& txn_options);

  // this func will be invoked when a txn finished, and it needs to cleanup all
  // txn's helding resources, so when derived classes override this func, they
  // need to cleanup their own specific resources firstly and then invoke their
  // direct parent class's cleanup func explicitly, like Parent::Clear();
  // assume we have classes as follow for example:
  //    class Parant;
  //    class Child : public Parent;     // Child derived from Parant
  //    class GrandChild : public Child; // GrandChild derived from Child
  // then each class's Clear() impl will be:
  //    Parant::Clear() {
  //      // do current class's own cleanup
  //      ...
  //    }
  //
  //    Child::Clear() {
  //      // first do current class's own cleanup
  //      ...
  //      // then invoke direct parent's Clear()
  //      Parant::Clear();
  //    }
  //
  //    GrandChild::Clear() {
  //      // first do current class's own cleanup
  //      ...
  //      // then invoke direct parent's Clear()
  //      Child::Clear();
  //    }
  // please stick to this cleanup principle
  virtual void Clear() {
    staging_write_->Clear();
    txn_lock_tracker_->Clear();
  }

  StagingWrite* GetStagingWrite() const {
    assert(staging_write_.get() != nullptr);
    return staging_write_.get();
  }

  TxnLockTracker* GetTxnLockTracker() const {
    assert(txn_lock_tracker_.get() != nullptr);
    return txn_lock_tracker_.get();
  }

  bool IsInWritingStage() {
    return txn_state_ == STAGE_WRITING;
  }

  uint64_t TxnId() const {
    return txn_id_;
  }

  uint64_t GetLockTimeOutMs() const {
    return lock_timeout_ms_;
  }

  bool IsExpired() const;

  bool IsReusable() const {
    // when NumTrackedKeys() == 0, we have:
    // 1. txn started, but hasn't successfully performed any writes yet, or
    // 2. txn has successfully performed Commit() or Rollback()
    // in any of these states, we can reuse the txn
    return txn_lock_tracker_->NumTrackedKeys() == 0;
  }

  MVCCTxnStore* txn_store_;
  std::atomic<TransactionState> txn_state_;

  WriteOptions write_options_;
  std::unique_ptr<StagingWrite> staging_write_ = nullptr;
  std::unique_ptr<TxnLockTracker> txn_lock_tracker_ = nullptr;

  uint64_t txn_id_ = 0;

  uint64_t lock_timeout_ms_;
  uint64_t txn_started_ts_us_;
  uint64_t txn_expired_ts_us_;  // txn_expired_ts_us_==0, means txn won't expire
};

}   // namespace COMPOSITE_STORE_NAMESPACE
