#include "mvcc_txn_store/pessimistic_txn_store/write_committed_txn_store.h"
#include "mvcc_txn_store/pessimistic_txn_store/write_prepared_txn_store.h"
#include "multi_versions/sequence_based/write_prepared/write_prepared_multi_versions.h"
#include "multi_versions/sequence_based/write_prepared/write_prepared_snapshot.h"

namespace COMPOSITE_STORE_NAMESPACE {

class TxnStoreFactory {
 public:
  virtual ~TxnStoreFactory() {}

  virtual TransactionStore* CreateTxnStore(
      const StoreOptions& store_options,
      const TransactionStoreOptions& txn_store_options,
      const StoreTraits& store_traits) const = 0;
};

namespace {
class WPAdvanceMaxCommittedByOneCallback :
    public AdvanceMaxCommittedByOneCallback {
  public:
  WPAdvanceMaxCommittedByOneCallback(WritePreparedTxnStore* txn_store)
      : txn_store_(txn_store) {}
  ~WPAdvanceMaxCommittedByOneCallback() {}

  void AdvanceLatestVisibleByOne() override {
    WriteOptions write_options;
    TransactionOptions txn_options;
    Transaction* txn = txn_store_->BeginTransaction(write_options,
                                                    txn_options, nullptr);
    // commit(without prepare) an empty staging write will consume one seq
    // commit(with prepare) an empty staging write will consume two seq
    Status s = txn->Commit();
    assert(s.IsOK());
    delete txn;
  }

  private:
  WritePreparedTxnStore* txn_store_;         
};

class WPGetSnapshotsCallback : public GetSnapshotsCallback {
  public:
  WPGetSnapshotsCallback(const WritePreparedSnapshotManager* mgr) : mgr_(mgr) {}
  ~WPGetSnapshotsCallback() {}

  void GetSnapshots(uint64_t max,
                    std::vector<uint64_t>& snapshots) const override {
    mgr_->GetSnapshots(max, snapshots);
  }
  private:
  const WritePreparedSnapshotManager* const mgr_;
};

void PostInitializationForWPTxnStore(WritePreparedTxnStore* txn_store) {
  WritePreparedMultiVersionsManager* multi_version_manager_impl =
      static_cast_with_check<WritePreparedMultiVersionsManager>(
          txn_store->GetMultiVersionsManager());
  WritePreparedSnapshotManager* snapshot_manager_impl =
      static_cast_with_check<WritePreparedSnapshotManager>(
          txn_store->GetSnapshotManager());
  // set AdvanceMaxCommittedByOneCallback to multi versions manager
  multi_version_manager_impl->SetAdvanceMaxCommittedByOneCallback(
      new WPAdvanceMaxCommittedByOneCallback(txn_store));
  // set SnapshotsRetrieveCallback to multi versions manager
  multi_version_manager_impl->SetSnapshotsRetrieveCallback(
      new WPGetSnapshotsCallback(snapshot_manager_impl));
  // set SnapshotCreationCallback to snapshot manager
  snapshot_manager_impl->SetSnapshotCreationCallback(
      multi_version_manager_impl->GetSnapshotCreationCallback());
}
}   // anonymous namespace

class MVCCTxnStoreFactory : public TxnStoreFactory {
 public:
  ~MVCCTxnStoreFactory() {}

  TransactionStore* CreateTxnStore(
      const StoreOptions& store_options,
      const TransactionStoreOptions& txn_store_options,
      const StoreTraits& store_traits) const override {
    MVCCTxnStore* txn_store = nullptr;
    MVCCTxnStoreCreationParam param;

    SkipListBackedMVCCWriteBufferFactory skiplist_backed_write_buffer_factory;
    if (store_traits.write_buffer_backed_type ==
        MVCCWriteBufferBackedType::kSkipListBacked) {
      param.write_buffer_factory = &skiplist_backed_write_buffer_factory;
    } else {
      assert(false);
    }

    if (store_traits.staging_write_type ==
        StagingWriteType::kOrderedMapBacked) {
      param.staging_write_factory = new OrderedMapBackedStagingWriteFactory();
    } else {
      assert(false);
    }

    WriteCommittedMultiVersionsManagerFactory
        wc_mvm_factory(store_options.enable_two_write_queues);
    WritePreparedMultiVersionsManagerFactory
        wp_mvm_factory(store_traits.commit_table_options,
                       store_options.enable_two_write_queues);
    if (store_traits.txn_store_write_policy ==
        TxnStoreWritePolicy::kWriteCommitted) {
      param.mvm_factory = &wc_mvm_factory;
      param.transaction_factory = new WriteCommittedTransactionFactory();
    } else if (store_traits.txn_store_write_policy ==
               TxnStoreWritePolicy::kWritePrepared) {
      param.mvm_factory = &wp_mvm_factory;
      param.transaction_factory = new WritePreparedTransactionFactory();
    } else {
      assert(false);
    }

    EmptyTxnLockManagerFactory empty_txn_lock_mgr_factory;
    if (store_traits.txn_lock_manager_type ==
        TxnLockManagerType::kEmptyTxnLoxkManager) {
      param.lock_manager_factory = &empty_txn_lock_mgr_factory;
    } else {
      assert(false);
    }

    if (store_traits.concurrency_control_Policy ==
        ConcurrencyControlPolicy::kPessimisticConcurrencyControl) {
      if (store_traits.txn_store_write_policy ==
          TxnStoreWritePolicy::kWriteCommitted) {
        txn_store = new WriteCommittedTxnStore(store_options, txn_store_options,
                                               param);
      } else if (store_traits.txn_store_write_policy ==
                 TxnStoreWritePolicy::kWritePrepared) {
        txn_store = new WritePreparedTxnStore(store_options, txn_store_options,
                                              param);
        PostInitializationForWPTxnStore(
            static_cast_with_check<WritePreparedTxnStore>(txn_store));
      } else {
        assert(false);
      }
    } else if (store_traits.concurrency_control_Policy ==
               ConcurrencyControlPolicy::kOptimisticConcurrencyControl) {
      assert(false);
    } else {
      assert(false);
    }
    return txn_store;
  }
};

Status TransactionStore::Open(const StoreOptions& store_options,
                              const TransactionStoreOptions& txn_store_options,
                              const StoreTraits& store_traits,
                              TransactionStore** txn_store_ptr) {
  assert(txn_store_ptr);
  *txn_store_ptr = nullptr;
  Status s;
  switch (store_traits.txn_store_impl_type) {
    case TxnStoreImplType::kDefault:
    case TxnStoreImplType::kMVCC:
      *txn_store_ptr = MVCCTxnStoreFactory().CreateTxnStore(store_options,
                                                            txn_store_options,
                                                            store_traits);
      break;
    default:
      *txn_store_ptr = nullptr;
  }
  
  if (*txn_store_ptr == nullptr) {
    s = Status::InvalidArgument();
  }
  return s;
}

}   // namespace COMPOSITE_STORE_NAMESPACE
