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
    EmptyTxnLockManagerFactory txn_lock_mgr_factory;
    SkipListBackedMVCCWriteBufferFactory mvcc_write_buffer_factory;
    switch (store_traits.txn_store_write_policy) {
      case TxnStoreWritePolicy::kWriteCommitted:
        txn_store =
            new WriteCommittedTxnStore(
                store_options,
                txn_store_options,
                WriteCommittedMultiVersionsManagerFactory(
                    store_options.enable_two_write_queues),
                txn_lock_mgr_factory,
                new WriteCommittedTransactionFactory(),
                new OrderedMapBackedStagingWriteFactory(),
                mvcc_write_buffer_factory);
        break;
      case TxnStoreWritePolicy::kWritePrepared:
        txn_store =
            new WritePreparedTxnStore(
                store_options,
                txn_store_options,
                WritePreparedMultiVersionsManagerFactory(
                    store_traits.commit_table_options,
                    store_options.enable_two_write_queues),
                txn_lock_mgr_factory,
                new WritePreparedTransactionFactory(),
                new OrderedMapBackedStagingWriteFactory(),
                mvcc_write_buffer_factory);
        PostInitializationForWPTxnStore(
            static_cast_with_check<WritePreparedTxnStore>(txn_store));
        break;
      default:
        txn_store = nullptr;
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
