#include "mvcc_store.h"

#include "transaction/mvcc_transaction/pessimistic_transaction/write_committed_transaction.h"

namespace MULTI_VERSIONS_NAMESPACE {

namespace {
// just a wrapper of WriteCommittedMultiVersionsManagerFactory and
// enable_two_write_queues == false
class EmptyMultiVersionsManagerFactory :
    public WriteCommittedMultiVersionsManagerFactory {
 public:
  EmptyMultiVersionsManagerFactory() {}
  ~EmptyMultiVersionsManagerFactory() {}
};

// just a wrapper of WCTxnMaintainVersionsCB
class EmptyMaintainVersionsCallbacks : public WCTxnMaintainVersionsCB {
 public:
  EmptyMaintainVersionsCallbacks(MVCCTxnStore* store)
      : WCTxnMaintainVersionsCB(store) {}
  ~EmptyMaintainVersionsCallbacks() {}
};
}   // anonymous namespace

MVCCStore::MVCCStore(const StoreOptions& store_options)
    : MVCCTxnStore(store_options,
                   TransactionStoreOptions(),
                   EmptyMultiVersionsManagerFactory(),
                   EmptyTxnLockManagerFactory(),
                   nullptr,
                   new OrderedMapBackedStagingWriteFactory(),
                   SkipListBackedMVCCWriteBufferFactory()) {}

Status MVCCStore::Put(const WriteOptions& write_options,
                      const std::string& key,
                      const std::string& value) {
  std::unique_ptr<StagingWrite>
      staging_write(GetStagingWriteFactory()->CreateStagingWrite());
  staging_write->Put(key, value);
  EmptyMaintainVersionsCallbacks empty_maintain_versions_cb(this);
  return CommitStagingWrite(write_options,
                            staging_write.get(),
                            empty_maintain_versions_cb,
                            GetCommitQueue());
}

Status MVCCStore::Delete(const WriteOptions& write_options,
                         const std::string& key) {
  std::unique_ptr<StagingWrite>
      staging_write(GetStagingWriteFactory()->CreateStagingWrite());
  staging_write->Delete(key);
  EmptyMaintainVersionsCallbacks empty_maintain_versions_cb(this);
  return CommitStagingWrite(write_options,
                            staging_write.get(),
                            empty_maintain_versions_cb,
                            GetCommitQueue());
}

Status MVCCStore::Get(const ReadOptions& read_options,
                      const std::string& key,
                      std::string* value) {
  return GetInternal(read_options, key, value);
}

Transaction* MVCCStore::BeginTransaction(
    const WriteOptions& /*write_options*/,
    const TransactionOptions& /*txn_options*/,
    Transaction* /*reused*/) {
  assert(false);
  return nullptr;
}

const Snapshot* MVCCStore::TakeSnapshot() {
  assert(false);
  return nullptr;
}

void MVCCStore::ReleaseSnapshot(const Snapshot* /*snapshot*/) {
  assert(false);
}

}   // namespace MULTI_VERSIONS_NAMESPACE
