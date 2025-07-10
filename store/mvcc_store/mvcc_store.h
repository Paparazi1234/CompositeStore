#include "transaction_store/mvcc_txn_store/mvcc_txn_store.h"

namespace MULTI_VERSIONS_NAMESPACE {

class MVCCStore : public MVCCTxnStore {
 public:
  // No copying allowed
  MVCCStore(const MVCCStore&) = delete;
  MVCCStore& operator=(const MVCCStore&) = delete;

  MVCCStore(const StoreOptions& store_options);
  virtual ~MVCCStore() {}

  virtual Status Put(const WriteOptions& write_options,
              		   const std::string& key,
										 const std::string& value) override;
  virtual Status Delete(const WriteOptions& write_options,
                				const std::string& key) override;
  virtual Status Get(const ReadOptions& read_options,
             				 const std::string& key,
                     std::string* value) override;

  virtual Transaction* BeginTransaction(
      const WriteOptions& /*write_options*/,
      const TransactionOptions& /*txn_options*/,
      Transaction* /*reused*/) override;
  virtual const Snapshot* TakeSnapshot() override;
  virtual void ReleaseSnapshot(const Snapshot* /*snapshot*/) override;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
