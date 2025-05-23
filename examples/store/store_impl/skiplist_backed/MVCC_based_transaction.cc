#include "MVCC_based_transaction.h"

namespace MULTI_VERSIONS_NAMESPACE {

Status MVCCBasedTransaction::TryLock(const std::string& key) {
  return transaction_store_->TryLock(key);
}

void MVCCBasedTransaction::UnLock(const std::string& key) {
  transaction_store_->TryLock(key);
}

Status MVCCBasedTransaction::Put(const std::string& key,
                                 const std::string& value) {
  Status s = TryLock(key);
  if (s.IsOK()) {
    write_batch_.Put(key, value);
  }
  return s;
}

Status MVCCBasedTransaction::Delete(const std::string& key) {
  Status s = TryLock(key);
  if (s.IsOK()) {
    write_batch_.Delete(key);
  }
  return s;
}

// first get from transaction self buffered writes, if not found then get from
// store
Status MVCCBasedTransaction::Get(const ReadOptions& read_options,
                                const std::string& key, std::string* value) {
  assert(value);
  value->clear();
  WriteBatch::GetReault result = write_batch_.Get(key, value);
  if (result == WriteBatch::GetReault::kFound) {
    return Status::OK();
  } else if (result == WriteBatch::GetReault::kDeleted) {
    return Status::NotFound();
  } else {
    assert(result == WriteBatch::GetReault::kNotFound);
    return transaction_store_->Get(read_options, key, value);
  }
}

void MVCCBasedTransaction::SetSnapshot() {

}

Status WriteCommittedTransaction::Prepare() {
  return Status::OK();
}

Status WriteCommittedTransaction::Commit() {
  return Status::OK();
}

Status WriteCommittedTransaction::Rollback() {
  return Status::OK();
}

}   // namespace MULTI_VERSIONS_NAMESPACE
