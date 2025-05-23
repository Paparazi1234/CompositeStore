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

Status MVCCBasedTransaction::Get(const std::string& key, std::string* value) {
  return Status::OK();
}

Status MVCCBasedTransaction::Prepare() {
  return Status::OK();
}

Status MVCCBasedTransaction::Commit() {
  return Status::OK();
}

Status MVCCBasedTransaction::Rollback() {
  return Status::OK();
}

void MVCCBasedTransaction::SetSnapshot() {

}

}   // namespace MULTI_VERSIONS_NAMESPACE
