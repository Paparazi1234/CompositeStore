#include "MVCC_based_transaction.h"

namespace MULTI_VERSIONS_NAMESPACE {

MVCCBasedTransaction::MVCCBasedTransaction() {

}

Status MVCCBasedTransaction::Put(const std::string& key,
                                 const std::string& value) {
  return Status::OK();
}

Status MVCCBasedTransaction::Delete(const std::string& key) {
  return Status::OK();
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
