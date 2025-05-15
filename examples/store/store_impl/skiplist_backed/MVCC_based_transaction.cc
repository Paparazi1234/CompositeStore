#include "MVCC_based_transaction.h"

namespace MULTI_VERSIONS_NAMESPACE {

MVCCBasedTransaction::MVCCBasedTransaction() {

}

Status MVCCBasedTransaction::Put(const std::string& key,
                                 const std::string& value) {

}

Status MVCCBasedTransaction::Delete(const std::string& key) {
  
}

Status MVCCBasedTransaction::Get(const std::string& key, std::string* value) {
  
}

Status MVCCBasedTransaction::Prepare() {
  
}

Status MVCCBasedTransaction::Commit() {
  
}

Status MVCCBasedTransaction::Rollback() {
  
}

void MVCCBasedTransaction::SetSnapshot() {
  
}

}   // namespace MULTI_VERSIONS_NAMESPACE
