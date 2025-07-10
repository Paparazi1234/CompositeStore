#include "optimistic_transaction.h"

namespace MULTI_VERSIONS_NAMESPACE {

Status OptimisticTransaction::Prepare() {
  return Status::OK();
}

Status OptimisticTransaction::Commit() {
  return Status::OK();
}

Status OptimisticTransaction::Rollback() {
  return Status::OK();
}

}   // namespace MULTI_VERSIONS_NAMESPACE
