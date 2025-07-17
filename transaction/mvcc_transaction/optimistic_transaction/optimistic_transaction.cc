#include "optimistic_transaction.h"

namespace COMPOSITE_STORE_NAMESPACE {

Status OptimisticTransaction::Prepare() {
  return Status::OK();
}

Status OptimisticTransaction::Commit() {
  return Status::OK();
}

Status OptimisticTransaction::Rollback() {
  return Status::OK();
}

}   // namespace COMPOSITE_STORE_NAMESPACE
