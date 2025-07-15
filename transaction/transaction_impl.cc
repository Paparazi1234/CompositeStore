#include "mvcc_transaction/pessimistic_transaction/write_committed_transaction.h"
#include "mvcc_transaction/pessimistic_transaction/write_prepared_transaction.h"

#include "util/cast_util.h"

namespace MULTI_VERSIONS_NAMESPACE {

Transaction* WriteCommittedTransactionFactory::CreateTransaction(
    const WriteOptions& write_options, 
    const TransactionOptions& txn_options,
    TransactionStore* txn_store,
    Transaction* reused) {
  if (reused) {
    WriteCommittedTransaction* txn_impl =
        static_cast_with_check<WriteCommittedTransaction>(reused);
    txn_impl->Reinitialize(txn_store, write_options, txn_options);
    return txn_impl;
  } else {
    return new WriteCommittedTransaction(txn_store,
                                         write_options,
                                         txn_options);
  }
}

Transaction* WritePreparedTransactionFactory::CreateTransaction(
    const WriteOptions& write_options, 
    const TransactionOptions& txn_options,
    TransactionStore* txn_store,
    Transaction* reused) {
  if (reused) {
    WritePreparedTransaction* txn_impl =
        static_cast_with_check<WritePreparedTransaction>(reused);
    txn_impl->Reinitialize(txn_store, write_options, txn_options);
    return txn_impl;
  } else {
    return new WritePreparedTransaction(txn_store,
                                        write_options,
                                        txn_options);
  }
}

}   // namespace MULTI_VERSIONS_NAMESPACE
