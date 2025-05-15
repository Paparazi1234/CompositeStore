#include "skliplist_backed_in_memory_txn_store.h"

namespace MULTI_VERSIONS_NAMESPACE {

SkipListBackedInMemoryTxnStore::SkipListBackedInMemoryTxnStore() {

}

Status SkipListBackedInMemoryTxnStore::Put(const WriteOptions& write_options,
                                           const std::string& key,
                                           const std::string& value) {

}

Status SkipListBackedInMemoryTxnStore::Delete(
    const WriteOptions& write_options, const std::string& key) {

}

Status SkipListBackedInMemoryTxnStore::Get(const ReadOptions& read_options,
                                           const std::string& key,
                                           std::string* value) {

}

Transaction* SkipListBackedInMemoryTxnStore::BeginTransaction(
    const TransactionOptions& txn_options, const WriteOptions& write_options) {

}

const Snapshot* SkipListBackedInMemoryTxnStore::TakeSnapshot() {

}

}   // namespace MULTI_VERSIONS_NAMESPACE
