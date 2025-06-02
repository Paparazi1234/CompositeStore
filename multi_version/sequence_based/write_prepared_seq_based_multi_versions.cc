#include "write_prepared_seq_based_multi_versions.h"

#include <assert.h>

#include "seq_based_snapshot.h"

namespace MULTI_VERSIONS_NAMESPACE {

void WritePreparedSeqBasedMultiVersionsManager::PrepareVersion(
    const Version& version) {

}

void WritePreparedSeqBasedMultiVersionsManager::PrepareVersion(
    const Version& base, uint32_t count) {

}

void WritePreparedSeqBasedMultiVersionsManager::CommitVersion(
    const Version& version) {

}

void WritePreparedSeqBasedMultiVersionsManager::CommitVersion(
    const Version& base, uint32_t count) {

}

void WritePreparedSeqBasedMultiVersionsManager::RollbackVersion(
    const Version& version) {

}
void WritePreparedSeqBasedMultiVersionsManager::RollbackVersion(
    const Version& base, uint32_t count) {

}

Version* WritePreparedSeqBasedMultiVersionsManager::
    MiniUncommittedVersion(Version* reused) const {
  return new SeqBasedVersion(0);
}

bool WritePreparedSeqBasedMultiVersionsManager::IsVersionVisibleToSnapshot(
    const Version& version, const Snapshot& snapshot) const {
  return true;
}

bool CommitCache::GetCommitEntry(const uint64_t indexed_seq,
                                 CommitEntry64b* entry_64b,
                                 CommitEntry* entry) const {
  *entry_64b = commit_cache_[static_cast<size_t>(indexed_seq)].load(
      std::memory_order_acquire);
  bool valid = entry_64b->Parse(indexed_seq, entry, FORMAT);
  return valid;
}

bool CommitCache::ExchangeCommitEntry(const uint64_t indexed_seq,
                                      CommitEntry64b& expected_entry_64b,
                                      const CommitEntry& new_entry) {
  auto& atomic_entry = commit_cache_[static_cast<size_t>(indexed_seq)];
  CommitEntry64b new_entry_64b(new_entry, FORMAT);
  bool succ = atomic_entry.compare_exchange_strong(
      expected_entry_64b, new_entry_64b, std::memory_order_acq_rel,
      std::memory_order_acquire);
  return succ;
}

}   // namespace MULTI_VERSIONS_NAMESPACE
