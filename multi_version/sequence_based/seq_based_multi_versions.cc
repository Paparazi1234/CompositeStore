#include "seq_based_multi_versions.h"

#include <assert.h>

#include "seq_based_snapshot.h"

namespace MULTI_VERSIONS_NAMESPACE {

void SeqBasedMultiVersionsManager::Initialize(const Version& orig) {
  const SeqBasedVersion* version_impl =
      reinterpret_cast<const SeqBasedVersion*>(&orig);
  seq_.store(version_impl->Seq(), std::memory_order_seq_cst);
}

Version* SeqBasedMultiVersionsManager::CreateVersion() const {
  return new SeqBasedVersion();
}

Version* SeqBasedMultiVersionsManager::LatestVisibleVersion(
    Version* reused) const {
  uint64_t latest_version = seq_.load(std::memory_order_acquire);
  if (reused != nullptr) {
    SeqBasedVersion* version_impl = reinterpret_cast<SeqBasedVersion*>(reused);
    version_impl->SetSeq(latest_version);
    return reused;
  } else {
    return new SeqBasedVersion(latest_version);
  }
}

void WriteCommittedSeqBasedMultiVersionsManager::PrepareVersion(
    const Version& version) {
  (void)version;
}

void WriteCommittedSeqBasedMultiVersionsManager::PrepareVersion(
    const Version& base, uint32_t count) {
  (void)base;
  (void)count;
}

void WriteCommittedSeqBasedMultiVersionsManager::CommitVersion(
    const Version& version) {
  const SeqBasedVersion* version_impl =
      reinterpret_cast<const SeqBasedVersion*>(&version);
  assert(version_impl->Seq() >= seq_);
  seq_.store(version_impl->Seq(), std::memory_order_seq_cst);
}

void WriteCommittedSeqBasedMultiVersionsManager::CommitVersion(
    const Version& base, uint32_t count) {
  const SeqBasedVersion* version_impl =
      reinterpret_cast<const SeqBasedVersion*>(&base);
  uint64_t end_version_seq = version_impl->Seq() + count;
  assert(end_version_seq >= seq_);
  seq_.store(end_version_seq, std::memory_order_seq_cst);
}

void WriteCommittedSeqBasedMultiVersionsManager::RollbackVersion(
    const Version& version) {
  (void)version;
}
void WriteCommittedSeqBasedMultiVersionsManager::RollbackVersion(
    const Version& base, uint32_t count) {
  (void)base;
  (void)count;
}

// seq 0 is always considered committed
Version* WriteCommittedSeqBasedMultiVersionsManager::
    MiniUncommittedVersion(Version* reused) const {
  uint64_t latest_version = seq_.load(std::memory_order_acquire);
  if (reused != nullptr) {
    SeqBasedVersion* version_impl = reinterpret_cast<SeqBasedVersion*>(reused);
    version_impl->SetSeq(latest_version + 1);
    return reused;
  } else {
    return new SeqBasedVersion(latest_version + 1);
  }
}

bool WriteCommittedSeqBasedMultiVersionsManager::IsVersionVisibleToSnapshot(
    const Version& version, const Snapshot& snapshot) const {
  const SeqBasedVersion* version_impl =
      reinterpret_cast<const SeqBasedVersion*>(&version);
  const SeqBasedSnapshot* snapshot_impl =
      reinterpret_cast<const SeqBasedSnapshot*>(&snapshot);
  return version_impl->Seq() <= snapshot_impl->Seq();
}

}   // namespace MULTI_VERSIONS_NAMESPACE
