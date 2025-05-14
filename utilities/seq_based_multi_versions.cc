#include "seq_based_multi_versions.h"

#include <assert.h>

#include "seq_based_snapshot.h"

namespace MULTI_VERSIONS_NAMESPACE {

void SeqBasedMultiVersionsManager::Initialize(const Version& orig) {
  const SeqBasedVersion* v = static_cast<const SeqBasedVersion*>(&orig);
  seq_.store(v->Seq(), std::memory_order_seq_cst);
}

const Version& SeqBasedMultiVersionsManager::AllocateVersion() {
  uint64_t last_version = seq_.fetch_add(1, std::memory_order_seq_cst);
  return SeqBasedVersion(last_version + 1);
}

void SeqBasedMultiVersionsManager::AdvanceVersionBy(size_t count) {
  seq_.fetch_add(count, std::memory_order_seq_cst);
}

const Version& SeqBasedMultiVersionsManager::LatestVisibleVersion() const {
  uint64_t latest_version = seq_.load(std::memory_order_acquire);
  return SeqBasedVersion(latest_version);
}

void WriteCommittedSeqBasedMultiVersionsManager::PrepareVersion(
    const Version& v) {
  (void)v;
}

void WriteCommittedSeqBasedMultiVersionsManager::PrepareVersion(
    const Version& base, size_t count) {
  (void)base;
  (void)count;
}

void WriteCommittedSeqBasedMultiVersionsManager::CommitVersion(
    const Version& v) {
  const SeqBasedVersion* seqBasedV = static_cast<const SeqBasedVersion*>(&v);
  assert(seqBasedV->Seq() >= seq_);
  seq_.store(seqBasedV->Seq(), std::memory_order_seq_cst);
}

void WriteCommittedSeqBasedMultiVersionsManager::CommitVersion(
    const Version& base, size_t count) {
  const SeqBasedVersion* startV = static_cast<const SeqBasedVersion*>(&base);
  uint64_t endV = startV->Seq() + count;
  assert(endV >= seq_);
  seq_.store(endV, std::memory_order_seq_cst);
}

void WriteCommittedSeqBasedMultiVersionsManager::RollbackVersion(
    const Version& v) {
  (void)v;
}
void WriteCommittedSeqBasedMultiVersionsManager::RollbackVersion(
    const Version& base, size_t count) {
  (void)base;
  (void)count;
}

const Version& WriteCommittedSeqBasedMultiVersionsManager::
    MiniUncommittedVersion() const {
  uint64_t latest_version = seq_.load(std::memory_order_acquire);
  return SeqBasedVersion(latest_version + 1);
}

bool WriteCommittedSeqBasedMultiVersionsManager::IsVersionVisibleToSnapshot(
    const Version& v, const Snapshot& s) const {
  const SeqBasedVersion* seqBasedV = static_cast<const SeqBasedVersion*>(&v);
  const SeqBasedSnapshot* seqBasedS = static_cast<const SeqBasedSnapshot*>(&s);
  return seqBasedV->Seq() <= seqBasedS->Seq();
}

}   // namespace MULTI_VERSIONS_NAMESPACE
