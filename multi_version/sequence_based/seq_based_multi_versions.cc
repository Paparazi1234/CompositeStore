#include "seq_based_multi_versions.h"

#include <assert.h>

#include "seq_based_snapshot.h"

namespace MULTI_VERSIONS_NAMESPACE {

void SeqBasedVersion::EncodeTo(std::string* dest) const {
  *dest = std::to_string(rep_);
}

void SeqBasedVersion::DecodeFrom(const std::string& input) {
  rep_ = std::stoull(input);
}

// descending order by version
int SeqBasedVersion::CompareWith(const Version& rhs) {
  const SeqBasedVersion* version_rhs =
      reinterpret_cast<const SeqBasedVersion*>(&rhs);
  if (rep_ != version_rhs->Seq()) {
    if (rep_ < version_rhs->Seq()) {
      return +1;
    }
    return -1;
  }
  return 0;
}

void SeqBasedMultiVersionsManager::Initialize(const Version& orig) {
  const SeqBasedVersion* orig_version =
      reinterpret_cast<const SeqBasedVersion*>(&orig);
  seq_.store(orig_version->Seq(), std::memory_order_seq_cst);
}

Version* SeqBasedMultiVersionsManager::CreateVersion() const {
  return new SeqBasedVersion();
}

Version* SeqBasedMultiVersionsManager::ConstructVersion(
    const Version& base, size_t i, Version* reused) const {
  const SeqBasedVersion* base_version =
    reinterpret_cast<const SeqBasedVersion*>(&base);
  if (reused == nullptr) {
    return new SeqBasedVersion(base_version->Seq() + i);
  }
  SeqBasedVersion* reused_version =
      reinterpret_cast<SeqBasedVersion*>(reused);
  reused_version->SetSeq(base_version->Seq() + i);
  return reused_version;
}

void SeqBasedMultiVersionsManager::AdvanceVersionBy(size_t count) {
  seq_.fetch_add(count, std::memory_order_seq_cst);
}

Version* SeqBasedMultiVersionsManager::LatestVisibleVersion() const {
  uint64_t latest_version = seq_.load(std::memory_order_acquire);
  return new SeqBasedVersion(latest_version);
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
  const SeqBasedVersion* version =
      reinterpret_cast<const SeqBasedVersion*>(&v);
  assert(version->Seq() >= seq_);
  seq_.store(version->Seq(), std::memory_order_seq_cst);
}

void WriteCommittedSeqBasedMultiVersionsManager::CommitVersion(
    const Version& base, size_t count) {
  const SeqBasedVersion* base_version =
      reinterpret_cast<const SeqBasedVersion*>(&base);
  uint64_t end_version_seq = base_version->Seq() + count;
  assert(end_version_seq >= seq_);
  seq_.store(end_version_seq, std::memory_order_seq_cst);
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

Version* WriteCommittedSeqBasedMultiVersionsManager::
    MiniUncommittedVersion() const {
  uint64_t latest_version = seq_.load(std::memory_order_acquire);
  return new SeqBasedVersion(latest_version + 1);
}

bool WriteCommittedSeqBasedMultiVersionsManager::IsVersionVisibleToSnapshot(
    const Version& v, const Snapshot& s) const {
  const SeqBasedVersion* version = reinterpret_cast<const SeqBasedVersion*>(&v);
  const SeqBasedSnapshot* snapshot =
      reinterpret_cast<const SeqBasedSnapshot*>(&s);
  return version->Seq() <= snapshot->Seq();
}

}   // namespace MULTI_VERSIONS_NAMESPACE
