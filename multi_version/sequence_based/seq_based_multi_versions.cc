#include "seq_based_multi_versions.h"

#include <assert.h>

#include "seq_based_snapshot.h"

namespace MULTI_VERSIONS_NAMESPACE {

const SeqBasedVersion SeqBasedMultiVersionsManager::version_limits_min_ =
    kSeqNumberLimitsMin;
const SeqBasedVersion SeqBasedMultiVersionsManager::version_limits_max_ =
    kSeqNumberLimitsMax;

void SeqBasedMultiVersionsManager::Initialize(const Version& orig) {
  const SeqBasedVersion* version_impl =
      reinterpret_cast<const SeqBasedVersion*>(&orig);
  uint64_t seq = version_impl->Seq() + 1;
  seq_allocator_.Initialize(seq);
  max_visible_seq_.store(seq, std::memory_order_seq_cst);
  max_readable_seq_.store(seq, std::memory_order_seq_cst);
}

Version* SeqBasedMultiVersionsManager::AllocateVersion(uint32_t count,
                                                       Version* reused) {
  assert(count > 0);
  uint64_t allocated_started = seq_allocator_.Allocate(count);
  if (reused != nullptr) {
    SeqBasedVersion* version_impl = reinterpret_cast<SeqBasedVersion*>(reused);
    version_impl->SetSeq(allocated_started);
    return reused;
  } else {
    return new SeqBasedVersion(allocated_started);
  }
}

Version* SeqBasedMultiVersionsManager::LatestVisibleVersion(
    Version* reused) const {
  uint64_t latest_visible = max_visible_seq_.load(std::memory_order_seq_cst);
  if (reused != nullptr) {
    SeqBasedVersion* version_impl = reinterpret_cast<SeqBasedVersion*>(reused);
    version_impl->SetSeq(latest_visible);
    return reused;
  } else {
    return new SeqBasedVersion(latest_visible);
  }
}

void WriteCommittedMultiVersionsManager::BeginPrepareVersions(
    const Version& /*started_uncommitted*/, uint32_t /*num_uncommitteds*/) {
}

void WriteCommittedMultiVersionsManager::EndPrepareVersions(
    const Version& /*end_uncommitted*/) {
}

void WriteCommittedMultiVersionsManager::BeginCommitVersions(
    const Version& /*started_uncommitted*/,
    const Version& /*committed*/,
    uint32_t /*num_uncommitteds*/) {
}

void WriteCommittedMultiVersionsManager::EndCommitVersions(
    const Version& /*started_uncommitted*/,
    const Version& committed,
    uint32_t /*num_uncommitteds*/) {
  AdvanceMaxVisibleVersion(committed);
}

bool WriteCommittedMultiVersionsManager::IsVersionVisibleToSnapshot(
    const Version& version, const Snapshot& snapshot, bool* snap_exists) const {
  *snap_exists = true;
  const SeqBasedVersion* version_impl =
      reinterpret_cast<const SeqBasedVersion*>(&version);
  const SeqBasedSnapshot* snapshot_impl =
      reinterpret_cast<const SeqBasedSnapshot*>(&snapshot);
  return version_impl->Seq() <= snapshot_impl->Seq();
}

void WritePreparedMultiVersionsManager::BeginPrepareVersions(
    const Version& started_uncommitted, uint32_t num_uncommitteds) {
  const SeqBasedVersion* started_uncommitted_impl =
      reinterpret_cast<const SeqBasedVersion*>(&started_uncommitted);
  uint64_t started_uncommitted_seq = started_uncommitted_impl->Seq();
  commit_table_.AddUnCommittedVersion(started_uncommitted_seq,
                                      num_uncommitteds);
}

void WritePreparedMultiVersionsManager::EndPrepareVersions(
    const Version& end_uncommitted) {
  AdvanceMaxReadableVersion(end_uncommitted);
}

void WritePreparedMultiVersionsManager::BeginCommitVersions(
    const Version& started_uncommitted,
    const Version& committed,
    uint32_t num_uncommitteds) {
  const SeqBasedVersion* started_uncommitted_impl =
      reinterpret_cast<const SeqBasedVersion*>(&started_uncommitted);
  const SeqBasedVersion* committed_impl =
      reinterpret_cast<const SeqBasedVersion*>(&committed);
  uint64_t started_uncommitted_seq = started_uncommitted_impl->Seq();
  uint64_t committed_seq = committed_impl->Seq();
  for (uint32_t i = 0; i < num_uncommitteds; ++i) {
    commit_table_.AddCommittedVersion(started_uncommitted_seq + i,
                                      committed_seq);
  }
}

void WritePreparedMultiVersionsManager::EndCommitVersions(
    const Version& started_uncommitted,
    const Version& committed,
    uint32_t num_uncommitteds) {
  // advance max_visible_seq_ after add committeds to commit_table_
  AdvanceMaxVisibleVersion(committed);
  // Cleanup uncommitteds after update max_visible_seq_
  if (num_uncommitteds > 0) { // num_uncommitteds > 0: means commit with prepare
    const SeqBasedVersion* started_uncommitted_impl =
      reinterpret_cast<const SeqBasedVersion*>(&started_uncommitted);
    uint64_t started_uncommitted_seq = started_uncommitted_impl->Seq();
    commit_table_.EraseUnCommittedVersion(started_uncommitted_seq,
                                          num_uncommitteds); 
  } // num_uncommitteds == 0: means commit without prepare, and we have no thing
    // to do with commit_table_
}

bool WritePreparedMultiVersionsManager::IsVersionVisibleToSnapshot(
    const Version& version, const Snapshot& snapshot, bool* snap_exists) const {
  const SeqBasedVersion* version_impl =
      reinterpret_cast<const SeqBasedVersion*>(&version);
  const WPSeqBasedSnapshot* snapshot_impl =
      reinterpret_cast<const WPSeqBasedSnapshot*>(&snapshot);
  return commit_table_.IsVersionVisibleToSnapshot(version_impl->Seq(), 
      snapshot_impl->Seq(), snapshot_impl->MiniUnCommitted(), snap_exists);
}

}   // namespace MULTI_VERSIONS_NAMESPACE
