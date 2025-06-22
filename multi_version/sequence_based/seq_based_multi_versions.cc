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

void WriteCommittedMultiVersionsManager::BeginRollbackVersions(
    const Version& /*started_uncommitted*/,
    const Version& /*rollbacked_uncommitted*/,
    const Version& /*committed*/,
    uint32_t /*num_uncommitteds*/,
    uint32_t /*num_rollbacked_uncommitteds*/) {
  // since write committed txn doesn't insert data to underlying store before
  // Commit(), so there is nothing to rollback
}

void WriteCommittedMultiVersionsManager::EndRollbackVersions(
    const Version& /*started_uncommitted*/,
    const Version& /*rollbacked_uncommitted*/,
    const Version& /*committed*/,
    uint32_t /*num_uncommitteds*/,
    uint32_t /*num_rollbacked_uncommitteds*/) {
  // since write committed txn doesn't insert data to underlying store before
  // Commit(), so there is nothing to rollback
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
  // when Prepare(), we only need to record the uncommitted version in
  // commit_table_
  commit_table_.AddUnCommittedVersion(started_uncommitted_seq,
                                      num_uncommitteds);
}

void WritePreparedMultiVersionsManager::EndPrepareVersions(
    const Version& end_uncommitted) {
  // advance max readable version after insert write buffer
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
  // commit the uncommitted versions to commit_table_ before insert write buffer
  // 1 when commit with prepare, the committed version comes from an internal
  //   empty write batch;
  // 2 when commit without prepare, the committed version comes from the txn's
  //   own write batch
  for (uint32_t i = 0; i < num_uncommitteds; ++i) {
    commit_table_.AddCommittedVersion(started_uncommitted_seq + i,
                                      committed_seq);
  }
}

void WritePreparedMultiVersionsManager::EndCommitVersions(
    const Version& started_uncommitted,
    const Version& committed,
    uint32_t num_uncommitteds) {
  // advance max visible version after add committeds to commit_table_ and
  // insert write buffer
  AdvanceMaxVisibleVersion(committed);
  // do cleanup
  // cleanup the prepared uncommitted versions
  if (num_uncommitteds > 0) {
    // num_uncommitteds > 0: means commit with prepare
    // otherwise, num_uncommitteds == 0: means commit without prepare
    const SeqBasedVersion* started_uncommitted_impl =
      reinterpret_cast<const SeqBasedVersion*>(&started_uncommitted);
    uint64_t started_uncommitted_seq = started_uncommitted_impl->Seq();
    commit_table_.EraseUnCommittedVersion(started_uncommitted_seq,
                                          num_uncommitteds);
  }
}

void WritePreparedMultiVersionsManager::BeginRollbackVersions(
    const Version& started_uncommitted,
    const Version& rollbacked_uncommitted,
    const Version& committed,
    uint32_t num_uncommitteds,
    uint32_t num_rollbacked_uncommitteds) {
  // there must be some prepared uncommitted versions if we get to here
  assert(num_uncommitteds > 0);
  // 1 the rollback write batch(without prepare) consume a version, or
  // 2 the empty write batch for commit rollback write batch(with prepare)
  //   purpose consume a version
  assert(num_rollbacked_uncommitteds == 1);
  const SeqBasedVersion* started_uncommitted_impl =
      reinterpret_cast<const SeqBasedVersion*>(&started_uncommitted);
  const SeqBasedVersion* rollbacked_uncommitted_impl =
      reinterpret_cast<const SeqBasedVersion*>(&rollbacked_uncommitted);
  const SeqBasedVersion* committed_impl =
      reinterpret_cast<const SeqBasedVersion*>(&committed);
  uint64_t started_uncommitted_seq = started_uncommitted_impl->Seq();
  uint64_t rollbacked_uncommitted_seq = rollbacked_uncommitted_impl->Seq();
  uint64_t committed_seq = committed_impl->Seq();
  // first commit the prepared uncommitted versions that we intend to rollback
  // to commit_table_
  for (uint64_t i = 0; i < num_uncommitteds; ++i) {
    commit_table_.AddCommittedVersion(started_uncommitted_seq + i,
                                      committed_seq);
  }
  // then commit the versions of rollback write batch to commit_table_
  // 1 when the rollback write batch went through an internal prepare stage,
  //   the committed version comes from an internal empty write batch;
  // 2 when the rollback write batch didn't go through an internal prepare
  //   stage, the committed version comes from the rollback write batch;
  // the prepared uncommitted versions and the rollback write batch share the
  // same committed version
  for (uint64_t j = 0; j < num_rollbacked_uncommitteds; ++j) {
    commit_table_.AddCommittedVersion(rollbacked_uncommitted_seq + j,
                                      committed_seq);
  }
}

void WritePreparedMultiVersionsManager::EndRollbackVersions(
    const Version& started_uncommitted,
    const Version& rollbacked_uncommitted,
    const Version& committed,
    uint32_t num_uncommitteds,
    uint32_t num_rollbacked_uncommitteds) {
  // there must be some prepared uncommitted versions if we get to here
  assert(num_uncommitteds > 0);
  const SeqBasedVersion* started_uncommitted_impl =
      reinterpret_cast<const SeqBasedVersion*>(&started_uncommitted);
  const SeqBasedVersion* rollbacked_uncommitted_impl =
      reinterpret_cast<const SeqBasedVersion*>(&rollbacked_uncommitted);
  uint64_t started_uncommitted_seq = started_uncommitted_impl->Seq();
  uint64_t rollbacked_uncommitted_seq = rollbacked_uncommitted_impl->Seq();

  // advance max visible version after commit the versions of rollback write
  // batch to commit_table_ and insert the rollback write batch to write buffer
  AdvanceMaxVisibleVersion(committed);
  // do cleanup
  // first cleanup the prepared uncommitted versions
  commit_table_.EraseUnCommittedVersion(started_uncommitted_seq,
                                        num_uncommitteds);
  // then cleanup the uncommitted versions of rollback write batch if the
  // rollback write batch went through a prepare stage
  if (num_rollbacked_uncommitteds > 0) {
    // num_rollbacked_uncommitteds > 0: means the rollback write batch went
    // through a prepare stage
    // otherwise, num_rollbacked_uncommitteds == 0: means the rollback write
    // batch didn't go through a prepare stage
    commit_table_.EraseUnCommittedVersion(rollbacked_uncommitted_seq,
                                          num_rollbacked_uncommitteds);
  }
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
