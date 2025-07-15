#pragma once

#include "multi_version/sequence_based/seq_based_multi_versions.h"

namespace MULTI_VERSIONS_NAMESPACE {

class WriteCommittedMultiVersionsManager : public SeqBasedMultiVersionsManager {
 public:
  // No copying allowed
  WriteCommittedMultiVersionsManager(
      const WriteCommittedMultiVersionsManager&) = delete;
  WriteCommittedMultiVersionsManager& operator=(
      const WriteCommittedMultiVersionsManager&) = delete;

  WriteCommittedMultiVersionsManager(bool enable_two_write_queues = false)
      : SeqBasedMultiVersionsManager(
          MaxReadableVersionRep(enable_two_write_queues),
          MaxVisibleVersionRep(enable_two_write_queues)) {
    assert(std::addressof(max_readable_seq_) ==
           std::addressof(max_visible_seq_));
    assert(std::addressof(max_readable_seq_) == &max_committed_rep_);
    assert(std::addressof(max_visible_seq_) == &max_committed_rep_);
  }
  ~WriteCommittedMultiVersionsManager() {}

  virtual void BeginPrepareVersions(
      const Version& /*prepared_uncommitted_started*/,
      uint32_t /*num_prepared_uncommitteds*/) override;
  virtual void EndPrepareVersions(const Version& /*end_uncommitted*/) override;

  virtual void BeginCommitVersions(
      const Version& /*prepared_uncommitted_started*/,
      const Version& /*committed*/,
      uint32_t /*num_prepared_uncommitteds*/) override;
  virtual void EndCommitVersions(
      const Version& /*prepared_uncommitted_started*/,
      const Version& committed,
      uint32_t /*num_prepared_uncommitteds*/) override;

  virtual void BeginRollbackVersions(
      const Version& /*prepared_uncommitted_started*/,
      const Version& /*rollbacked_uncommitted_started*/,
      const Version& /*committed*/,
      uint32_t /*num_prepared_uncommitteds*/,
      uint32_t /*num_rollbacked_uncommitteds*/) override;
  virtual void EndRollbackVersions(
      const Version& /*prepared_uncommitted_started*/,
      const Version& /*rollbacked_uncommitted_started*/,
      const Version& /*committed*/,
      uint32_t /*num_prepared_uncommitteds*/,
      uint32_t /*num_rollbacked_uncommitteds*/) override;
  virtual bool IsVersionVisibleToSnapshot(const Version& version,
                                          const Snapshot& snapshot,
                                          bool* snap_exists) const override;

 private:
  virtual std::atomic<uint64_t>& MaxReadableVersionRep(
      bool /*enable_two_write_queues*/) override {
    return max_committed_rep_;
  }

  virtual std::atomic<uint64_t>& MaxVisibleVersionRep(
      bool /*enable_two_write_queues*/) override {
    return max_committed_rep_;
  }

  std::atomic<uint64_t> max_committed_rep_ = {0};
};

}   // namespace MULTI_VERSIONS_NAMESPACE
