#pragma once

#include <vector>
#include <string>
#include <stdint.h>

#include "status.h"
#include "store_traits.h"

namespace MULTI_VERSIONS_NAMESPACE {

class Snapshot;

class Version {
 public:
  virtual ~Version() {}
  
  virtual void IncreaseBy(uint32_t count) = 0;
  virtual void IncreaseByOne() = 0;
  virtual void DuplicateFrom(const Version& src) = 0;
  virtual int CompareWith(const Version& rhs) const = 0;

  virtual void EncodeTo(std::string* dest) const = 0;
  virtual void DecodeFrom(const std::string& input) = 0;
};

class MultiVersionsManager {
 public:
  // No copying allowed
  MultiVersionsManager(const MultiVersionsManager&) = delete;
  MultiVersionsManager& operator=(const MultiVersionsManager&) = delete;

  MultiVersionsManager() {}
  virtual ~MultiVersionsManager() {}

  virtual void Initialize(const Version& orig) = 0;
  // factory func
  virtual Version* CreateVersion() const = 0;
  virtual Version* AllocateVersion(uint32_t count,
                                   Version* reused = nullptr) = 0;
  virtual void BeginPrepareVersions(
      const Version& prepared_uncommitted_started,
      uint32_t num_prepared_uncommitteds) = 0;
  virtual void EndPrepareVersions(const Version& end_uncommitted) = 0;

  virtual void BeginCommitVersions(
      const Version& prepared_uncommitted_started,
      const Version& committed,
      uint32_t num_prepared_uncommitteds) = 0;
  virtual void EndCommitVersions(
      const Version& prepared_uncommitted_started,
      const Version& committed,
      uint32_t num_prepared_uncommitteds) = 0;

  virtual void BeginRollbackVersions(
      const Version& prepared_uncommitted_started,
      const Version& rollbacked_uncommitted_started,
      const Version& committed,
      uint32_t num_prepared_uncommitteds,
      uint32_t num_rollbacked_uncommitteds) = 0;
  virtual void EndRollbackVersions(
      const Version& prepared_uncommitted_started,
      const Version& rollbacked_uncommitted_started,
      const Version& committed,
      uint32_t num_prepared_uncommitteds,
      uint32_t num_rollbacked_uncommitteds) = 0;

  virtual Version* LatestVisibleVersion(Version* reused = nullptr) const = 0;
  virtual bool IsVersionVisibleToSnapshot(const Version& version,
                                          const Snapshot& snapshot,
                                          bool* snap_exists) const = 0;
  virtual const Version& VersionLimitsMax() const = 0;
  virtual const Version& VersionLimitsMin() const = 0;

  virtual void MaybeCleanupVersionsWhenFails() {}       // Todo: 实现之

  virtual void TEST_Crash() {}
};

class Snapshot {
 public:
  // No copying allowed
  Snapshot(const Snapshot&) = delete;
  Snapshot& operator=(const Snapshot&) = delete;

  Snapshot() {}
  virtual ~Snapshot() {}

  // caller own the returned version
  virtual const Version* MaxVersionInSnapshot(
      Version* old_version = nullptr) const = 0;
};

class SnapshotManager {
 public:
  // No copying allowed
  SnapshotManager(const SnapshotManager&) = delete;
  SnapshotManager& operator=(const SnapshotManager&) = delete;

  SnapshotManager() {}
  virtual ~SnapshotManager() {}
  // factory func
  virtual Snapshot* CreateSnapshot() const = 0;
  // caller own the returned snapshot
  virtual const Snapshot* LatestReadView(Snapshot* reused = nullptr) = 0;
  // the returned snapshot must be released through ReleaseSnapshot()
  virtual const Snapshot* TakeSnapshot() = 0;
  virtual void ReleaseSnapshot(const Snapshot* snapshot) = 0;

  virtual bool IsEmpty() const = 0;
  virtual uint32_t NumLivingSnapshot() const = 0;
  virtual void GetAllLivingSnapshots(
      std::vector<const Snapshot*>& snapshots) const = 0;
};

// Factory function
class MultiVersionsManagerFactory {
 public:
  virtual ~MultiVersionsManagerFactory() {}

  virtual MultiVersionsManager* CreateMultiVersionsManager() const = 0;
  virtual SnapshotManager* CreateSnapshotManager(
      MultiVersionsManager* multi_versions_manager) const = 0;
};

class WriteCommittedMultiVersionsManagerFactory :
    public MultiVersionsManagerFactory {
 public:
  WriteCommittedMultiVersionsManagerFactory(
      bool enable_two_write_queues = false)
      : enable_two_write_queues_(enable_two_write_queues) {}
  ~WriteCommittedMultiVersionsManagerFactory() {}

  virtual MultiVersionsManager* CreateMultiVersionsManager() const override;
  virtual SnapshotManager* CreateSnapshotManager(
      MultiVersionsManager* multi_versions_manager) const override;
 private:
  bool enable_two_write_queues_;
};

class WritePreparedMultiVersionsManagerFactory :
    public MultiVersionsManagerFactory {
 public:
  WritePreparedMultiVersionsManagerFactory(
      const CommitTableOptions& options = CommitTableOptions(),
      bool enable_two_write_queues = false)
      : commit_table_options_(options),
        enable_two_write_queues_(enable_two_write_queues) {}
  ~WritePreparedMultiVersionsManagerFactory() {}

  virtual MultiVersionsManager* CreateMultiVersionsManager() const override;
  virtual SnapshotManager* CreateSnapshotManager(
      MultiVersionsManager* multi_versions_manager) const override;
 private:
  CommitTableOptions commit_table_options_;
  bool enable_two_write_queues_;
};

// Wrapper of WriteCommittedMultiVersionsManagerFactory
class EmptyMultiVersionsManagerFactory : public MultiVersionsManagerFactory {
 public:
  ~EmptyMultiVersionsManagerFactory() {}

  virtual MultiVersionsManager* CreateMultiVersionsManager() const override;
  virtual SnapshotManager* CreateSnapshotManager(
      MultiVersionsManager* multi_versions_manager) const override;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
