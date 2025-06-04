#pragma once

#include <vector>
#include <string>
#include <stdint.h>

#include "status.h"

namespace MULTI_VERSIONS_NAMESPACE {

class Snapshot;

class Version {
 public:
  virtual ~Version() {}
  
  virtual void IncreaseBy(uint32_t count) = 0;
  virtual void IncreaseByOne() = 0;
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
  virtual void PrepareVersion(const Version& version) = 0;
  virtual void PrepareVersion(const Version& base, uint32_t count) = 0;
  virtual void CommitVersion(const Version& version) = 0;
  virtual void CommitVersion(const Version& base, uint32_t count) = 0;
  virtual void RollbackVersion(const Version& version) = 0;
  virtual void RollbackVersion(const Version& base, uint32_t count) = 0;
  virtual Version* MiniUncommittedVersion(Version* reused = nullptr) const = 0;
  virtual Version* LatestVisibleVersion(Version* reused = nullptr) const = 0;
  virtual bool IsVersionVisibleToSnapshot(
      const Version& version, const Snapshot& snapshot) const = 0;
};

class Snapshot {
 public:
  // No copying allowed
  Snapshot(const Snapshot&) = delete;
  Snapshot& operator=(const Snapshot&) = delete;

  Snapshot() {}
  virtual ~Snapshot() {}

  // caller own the returned version
  virtual const Version* MaxVersionInSnapshot() const = 0;
};

class SnapshotManager {
 public:
  // No copying allowed
  SnapshotManager(const SnapshotManager&) = delete;
  SnapshotManager& operator=(const SnapshotManager&) = delete;

  SnapshotManager() {}
  virtual ~SnapshotManager() {}
  // caller own the returned snapshot
  virtual const Snapshot* LatestReadView() = 0;
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
  ~WriteCommittedMultiVersionsManagerFactory() {}

  virtual MultiVersionsManager* CreateMultiVersionsManager() const override;
  virtual SnapshotManager* CreateSnapshotManager(
      MultiVersionsManager* multi_versions_manager) const override;
};

class WritePreparedMultiVersionsManagerFactory :
    public MultiVersionsManagerFactory {
 public:
  ~WritePreparedMultiVersionsManagerFactory() {}

  virtual MultiVersionsManager* CreateMultiVersionsManager() const override;
  virtual SnapshotManager* CreateSnapshotManager(
      MultiVersionsManager* multi_versions_manager) const override;
};

// Wrapper of WriteCommittedMultiVersionsManagerFactory
class EmptyMultiVersionsManagerFactory : public MultiVersionsManagerFactory {
 public:
  ~EmptyMultiVersionsManagerFactory() {}

  virtual MultiVersionsManager* CreateMultiVersionsManager() const override {
    return factory_.CreateMultiVersionsManager();
  }

  virtual SnapshotManager* CreateSnapshotManager(
      MultiVersionsManager* multi_versions_manager) const  {
    return factory_.CreateSnapshotManager(multi_versions_manager);
  }

 private:
  WriteCommittedMultiVersionsManagerFactory factory_;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
