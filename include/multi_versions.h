#pragma once

#include <string>

#include "status.h"

namespace MULTI_VERSIONS_NAMESPACE {

class Snapshot;

class Version {
 public:
  virtual ~Version() {}

  virtual void EncodeTo(std::string* dest) const = 0;
  virtual void DecodeFrom(const std::string& input) = 0;
  
  virtual int CompareWith(const Version& rhs) = 0;
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
  virtual Version* ConstructVersion(
      const Version& base, size_t i, Version* reused = nullptr) const = 0;
  virtual void AdvanceVersionBy(size_t count) = 0;
  virtual void PrepareVersion(const Version& v) = 0;
  virtual void PrepareVersion(const Version& base, size_t count) = 0;
  virtual void CommitVersion(const Version& v) = 0;
  virtual void CommitVersion(const Version& base, size_t count) = 0;
  virtual void RollbackVersion(const Version& v) = 0;
  virtual void RollbackVersion(const Version& base, size_t count) = 0;
  virtual Version* MiniUncommittedVersion() const = 0;
  virtual Version* LatestVisibleVersion() const = 0;
  virtual bool IsVersionVisibleToSnapshot(
    const Version& v, const Snapshot& s) const = 0;
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
  virtual void GetAllLivingSnapshot() = 0;
};

// Factory function
class MultiVersionsManagerFactory {
 public:
  virtual ~MultiVersionsManagerFactory() {}

  virtual MultiVersionsManager* CreateMultiVersionsManager() const = 0;
  virtual SnapshotManager* CreateSnapshotManager(
      MultiVersionsManager* multi_versions_manager) const = 0;
};

class WCSeqBasedMultiVersionsManagerFactory :
    public MultiVersionsManagerFactory {
 public:
  ~WCSeqBasedMultiVersionsManagerFactory() {}

  virtual MultiVersionsManager* CreateMultiVersionsManager() const override;
  virtual SnapshotManager* CreateSnapshotManager(
      MultiVersionsManager* multi_versions_manager) const override;
};

class WPSeqBasedMultiVersionsManagerFactory :
    public MultiVersionsManagerFactory {
 public:
  ~WPSeqBasedMultiVersionsManagerFactory() {}

  virtual MultiVersionsManager* CreateMultiVersionsManager() const override;
  virtual SnapshotManager* CreateSnapshotManager(
      MultiVersionsManager* multi_versions_manager) const override;
};

// Wrapper of WCSeqBasedMultiVersionsManagerFactory
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
  WCSeqBasedMultiVersionsManagerFactory factory_;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
