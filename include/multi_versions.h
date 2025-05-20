#pragma once

#include <string>

#include "status.h"

namespace MULTI_VERSIONS_NAMESPACE {

class Snapshot;
class SnapshotManager;

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

// Factory function
class MultiVersionsManagerFactory {
 public:
  virtual ~MultiVersionsManagerFactory() {}

  virtual MultiVersionsManager* CreateMultiVersionsManager() = 0;
  virtual SnapshotManager* CreateSnapshotManager(
      MultiVersionsManager* multi_versions_manager) = 0;
};

class WCSeqBasedMultiVersionsManagerFactory :
    public MultiVersionsManagerFactory {
 public:
  ~WCSeqBasedMultiVersionsManagerFactory() {}

  virtual MultiVersionsManager* CreateMultiVersionsManager() override;
  virtual SnapshotManager* CreateSnapshotManager(
      MultiVersionsManager* multi_versions_manager) override;
};

class WPSeqBasedMultiVersionsManagerFactory :
    public MultiVersionsManagerFactory {
 public:
  ~WPSeqBasedMultiVersionsManagerFactory() {}

  virtual MultiVersionsManager* CreateMultiVersionsManager() override;
  virtual SnapshotManager* CreateSnapshotManager(
      MultiVersionsManager* multi_versions_manager) override;
};

// Wrapper of MultiVersionsManagerFactory
class EmptyMultiVersionsManagerFactory : public MultiVersionsManagerFactory {
 public:
  ~EmptyMultiVersionsManagerFactory() {}

  virtual MultiVersionsManager* CreateMultiVersionsManager() override {
    return factory_.CreateMultiVersionsManager();
  }

  virtual SnapshotManager* CreateSnapshotManager(
      MultiVersionsManager* multi_versions_manager) {
    return factory_.CreateSnapshotManager(multi_versions_manager);
  }

 private:
  WCSeqBasedMultiVersionsManagerFactory factory_;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
