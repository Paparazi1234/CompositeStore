#pragma once

#include "multi_versions_namespace.h"
#include "snapshot.h"

namespace MULTI_VERSIONS_NAMESPACE {

class Version {
 public:
  virtual ~Version() {}
};

class MultiVersionsManager {
 public:
  // No copying allowed
  MultiVersionsManager(const MultiVersionsManager&) = delete;
  MultiVersionsManager& operator=(const MultiVersionsManager&) = delete;

  virtual ~MultiVersionsManager() {}

  virtual void Initialize(const Version& orig) = 0;
  virtual const Version& AllocateVersion() = 0;
  virtual void AdvanceVersionBy(size_t count) = 0;
  virtual void PrepareVersion(const Version& v) = 0;
  virtual void PrepareVersion(const Version& base, size_t count) = 0;
  virtual void CommitVersion(const Version& v) = 0;
  virtual void CommitVersion(const Version& base, size_t count) = 0;
  virtual void RollbackVersion(const Version& v) = 0;
  virtual void RollbackVersion(const Version& base, size_t count) = 0;
  virtual const Version& MiniUncommittedVersion() const = 0;
  virtual const Version& LatestVisibleVersion() const = 0;
  virtual bool IsVersionVisibleToSnapshot(
    const Version& v, const Snapshot& s) const = 0;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
