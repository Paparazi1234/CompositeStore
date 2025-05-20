#pragma once

#include "multi_versions.h"

namespace MULTI_VERSIONS_NAMESPACE {

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

}   // namespace MULTI_VERSIONS_NAMESPACE
