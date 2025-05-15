#pragma once

#include "multi_versions_namespace.h"

namespace MULTI_VERSIONS_NAMESPACE {

class Snapshot {
 public:
  // No copying allowed
  Snapshot(const Snapshot&) = delete;
  Snapshot& operator=(const Snapshot&) = delete;

  Snapshot() {}
  virtual ~Snapshot() {}
};

class SnapshotManager {
 public:
  // No copying allowed
  SnapshotManager(const SnapshotManager&) = delete;
  SnapshotManager& operator=(const SnapshotManager&) = delete;

  SnapshotManager() {}
  virtual ~SnapshotManager() {}

  virtual const Snapshot* TakeSnapshot() = 0;   
  virtual void ReleaseSnapshot(const Snapshot* snapshot) = 0;
  virtual void GetAllLivingSnapshot() = 0;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
