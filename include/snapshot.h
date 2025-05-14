#pragma once

#include "multi_versions_namespace.h"

namespace MULTI_VERSIONS_NAMESPACE {

class Snapshot {
 public:
  // No copying allowed
  Snapshot(const Snapshot&) = delete;
  Snapshot& operator=(const Snapshot&) = delete;

  virtual ~Snapshot() {}
};

class SnapshotManager {
 public:
  // No copying allowed
  SnapshotManager(const SnapshotManager&) = delete;
  SnapshotManager& operator=(const SnapshotManager&) = delete;

  virtual ~SnapshotManager() {}
};

}   // namespace MULTI_VERSIONS_NAMESPACE
