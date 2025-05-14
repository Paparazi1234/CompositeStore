#pragma once

#include <stdint.h>

#include "snapshot.h"

namespace MULTI_VERSIONS_NAMESPACE {

class SeqBasedSnapshot : public Snapshot {
 public:
  ~SeqBasedSnapshot() {}
  uint64_t Seq() const {
    return rep_;
  }

 private:
  uint64_t rep_;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
