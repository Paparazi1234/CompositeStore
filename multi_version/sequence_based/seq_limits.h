#pragma once

#include <stdint.h>
#include <limits>

#include "include/multi_versions_namespace.h"

namespace MULTI_VERSIONS_NAMESPACE {

const uint64_t kSeqNumberLimitsMax = std::numeric_limits<uint64_t>::max();
const uint64_t kSeqNumberLimitsMin = 0;
const uint64_t kUnCommittedLimitsMin = 1;

}   // namespace MULTI_VERSIONS_NAMESPACE
