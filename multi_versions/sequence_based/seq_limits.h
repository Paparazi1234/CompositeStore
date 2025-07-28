#pragma once

#include <stdint.h>
#include <limits>

#include "composite_store/composite_store_namespace.h"

namespace COMPOSITE_STORE_NAMESPACE {

const uint64_t kSeqNumberLimitsMax = std::numeric_limits<uint64_t>::max();
const uint64_t kSeqNumberLimitsMin = 0;
const uint64_t kUnCommittedLimitsMin = 1;

}   // namespace COMPOSITE_STORE_NAMESPACE
