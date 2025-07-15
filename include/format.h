#pragma once

#include <stdint.h>

#include "include/multi_versions_namespace.h"

namespace MULTI_VERSIONS_NAMESPACE {

enum class ValueType : uint32_t {
  kTypeValue = 0x0,
  kTypeDeletion = 0x1,
  kMaxValueType
};

}   // namespace MULTI_VERSIONS_NAMESPACE
