#pragma once

#include <stdint.h>

#include "composite_store_namespace.h"

namespace COMPOSITE_STORE_NAMESPACE {

enum class ValueType : uint32_t {
  kTypeValue = 0x0,
  kTypeDeletion = 0x1,
  kMaxValueType
};

}   // namespace COMPOSITE_STORE_NAMESPACE
