#pragma once

#include "include/multi_versions_namespace.h"

namespace MULTI_VERSIONS_NAMESPACE {

enum ValueType : unsigned char {
  kTypeValue = 0x0,
  kTypeDeletion = 0x1,
  kMaxValueType
};

}   // namespace MULTI_VERSIONS_NAMESPACE