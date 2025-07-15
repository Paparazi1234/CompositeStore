#pragma once

#include "include/multi_versions_namespace.h"

namespace MULTI_VERSIONS_NAMESPACE {
// The helper function to assert the move from dynamic_cast<> to
// static_cast<> is correct. This function is to deal with legacy code.
// It is not recommended to add new code to issue class casting. The preferred
// solution is to implement the functionality without a need of casting.
template <class DestClass, class SrcClass>
inline DestClass* static_cast_with_check(SrcClass* x) {
  DestClass* ret = static_cast<DestClass*>(x);
#ifdef USE_RTTI
  assert(ret == dynamic_cast<DestClass*>(x));
#endif
  return ret;
}

}  // namespace MULTI_VERSIONS_NAMESPACE
