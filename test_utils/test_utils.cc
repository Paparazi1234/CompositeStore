#include "test_utils.h"

namespace COMPOSITE_STORE_NAMESPACE {

::testing::AssertionResult AssertStatus(const char* s_expr, const Status& s) {
  if (s.IsOK()) {
    return ::testing::AssertionSuccess();
  } else {
    return ::testing::AssertionFailure() << s_expr << std::endl << s.ToString();
  }
}

}   // namespace COMPOSITE_STORE_NAMESPACE
