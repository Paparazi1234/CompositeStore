#include "include/status.h"

#include <assert.h>

namespace COMPOSITE_STORE_NAMESPACE {

std::string Status::ToString() const {
  const char* type = nullptr;
  switch (code_) {
    case kOK:
      return "OK";
    case kNotFound:
      type = "NotFound";
      break;
    case kNotSupported:
      type = "Not implemented";
      break;
    case kInvalidArgument:
      type = "Invalid argument";
      break;
    case kCorruption:
      type = "Corruption";
      break;
    case kTimedOut:
      type = "Operation timed out";
      break;
    case kBusy:
      type = "Resource busy";
      break;
    case kExpired:
      type = "Operation expired";
      break;
    case kTryAgain:
      type = "Operation failed. Try again.";
      break;
    case kMaxCode:
      assert(false);
      break;
  }

  return std::string(type);
}

}   // namespace COMPOSITE_STORE_NAMESPACE
