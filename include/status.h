#pragma once

#include "multi_versions_namespace.h"

namespace MULTI_VERSIONS_NAMESPACE {

class Status {
 public:
  Status() : code_(kOK) {}
  ~Status() {}

  Status(const Status& s);
  Status& operator=(const Status& s);
  Status(Status&& s) noexcept;
  Status& operator=(Status&& s) noexcept;
  bool operator==(const Status& rhs) const;
  bool operator!=(const Status& rhs) const;

  enum Code : unsigned char {
    kOK = 0x0,
    kNotFound = 0x1,
    kNotSupported = 0x2,
    kInvalidArgument = 0x3,
    kBusy = 0x4,
    kTryAgain = 0x5,
    kMaxCode
  };

  Status(Code code) : code_(code) {}

  Code code() const {
    return code_;
  }

  static Status OK() {
    return Status();
  }

  static Status NotFound() {
    return Status(kNotFound);
  }

  static Status NotSupported() {
    return Status(kNotSupported);
  }

  static Status InvalidArgument() {
    return Status(kInvalidArgument);
  }

  static Status Busy() {
    return Status(kBusy);
  }

  static Status TryAgain() {
    return Status(kTryAgain);
  }

  bool IsOK() const {
    return code() == kOK;
  }

  bool IsNotFound() const {
    return code() == kNotFound;
  }

  bool IsNotSupported() const {
    return code() == kNotSupported;
  }

  bool IsInvalidArgument() const {
    return code() == kInvalidArgument;
  }

  bool IsBusy() const {
    return code() == kBusy;
  }

  bool IsTryAgain() const {
    return code() == kTryAgain;
  }

 private:
  Code code_;
};

inline Status::Status(const Status& s) :code_(s.code_) {
}

inline Status& Status::operator=(const Status& s) {
  if (this != &s) {
    code_ = s.code_;
  }
  return *this;
}

inline Status::Status(Status&& s) noexcept {
  *this = std::move(s);
}

inline Status& Status::operator=(Status&& s) noexcept {
  if (this != &s) {
    code_ = std::move(s.code_);
  }
  return *this;
}

inline bool Status::operator==(const Status& rhs) const {
  return code_ == rhs.code_;
}

inline bool Status::operator!=(const Status& rhs) const {
  return !(*this == rhs);
}

}   // namespace MULTI_VERSIONS_NAMESPACE
