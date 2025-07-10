#pragma once

#include <string>

#include "status.h"

namespace MULTI_VERSIONS_NAMESPACE {

class MVCCWriteBuffer {
 public:
  // No copying allowed
  MVCCWriteBuffer(const MVCCWriteBuffer&) = delete;
  MVCCWriteBuffer& operator=(const MVCCWriteBuffer&) = delete;

  MVCCWriteBuffer() {}
  virtual ~MVCCWriteBuffer() {}

  virtual Status Insert(const std::string& key, const std::string& value) = 0;
  virtual Status Get(const std::string& key, std::string* value) = 0;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
