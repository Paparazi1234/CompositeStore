#pragma once

#include <string>

#include "format.h"
#include "multi_versions.h"

namespace COMPOSITE_STORE_NAMESPACE {

class MVCCWriteBuffer {
 public:
  // No copying allowed
  MVCCWriteBuffer(const MVCCWriteBuffer&) = delete;
  MVCCWriteBuffer& operator=(const MVCCWriteBuffer&) = delete;

  MVCCWriteBuffer() {}
  virtual ~MVCCWriteBuffer() {}

  virtual Status Insert(const std::string& key,
                        const std::string& value,
                        ValueType value_type,
                        const Version& version) = 0;
  virtual Status Get(const std::string& key,
                     std::string* value,
                     const Snapshot& read_snapshot) = 0;

  virtual uint64_t RawDataSize() const = 0;
  virtual void Dump(std::stringstream* oss, const size_t dump_count) = 0;

  virtual uint64_t NumInserts() const = 0;
  virtual uint64_t NumDeletes() const = 0;
};

class MVCCWriteBufferFactory {
 public:
  virtual ~MVCCWriteBufferFactory() {}

  virtual MVCCWriteBuffer* CreateMVCCWriteBuffer(
      const MultiVersionsManager* mvm) const = 0;
};

class SkipListBackedMVCCWriteBufferFactory : public MVCCWriteBufferFactory {
 public:
  virtual ~SkipListBackedMVCCWriteBufferFactory() {}

  MVCCWriteBuffer* CreateMVCCWriteBuffer(
      const MultiVersionsManager* mvm) const override;
};

}   // namespace COMPOSITE_STORE_NAMESPACE
