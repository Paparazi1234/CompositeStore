#pragma once

#include "skiplist_rep.h"
#include "composite_store/mvcc_write_buffer.h"

namespace COMPOSITE_STORE_NAMESPACE {

class SkipListBackedMVCCWriteBuffer : public MVCCWriteBuffer {
 public:
  // No copying allowed
  SkipListBackedMVCCWriteBuffer(const SkipListBackedMVCCWriteBuffer&) = delete;
  SkipListBackedMVCCWriteBuffer& operator=(
      const SkipListBackedMVCCWriteBuffer&) = delete;

  SkipListBackedMVCCWriteBuffer(const MultiVersionsManager* mvm)
      : skiplist_backed_rep_(mvm, &memory_allocator_) {}
  virtual ~SkipListBackedMVCCWriteBuffer() {}

  Status Insert(const std::string& key,
                const std::string& value,
                ValueType value_type,
                const Version& version) override;
  Status Get(const std::string& key,
             std::string* value,
             const Snapshot& read_snapshot) override;

  uint64_t RawDataSize() const override {
		return raw_data_size_;
	}

  void Dump(std::stringstream* oss, const size_t dump_count) override;

  uint64_t NumInserts() const override {
    assert(num_entries_ >= num_deletes_);
    return num_entries_ - num_deletes_;
  }

  uint64_t NumDeletes() const override {
    return num_deletes_;
  }

 private:
  void RecordRawDataSize(const std::string& key, const std::string& value) {
		raw_data_size_ += key.size();
		raw_data_size_ += value.size();
	}

  MemoryAllocator memory_allocator_;
  SkipListBackedRep skiplist_backed_rep_;

  uint64_t num_entries_ = 0;
	uint64_t num_deletes_ = 0;
	uint64_t raw_data_size_ = 0;	// the successfully inserted raw KV pairs size
};

}   // namespace COMPOSITE_STORE_NAMESPACE
