#pragma once

#include <string>
#include <cstdint>

#include "slice.h"

namespace ROCKSDB_NAMESPACE {

// The maximum length of a varint in bytes for 64-bit.
const uint32_t kMaxVarint64Length = 10;

extern void PutVarint32(std::string* dst, uint32_t value);
extern void PutVarint64(std::string* dst, uint64_t value);
extern bool GetVarint32(Slice* input, uint32_t* value);
extern bool GetVarint64(Slice* input, uint64_t* value);
extern void PutLengthPrefixedSlice(std::string* dst, const Slice& value);
extern bool GetLengthPrefixedSlice(Slice* input, Slice* result);
// This function assumes data is well-formed.
extern Slice GetLengthPrefixedSlice(const char* data);

// Pointer-based variants of GetVarint...  These either store a value
// in *v and return a pointer just past the parsed value, or return
// nullptr on error.  These routines only look at bytes in the range
// [p..limit-1]
extern const char* GetVarint32Ptr(const char* p, const char* limit,
                                  uint32_t* v);
extern const char* GetVarint64Ptr(const char* p, const char* limit,
                                  uint64_t* v);

// Returns the length of the varint32 or varint64 encoding of "v"
extern int VarintLength(uint64_t v);

// Lower-level versions of Put... that write directly into a character buffer
// and return a pointer just past the last byte written.
// REQUIRES: dst has enough space for the value being written
extern char* EncodeVarint32(char* dst, uint32_t value);
extern char* EncodeVarint64(char* dst, uint64_t value);

// Internal routine for use by fallback path of GetVarint32Ptr
extern const char* GetVarint32PtrFallback(const char* p, const char* limit,
                                          uint32_t* value);
inline const char* GetVarint32Ptr(const char* p, const char* limit,
                                  uint32_t* value) {
  if (p < limit) {
    uint32_t result = *(reinterpret_cast<const unsigned char*>(p));
    if ((result & 128) == 0) {
      *value = result;
      return p + 1;
    }
  }
  return GetVarint32PtrFallback(p, limit, value);
}

inline int VarintLength(uint64_t v) {
  int len = 1;
  while (v >= 128) {
    v >>= 7;
    len++;
  }
  return len;
}

inline void PutVarint32(std::string* dst, uint32_t v) {
  char buf[5];
  char* ptr = EncodeVarint32(buf, v);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline char* EncodeVarint64(char* dst, uint64_t v) {
  static const unsigned int B = 128;
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  while (v >= B) {
    *(ptr++) = (v & (B - 1)) | B;
    v >>= 7;
  }
  *(ptr++) = static_cast<unsigned char>(v);
  return reinterpret_cast<char*>(ptr);
}


inline void PutVarint64(std::string* dst, uint64_t v) {
  char buf[kMaxVarint64Length];
  char* ptr = EncodeVarint64(buf, v);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline bool GetVarint32(Slice* input, uint32_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint32Ptr(p, limit, value);
  if (q == nullptr) {
    return false;
  } else {
    *input = Slice(q, static_cast<size_t>(limit - q));
    return true;
  }
}

inline bool GetVarint64(Slice* input, uint64_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint64Ptr(p, limit, value);
  if (q == nullptr) {
    return false;
  } else {
    *input = Slice(q, static_cast<size_t>(limit - q));
    return true;
  }
}

inline void PutLengthPrefixedSlice(std::string* dst, const Slice& value) {
  PutVarint32(dst, static_cast<uint32_t>(value.size()));
  dst->append(value.data(), value.size());
}

inline bool GetLengthPrefixedSlice(Slice* input, Slice* result) {
  uint32_t len = 0;
  if (GetVarint32(input, &len) && input->size() >= len) {
    *result = Slice(input->data(), len);
    input->remove_prefix(len);
    return true;
  } else {
    return false;
  }
}

inline Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len = 0;
  // +5: we assume "data" is not corrupted
  // unsigned char is 7 bits, uint32_t is 32 bits, need 5 unsigned char
  auto p = GetVarint32Ptr(data, data + 5 /* limit */, &len);
  return Slice(p, len);
}

}  // namespace ROCKSDB_NAMESPACE
