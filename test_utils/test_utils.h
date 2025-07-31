#pragma once

#include <assert.h>

#include "composite_store/status.h"
#include "util/random.h"
#include "third-party/gtest/gtest.h"

namespace COMPOSITE_STORE_NAMESPACE {

::testing::AssertionResult AssertStatus(const char* s_expr, const Status& s);

#define ASSERT_OK(s) \
  ASSERT_PRED_FORMAT1(COMPOSITE_STORE_NAMESPACE::AssertStatus, s)
#define ASSERT_NOK(s) ASSERT_FALSE((s).IsOK())
#define EXPECT_OK(s) \
  EXPECT_PRED_FORMAT1(COMPOSITE_STORE_NAMESPACE::AssertStatus, s)
#define EXPECT_NOK(s) EXPECT_FALSE((s).IsOK())

class RandomKeyGenerator {
 public:
  RandomKeyGenerator() {}


 private:
};

enum class GenType : unsigned char {
  kRandom = 0x0,
  kMin,
  kMedian,
  kMax,
};

class UIntRange {
 public:
  UIntRange(uint32_t min, uint32_t max, GenType gen_type)
      : min_(min), max_(max), gen_type_(gen_type) {
    assert(min_ <= max_);
    median_ = (min_ + max_) / 2;
    interval_ = max_ - min_;
  }
  ~UIntRange() {}

  uint32_t Next() {
    uint32_t res;
    if (gen_type_ == GenType::kMin) {
      res = min_;
    } else if (gen_type_ == GenType::kMedian) {
      res = median_;
    } else if (gen_type_ == GenType::kMax) {
      res = max_;
    } else {
      uint32_t rnd_percentage = Random::GetTLSInstance()->Uniform(101);
      res = (interval_ * rnd_percentage) / 100 + min_;
      res = std::max(min_, res);
      res = std::min(max_, res);
    }
    return res;
  }

 private:
  uint32_t min_;
  uint32_t max_;
  uint32_t median_;
  uint32_t interval_;
  GenType gen_type_;
};

}   // namespace COMPOSITE_STORE_NAMESPACE
