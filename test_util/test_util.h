#pragma once

#include <assert.h>

#include "include/options.h"

namespace MULTI_VERSIONS_NAMESPACE {

class TestSetupsGenerator {
 public:
  TestSetupsGenerator() {
    write_policies_iter_ = write_policies_.begin();
    enables_iter_ = enables_.begin();
  }

  bool GenerateTestSetups(TxnStoreWritePolicy* write_policy,
                          bool* enable_two_write_queues) {
    if (write_policies_iter_ == write_policies_.end()) {
      assert(enables_iter_ == enables_.end());
      return false;
    }

    assert(write_policies_iter_ != write_policies_.end() &&
           enables_iter_ != enables_.end());
    *write_policy = *write_policies_iter_;
    *enable_two_write_queues = *enables_iter_;
    ++enables_iter_;
    if (enables_iter_ == enables_.end()) {
      ++write_policies_iter_;
      if (write_policies_iter_ != write_policies_.end()) {
        enables_iter_ = enables_.begin();
      }
    }
    assert((write_policies_iter_ != write_policies_.end() &&
            enables_iter_ != enables_.end()) ||
           (write_policies_iter_ == write_policies_.end() &&
            enables_iter_ == enables_.end()));
    return true;
  }

  void Reset() {
    write_policies_iter_ = write_policies_.begin();
    enables_iter_ = enables_.begin();
  }

 private:
  std::vector<TxnStoreWritePolicy> write_policies_ =
      {WRITE_COMMITTED, WRITE_PREPARED};
  std::vector<bool> enables_ = {false, true};
  std::vector<TxnStoreWritePolicy>::iterator write_policies_iter_;
  std::vector<bool>::iterator enables_iter_;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
