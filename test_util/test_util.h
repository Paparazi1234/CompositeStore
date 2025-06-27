#pragma once

#include <assert.h>

#include "cartesian_product.h"
#include "include/options.h"

namespace MULTI_VERSIONS_NAMESPACE {

class TxnTestSetupsGenerator {
 public:
  TxnTestSetupsGenerator(
      const std::vector<TxnStoreWritePolicy>& vec_write_policy,
      const std::vector<bool>& vec_enable_two_write_queues,
      const std::vector<bool>& with_prepare = {true},
      const std::vector<std::string>& orig_version = {""})
        : generator_(vec_write_policy,
                     vec_enable_two_write_queues,
                     with_prepare,
                     orig_version),
          iter_(&generator_){
    iter_.SeekToFirst();
  }

  bool NextTxnTestSetups(TxnStoreWritePolicy* write_policy,
                         bool* enable_two_write_queues,
                         bool* with_prepare,
                         std::string* orig_version) {
    if (!iter_.Valid()) {
      return false;
    }
    iter_.Value(write_policy, enable_two_write_queues, with_prepare,
                orig_version);
    iter_.Next();
    return true;
  }

  void Reset() {
    iter_.SeekToFirst();
  }

 private:
  using CPGenerator =
      CartesianProductGenerator4<TxnStoreWritePolicy, bool, bool, std::string>;
  CPGenerator generator_;
  CPGenerator::Iterator iter_;
};


class RandomKeyGenerator {
 public:
  RandomKeyGenerator() {}


 private:
};

struct TransactionExecutorCfg {

};


class TransactionExecutor {
 public:
  TransactionExecutor() {}

  

 private:
  
};

}   // namespace MULTI_VERSIONS_NAMESPACE
