#pragma once

#include <assert.h>
#include <thread>

#include "cartesian_product.h"
#include "include/transaction_store.h"
#include "util/system_clock.h"
#include "util/random.h"

namespace MULTI_VERSIONS_NAMESPACE {

extern uint64_t SeededByThreadId();

struct TxnTestsSetups {
  TxnStoreWritePolicy write_policy = WRITE_COMMITTED;
  bool enable_two_write_queues = false;
  bool with_prepare = true;
  std::string encoded_version = "";
};

class TxnTestSetupsGenerator {
 public:
  // No copying allowed
  TxnTestSetupsGenerator(const TxnTestSetupsGenerator&) = delete;
  TxnTestSetupsGenerator& operator=(const TxnTestSetupsGenerator&) = delete;

  TxnTestSetupsGenerator(
      const std::vector<TxnStoreWritePolicy>& vec_write_policy,
      const std::vector<bool>& vec_enable_two_write_queues,
      const std::vector<bool>& vec_with_prepare = {true},
      const std::vector<std::string>& vec_encoded_version = {""})
        : generator_(vec_write_policy,
                     vec_enable_two_write_queues,
                     vec_with_prepare,
                     vec_encoded_version),
          iter_(&generator_){
    iter_.SeekToFirst();
  }

  bool NextTxnTestSetups(TxnTestsSetups* setups) {
    assert(setups);
    if (!iter_.Valid()) {
      return false;
    }
    iter_.Value(&(setups->write_policy), &(setups->enable_two_write_queues),
                &(setups->with_prepare), &(setups->encoded_version));
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

enum GenType {
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
    if (gen_type_ == kMin) {
      res = min_;
    } else if (gen_type_ == kMedian) {
      res = median_;
    } else if (gen_type_ == kMax) {
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

// cfg for TransactionExecutor
struct TransactionExecutorCfg {
  TransactionExecutorCfg()
      : with_prepare_rate(90, 90, kMin),      // default: 90% with_prepare
        to_be_rollbacked_rate(5, 5, kMin),    // default: 5% rollback txn
        delay_ms_after_prepare(0, 0, kMin),   // default: no delay after prepare
        inc_per_time(1, 1, kMin) {}           // default: increase 1 per time
  UIntRange with_prepare_rate;
  UIntRange to_be_rollbacked_rate;
  UIntRange delay_ms_after_prepare;
  UIntRange inc_per_time;
};

class TransactionExecutor {
 public:
  // No copying allowed
  TransactionExecutor(const TransactionExecutor&) = delete;
  TransactionExecutor& operator=(const TransactionExecutor&) = delete;

  explicit TransactionExecutor(TransactionStore* txn_store,
                               TransactionExecutorCfg* cfg)
      : cfg_(cfg),
        txn_store_(txn_store) {}
  ~TransactionExecutor() {}

  // perform read/modify/write style write
  Status InsertStoreRdMoWr(
      uint16_t target_key_set,
      uint32_t num_keys_in_set,
      uint64_t target_increment,
      const TransactionOptions& txn_options = TransactionOptions());

  // perform write-only style write
  Status InsertStoreWrOnly(
      uint16_t target_key_set,
      uint32_t num_keys_in_set,
      uint64_t target_increment,
      const TransactionOptions& txn_options = TransactionOptions());

  // perform read-only style read
  void ReadStoreRdOnly(
      uint16_t target_key_set,
      uint32_t num_keys_in_set,
      uint64_t* total_increment);

  uint64_t SumAllKeysOfSet(uint16_t target_key_set,
                           uint32_t num_keys_in_set);

 private:
  std::string GetKeyPrefix(uint16_t target_key_set) const {
    char prefix_buf[6] = {0};
    // use key set id as key prefix
    snprintf(prefix_buf, sizeof(prefix_buf), "%.4u", target_key_set + 1);
    return std::string(prefix_buf);
  }

  uint32_t PickKeyRandomly(Random* random, uint32_t num_keys_in_set) {
    // pick a key of the target key set randomly
    return random->Next() % num_keys_in_set;
  }

  Status GetFromStore(
      TransactionStore* txn_store,
      const std::string& key_prefix,
      uint32_t num_keys_in_set,
      uint64_t target_inc,
      std::string* full_key,
      uint64_t* int_value,
      Random* random);
    
  Status GetFromMap(
      std::unordered_map<uint32_t, uint64_t>& map,
      const std::string& key_prefix,
      uint32_t num_keys_in_set,
      uint64_t target_inc,
      std::string* full_key,
      uint64_t* int_value,
      Random* random);
  
  bool NextIncrement(uint64_t* remaining_increment, uint32_t* next_inc);

  TransactionExecutorCfg* cfg_;
  TransactionStore* txn_store_ = nullptr;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
