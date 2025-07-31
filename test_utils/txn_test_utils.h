#pragma once

#include "composite_store/transaction_store.h"
#include "cartesian_product.h"
#include "test_utils.h"

namespace COMPOSITE_STORE_NAMESPACE {

struct TxnTestsSetups {
  TxnStoreWritePolicy write_policy = TxnStoreWritePolicy::kWriteCommitted;
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

// cfg for TransactionExecutor
struct TransactionExecutorCfg {
  TransactionExecutorCfg()
        // default: 90% with_prepare
      : with_prepare_rate(90, 90, GenType::kMin),
        // default: 5% rollback txn
        to_be_rollbacked_rate(5, 5, GenType::kMin),
        // default: no delay after prepare
        delay_ms_after_prepare(0, 0, GenType::kMin),
        // default: increase 1 per time
        inc_per_time(1, 1, GenType::kMin) {}
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

}   // namespace COMPOSITE_STORE_NAMESPACE
