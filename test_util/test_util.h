#pragma once

#include <assert.h>
#include <thread>

#include "cartesian_product.h"
#include "include/transaction_store.h"
#include "util/system_clock.h"
#include "util/random.h"

namespace MULTI_VERSIONS_NAMESPACE {

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
  // No copying allowed
  UIntRange(const UIntRange&) = delete;
  UIntRange& operator=(const UIntRange&) = delete;

  UIntRange(uint32_t min, uint32_t max, GenType gen_type)
      : min_(min), max_(max), gen_type_(gen_type), random_(nullptr) {
    assert(min_ <= max_);
    if (gen_type_ == kRandom) {
      size_t seed = std::hash<std::thread::id>()(std::this_thread::get_id());
      random_ = new Random((uint32_t)seed);
    }
    median_ = (min_ + max_) / 2;
    interval_ = max_ - min_;
    assert((gen_type_ == kRandom && random_ != nullptr) ||
           (gen_type_ != kRandom && random_ == nullptr));
  }

  UIntRange(UIntRange&& other) noexcept
      : min_(other.min_), max_(other.max_), median_(other.median_),
        interval_(other.interval_), gen_type_(other.gen_type_),
        random_(other.random_) {
    other.ResetFields();
    assert((this->gen_type_ == kRandom && this->random_ != nullptr) ||
           (this->gen_type_ != kRandom && this->random_ == nullptr));
  }

  UIntRange& operator=(UIntRange&& other) noexcept {
    if (this == &other) {
      return *this;
    }
    ReleaseRandom();
    this->min_ = other.min_;
    this->max_ = other.max_;
    this->median_ = other.median_;
    this->interval_ = other.interval_;
    this->gen_type_ = other.gen_type_;
    this->random_ = other.random_;
    other.ResetFields();
    assert((this->gen_type_ == kRandom && this->random_ != nullptr) ||
           (this->gen_type_ != kRandom && this->random_ == nullptr));
    return *this;
  }

  ~UIntRange() {
    ReleaseRandom();
  }

  uint32_t Next() {
    uint32_t res;
    if (gen_type_ == kMin) {
      res = min_;
    } else if (gen_type_ == kMedian) {
      res = median_;
    } else if (gen_type_ == kMax) {
      res = max_;
    } else {
      assert(gen_type_ == kRandom && random_ != nullptr);
      uint32_t rnd = random_->Uniform(101);
      res = (interval_ * rnd) / 100 + min_;
      res = std::max(min_, res);
      res = std::min(max_, res);
    }
    return res;
  }

 private:
  void ReleaseRandom() {
    if (random_) {
      assert(gen_type_ == kRandom);
      delete random_;
      random_ = nullptr;
      gen_type_ = kMin;
    }
  }

  void ResetFields() {
    min_ = 0;
    max_ = 0;
    median_ = 0;
    interval_ = 0;
    gen_type_ = kMin;
    random_ = nullptr;
  }

  uint32_t min_;
  uint32_t max_;
  uint32_t median_;
  uint32_t interval_;
  GenType gen_type_;
  Random* random_;
};

struct TransactionExecutorCfg {
  uint16_t target_key_set = 0;
  uint32_t num_keys_in_set = 1000;
  uint64_t total_increment = 1000;
  // default: 90% with_prepare
  UIntRange with_prepare_rate = UIntRange(90, 90, kMin);
  // default: 5% rollback txn
  UIntRange to_be_rollbacked_rate = UIntRange(5, 5, kMin);
  // default: no delay after prepare
  UIntRange delay_ms_after_prepare = UIntRange(0, 0, kMin);
  // default: increase 1 per time
  UIntRange inc_per_time = UIntRange(1, 1, kMin);
  SystemClock* system_clock = nullptr;
};

class TransactionExecutor {
 public:
  // No copying allowed
  TransactionExecutor(const TransactionExecutor&) = delete;
  TransactionExecutor& operator=(const TransactionExecutor&) = delete;

  explicit TransactionExecutor(TransactionStore* txn_store,
                               TransactionExecutorCfg* cfg)
      : cfg_(cfg),
        txn_store_(txn_store),
        random_(10) {
    char prefix_buf[6] = {0};
    // use key set id as key prefix
    snprintf(prefix_buf, sizeof(prefix_buf), "%.4u", cfg_->target_key_set + 1);
    key_prefix_ = std::string(prefix_buf);
    remain_increment_ = cfg_->total_increment;
  }
  ~TransactionExecutor() {
    delete txn_;
  }

  bool InsertStore(
      const TransactionOptions& txn_options = TransactionOptions());

  uint64_t SumAllKeysOfSet();

 private:
  Status GetFromStore(std::string* key, uint64_t* int_value);

  bool NextIncrement(uint32_t* next_inc) {
    if (remain_increment_ == 0) {
      *next_inc = 0;
      return false;
    }
    uint32_t rand_inc = cfg_->inc_per_time.Next();
    if (rand_inc > remain_increment_) {
      rand_inc = remain_increment_;
    }
    remain_increment_ -= rand_inc;
    *next_inc = rand_inc;
    return true;
  }

  WriteOptions write_options_;
  ReadOptions read_options_;
  std::string key_prefix_;
  TransactionExecutorCfg* cfg_;
  Transaction* txn_ = nullptr;
  TransactionStore* txn_store_ = nullptr;
  uint64_t remain_increment_;
  Random random_;
};

}   // namespace MULTI_VERSIONS_NAMESPACE
