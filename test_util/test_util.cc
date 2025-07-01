#include "test_util.h"

namespace MULTI_VERSIONS_NAMESPACE {

bool TransactionExecutor::InsertStore(const TransactionOptions& txn_options) {
  uint32_t next_inc = 0;
  bool need_redo = false;
  while (need_redo || NextIncrement(&next_inc)) {
    assert(next_inc > 0);
    txn_ = txn_store_->BeginTransaction(write_options_, txn_options, txn_);
    // read
    std::string key;
    uint64_t int_value = 0;
    Status s = GetFromStore(&key, &int_value);
    if (!s.IsOK()) {
      return false;
    }

    // modify
    std::string new_value = std::to_string(int_value + next_inc);
    s = txn_->Put(key, new_value);
    assert(s.IsOK());

    // write
    bool with_prepare =
        random_.PercentTrue(cfg_->with_prepare_rate.Next());
    if (with_prepare) {
      s = txn_->Prepare();
      assert(s.IsOK());
      // uint32_t delay_ms = cfg_->delay_ms_after_prepare.Next();
      // cfg_->system_clock->SleepForMicroseconds(delay_ms * 1000);
    }

    bool to_be_rollbacked =
        random_.PercentTrue(cfg_->to_be_rollbacked_rate.Next());
    if (!to_be_rollbacked) {
      s = txn_->Commit();
      assert(s.IsOK());
      need_redo = false;
    } else {
      s = txn_->Rollback();
      assert(s.IsOK());
      need_redo = true;
    }
  }
  assert(need_redo == false && remain_increment_ == 0);
  return true;
}

uint64_t TransactionExecutor::SumAllKeysOfSet() {
  uint64_t sum = 0;
  ReadOptions read_options;
  std::string key;
  std::string value;
  // only support point lookup currently
  for (uint32_t i = 0; i < cfg_->num_keys_in_set; ++i) {
    key = key_prefix_ + std::to_string(i);
    Status s = txn_store_->Get(read_options, key, &value);
    if (s.IsOK()) {
      sum += std::stoull(value);
    }
  }
  return sum;
}

Status TransactionExecutor::GetFromStore(std::string* key,
                                         uint64_t* int_value) {
  assert(txn_ != nullptr);
  // pick a key of the target key set randomly and retrieve its value
  uint64_t picked_key = random_.Next() % cfg_->num_keys_in_set;
  *key = key_prefix_ + std::to_string(picked_key);
  std::string value;
  Status s = txn_->Get(read_options_, *key, &value);
  if (s.IsOK()) {
    *int_value = std::stoull(value);
  } else if (s.IsNotFound()) {
    *int_value = 0;
    s = Status::OK();
  }
  return s;
}

}   // namespace MULTI_VERSIONS_NAMESPACE
