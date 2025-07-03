#include "test_util.h"

#include <unordered_map>

namespace MULTI_VERSIONS_NAMESPACE {

Status TransactionExecutor::InsertStoreRdMoWr(
    uint16_t target_key_set,
    uint32_t num_keys_in_set,
    uint64_t target_increment,
    const TransactionOptions& txn_options) {
  uint32_t next_inc = 0;
  uint64_t remaining_increment = target_increment;
  bool need_redo = false;
  Random* random = Random::GetTLSInstance();
  std::string key_prefix = GetKeyPrefix(target_key_set);
  Transaction* txn = nullptr;
  WriteOptions write_options;
  Status s;
  while (need_redo || NextIncrement(&remaining_increment, &next_inc)) {
    assert(next_inc > 0);
    txn = txn_store_->BeginTransaction(write_options, txn_options, txn);

    // read and modify
    std::string full_key;
    uint64_t new_value = 0;
    s = GetFromStore(txn_store_, key_prefix, num_keys_in_set, next_inc,
                     &full_key, &new_value, random);
    if (!s.IsOK()) {
      break;
    }

    // write
    std::string new_value_str = std::to_string(new_value);
    s = txn->Put(full_key, new_value_str);
    assert(s.IsOK());

    bool with_prepare = random->PercentTrue(cfg_->with_prepare_rate.Next());
    if (with_prepare) {
      s = txn->Prepare();
      assert(s.IsOK());
      // uint32_t delay_ms = cfg_->delay_ms_after_prepare.Next();
    }

    bool to_be_rollbacked =
        random->PercentTrue(cfg_->to_be_rollbacked_rate.Next());
    if (!to_be_rollbacked) {
      s = txn->Commit();
      assert(s.IsOK());
      need_redo = false;
    } else {
      s = txn->Rollback();
      assert(s.IsOK());
      need_redo = true;
    }
  }

  delete txn;
  if (s.IsOK()) {
    assert(need_redo == false && remaining_increment == 0);
  }
  return s;
}

Status TransactionExecutor::InsertStoreWrOnly(
    uint16_t target_key_set,
    uint32_t num_keys_in_set,
    uint64_t target_increment,
    const TransactionOptions& txn_options) {
  std::unordered_map<uint32_t, uint64_t> key_value_inserted;
  uint32_t next_inc = 0;
  uint64_t remaining_increment = target_increment;
  bool need_redo = false;
  Random* random = Random::GetTLSInstance();
  std::string key_prefix = GetKeyPrefix(target_key_set);
  Transaction* txn = nullptr;
  WriteOptions write_options;
  Status s;
  uint64_t new_value = 0;
  std::string full_key;
  while (need_redo || NextIncrement(&remaining_increment, &next_inc)) {
    assert(next_inc > 0);
    txn = txn_store_->BeginTransaction(write_options, txn_options, txn);

    if (need_redo == false) {
      s = GetFromMap(key_value_inserted, key_prefix, num_keys_in_set, next_inc,
                     &full_key, &new_value, random);
      assert(s.IsOK());
    }

    // write
    std::string new_value_str = std::to_string(new_value);
    s = txn->Put(full_key, new_value_str);
    assert(s.IsOK());
    bool with_prepare = random->PercentTrue(cfg_->with_prepare_rate.Next());
    if (with_prepare) {
      s = txn->Prepare();
      assert(s.IsOK());
      // uint32_t delay_ms = cfg_->delay_ms_after_prepare.Next();
    }

    bool to_be_rollbacked =
        random->PercentTrue(cfg_->to_be_rollbacked_rate.Next());
    if (!to_be_rollbacked) {
      s = txn->Commit();
      assert(s.IsOK());
      need_redo = false;
    } else {
      s = txn->Rollback();
      assert(s.IsOK());
      need_redo = true;
    }
  }

  delete txn;
  assert(s.IsOK());
  assert(need_redo == false && remaining_increment == 0);
  return s;
}

void TransactionExecutor::ReadStoreRdOnly(
    uint16_t target_key_set,
    uint32_t num_keys_in_set,
    uint64_t* total_increment) {
  std::string key_prefix = GetKeyPrefix(target_key_set);
  const Snapshot* snapshot = txn_store_->TakeSnapshot();
  ReadOptions read_options;
  read_options.snapshot = snapshot;
  uint64_t sum = 0;
  std::string key;
  std::string value;
  // only support point lookup currently
  for (uint32_t i = 0; i < num_keys_in_set; ++i) {
    key = key_prefix + std::to_string(i);
    Status s = txn_store_->Get(read_options, key, &value);
    if (s.IsOK()) {
      sum += std::stoull(value);
    } else {
      assert(s.IsNotFound());
    }
  }
  *total_increment = sum;
  txn_store_->ReleaseSnapshot(snapshot);
}

uint64_t TransactionExecutor::SumAllKeysOfSet(uint16_t target_key_set,
                                              uint32_t num_keys_in_set) {
  uint64_t sum = 0;
  ReadStoreRdOnly(target_key_set, num_keys_in_set, &sum);
  return sum;
}

Status TransactionExecutor::GetFromStore(
    TransactionStore* txn_store, const std::string& key_prefix, 
    uint32_t num_keys_in_set, uint64_t target_inc, std::string* full_key,
    uint64_t* int_value, Random* random) {
  uint32_t picked_key = PickKeyRandomly(random, num_keys_in_set);
  *full_key = key_prefix + std::to_string(picked_key);
  ReadOptions read_options;
  std::string value;
  Status s = txn_store->Get(read_options, *full_key, &value);
  if (s.IsOK()) {
    *int_value = std::stoull(value) + target_inc;
  } else if (s.IsNotFound()) {
    *int_value = target_inc;
    s = Status::OK();
  }
  return s;
}

Status TransactionExecutor::GetFromMap(
    std::unordered_map<uint32_t, uint64_t>& map, const std::string& key_prefix, 
    uint32_t num_keys_in_set, uint64_t target_inc, std::string* full_key,
    uint64_t* int_value, Random* random) {
  uint32_t picked_key = PickKeyRandomly(random, num_keys_in_set);
  auto iter = map.find(picked_key);
  if (iter == map.end()) {
    iter = map.insert_or_assign(picked_key, target_inc).first;
  } else {
    iter->second +=  target_inc;
  }
  *full_key = key_prefix + std::to_string(picked_key);
  *int_value = iter->second;
  return Status::OK();
}

bool TransactionExecutor::NextIncrement(uint64_t* remaining_increment,
                                        uint32_t* next_inc) {
  if (*remaining_increment == 0) {
    *next_inc = 0;
    return false;
  }
  uint32_t rand_inc = cfg_->inc_per_time.Next();
  if (rand_inc > *remaining_increment) {
    rand_inc = *remaining_increment;
  }
  *remaining_increment -= rand_inc;
  *next_inc = rand_inc;
  return true;
}

}   // namespace MULTI_VERSIONS_NAMESPACE
