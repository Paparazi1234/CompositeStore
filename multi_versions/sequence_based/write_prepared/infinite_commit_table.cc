#include "infinite_commit_table.h"

namespace COMPOSITE_STORE_NAMESPACE {

void HistoryCommitteds::AddHistoryCommitted(
    const SequenceNumber& prepared_version,
    const SequenceNumber& committed_version) {
  // First check the snapshot cache that is efficient for concurrent access
  auto cnt = num_long_live_snapshots_.load(std::memory_order_acquire);
  // The list might get updated concurrently as we are reading from it. The
  // reader should be able to read all the snapshots that are still valid
  // after the update. Since the survived snapshots are written in a higher
  // place before gets overwritten the reader that reads bottom-up will
  // eventully see it.
  const bool next_is_larger = true;
  // We will set to true if the border line snapshot suggests that.
  bool search_extra_list = false; 
  size_t ip1 = std::min(cnt, SNAPSHOT_CACHE_SIZE);
  for (; 0 < ip1; ip1--) {      // 1、首先在无锁情况下反向处理一遍snapshot cache
    SequenceNumber snapshot_seq =
        long_live_snapshots_[ip1 - 1].load(std::memory_order_acquire);
    if (ip1 == SNAPSHOT_CACHE_SIZE) {  // border line snapshot
      // snapshot_seq < commit_seq => larger_snapshot_seq <= commit_seq
      // then later also continue the search to larger snapshots
      search_extra_list = snapshot_seq < committed_version;     // 当snapshot cache满且snapshot cache中的最大snapshot小于目标evict commit entry的commit seq，那么后续需要继续检查second snapshot list
    }                                                             // 因为second snapshot list中可能存在snapshot满足：snapshot seq < 目标evict commit entry的commit seq
    if (!UpdateSnapshotInvisibleVersionsLowerBounds(prepared_version,
            committed_version, snapshot_seq, !next_is_larger)) {    // 如果不需要继续检查snapshot list中的下一个snapshot，那么退出循环
      break;
    }
  }

  if (UNLIKELY(SNAPSHOT_CACHE_SIZE < cnt && search_extra_list)) {     // 通常情况下snapshot cache已经足够容纳所有的snapshot了，如果snapshot cache不能够容纳所有的snapshot，表示second snapshot list中也存在snapshot
    // Then access the less efficient list of snapshots_              // 如果在上面的流程中判定需要检查second snapshot list的话，那么对snapshot cache和second snapshot list重新执行一次正向的按序检测
    ReadLock read_lock(&snapshots_mutex_);                            // 由于本次是对snapshot cache和second snapshot list重新执行的一次正向的按序检测，
                                                                      // 因此需要对snapshots_mutex_加读锁（即允许并发读，但是不允许并发写，因为本函数会被并发的commit或者rollback事务调用）
    // Items could have moved from the snapshots_ to snapshot_cache_ before
    // accquiring the lock. To make sure that we do not miss a valid snapshot,
    // read snapshot_cache_ again while holding the lock.
    for (size_t i = 0; i < SNAPSHOT_CACHE_SIZE; i++) {      // 由于在请求snapshots_mutex_读锁期间，可能发生snapshots_中的snapshot被移到snapshot_cache_中，因此需要在持有snapshots_mutex_的情况下，重新处理一遍snapshot_cache_
      SequenceNumber snapshot_seq =
          long_live_snapshots_[i].load(std::memory_order_acquire);
      if (!UpdateSnapshotInvisibleVersionsLowerBounds(prepared_version,
              committed_version, snapshot_seq, next_is_larger)) {
        break;
      }
    }
    for (auto snapshot_seq_2 : long_live_snapshots_extra_) {    // 处理完snapshot_cache_之后，继续检测second snapshot list
      if (!UpdateSnapshotInvisibleVersionsLowerBounds(prepared_version,
              committed_version, snapshot_seq_2, next_is_larger)) {
        break;
      }
    }
  }
}

bool HistoryCommitteds::UpdateSnapshotInvisibleVersionsLowerBounds(
    const uint64_t& prepared_version, const uint64_t& committed_version,
    const uint64_t& snapshot, const bool next_is_larger) {
  // if the committed version <= snapshot, then the corresponding prepared
  // version is visible to the snapshot, just throw away(no need to keep track
  // of this committed entry for the snapshot any more), next time when we check
  // for whether a version <= snapshot is visible to the snapshot, if the
  // version isn't one of the snapshot's invisible lower bound versions,
  // then we can tell it's visible to the snapshot
  if (committed_version <= snapshot) {
    return !next_is_larger;
  }

  // if a version > snapshot, it's invisible to the snapshot straightforward,
  // just throw away(no need to keep track of this committed entry for the 
  // snapshot any more)
  if (snapshot < prepared_version) {
    return next_is_larger;
  }

  // note: prepared_version == committed_version is filter out by the above
  // conditions

  // we keep track of an overlapping committed entry of a snapshot, and the
  // committed entry serve as one of invisible lower bound versions of the
  // snapshot
  assert(prepared_version < committed_version);
  assert(prepared_version <= snapshot && snapshot < committed_version);
  snap_invisible_versions_lower_bounds_.AddLowerBound(
      snapshot, prepared_version, committed_version);
  return true;
}

void HistoryCommitteds::AdvanceHistoryBoundary(
    const SequenceNumber& new_boundary) {
  SequenceNumber new_snapshots_version = new_boundary;
  std::vector<uint64_t> new_snapshots;
  bool update_snapshots = false;
  if (new_snapshots_version > snapshots_version_) {                     // 如果new_snapshots_version  <= snapshots_version_，表示snapshot cache已经被
    // This is to avoid updating the snapshots_ if it already updated   // 一个并发的AdvanceMaxEvictedSeq调用更新为比本次调用更加新的snapshot cache，那么本次不需要更新snapshot cache
    // with a more recent vesion by a concrrent thread
    update_snapshots = true;
    // We only care about snapshots lower than max
    get_snapshots_callback_->GetSnapshots(new_boundary, new_snapshots);
  }
  if (update_snapshots) {
    UpdateLongLiveSnapshots(new_snapshots, new_snapshots_version);
    if (!new_snapshots.empty()) {
      snap_invisible_versions_lower_bounds_.InitializeNewlyAdded(new_snapshots);
    }
  }
}

void HistoryCommitteds::UpdateLongLiveSnapshots(
    const std::vector<uint64_t>& new_snapshots, const uint64_t version) {
  WriteLock write_lock(&snapshots_mutex_);
  snapshots_version_ = version;
  // We update the list concurrently with the readers.
  // Both new and old lists are sorted and the new list is subset of the
  // previous list plus some new items. Thus if a snapshot repeats in
  // both new and old lists, it will appear upper in the new list. So if
  // we simply insert the new snapshots in order, if an overwritten item
  // is still valid in the new list is either written to the same place in
  // the array or it is written in a higher palce before it gets
  // overwritten by another item. This guarantess a reader that reads the
  // list bottom-up will eventaully see a snapshot that repeats in the
  // update, either before it gets overwritten by the writer or
  // afterwards.
  size_t i = 0;
  auto it = new_snapshots.begin();
  for (; it != new_snapshots.end() && i < SNAPSHOT_CACHE_SIZE; ++it, ++i) {   // 1、先将snapshot填充到snapshot_cache_（从头到尾填充）中，如果snapshot_cache_不能完全容纳这些新snapshot，
    long_live_snapshots_[i].store(*it, std::memory_order_release);             // 那么将剩余的新snapshot，保存在snapshots_中
  }
  long_live_snapshots_extra_.clear();   // 2、先清理snapshots_，然后将剩余的新snapshot（if any）填充到snapshots_
  for (; it != new_snapshots.end(); ++it) {
    // Insert them to a vector that is less efficient to access
    // concurrently
    long_live_snapshots_extra_.push_back(*it);
  }
  // Update the size at the end. Otherwise a parallel reader might read
  // items that are not set yet.
  num_long_live_snapshots_.store(
      new_snapshots.size(), std::memory_order_release);    
                                                                          // 3、在更新完snapshot_cache_和snapshots_之后才更新snapshots_total_。这样能够避免并发的Reader读取还没有设置的item，这是因为
                                                                          // 在对snapshot_cache_执行反向遍历的时候，是不需要持有snapshots_mutex_读锁的，因此如果先更新snapshots_total_，那么并发Reader
                                                                          // 在对snapshot_cache_执行反向遍历时，可能就会看到还没有设置的item(即无效item)；而对snapshot_cache_执行正向遍历的时候则需要持有
                                                                          // snapshots_mutex_读锁，这种情况下对snapshots_total_更新的先后顺序则没有影响（因为在加锁之后，肯定需要double check）
  // Note: this must be done after the snapshots data structures are updated
  // with the new list of snapshots.
  CleanupReleasedLongLiveSnapshots(long_live_snapshots_all_, new_snapshots);    // 清理所有已经释放的snapshots
  long_live_snapshots_all_ = new_snapshots;
}

void HistoryCommitteds::CleanupReleasedLongLiveSnapshots(
    const std::vector<uint64_t>& old_snapshots,
    const std::vector<uint64_t>& new_snapshots) {
  auto newi = new_snapshots.begin();
  auto oldi = old_snapshots.begin();
  for (; newi != new_snapshots.end() && oldi != old_snapshots.end();) {
    assert(*newi >= *oldi);  // cannot have new snapshots with lower seq
    if (*newi == *oldi) {    // still not released    // 1、如果old_snapshots中的snapshot也在new_snapshots中，表示该snapshot还没有Release，不需要清理
      auto value = *newi;
      while (newi != new_snapshots.end() && *newi == value) {   // advance new_snapshots和old_snapshots
        newi++;
      }
      while (oldi != old_snapshots.end() && *oldi == value) {
        oldi++;
      }
    } else {    // *newi > *oldi，表示当前的old snapshot已经Release，那么清理之：如果该snapshot在old_commit_map_中，那么将其从old_commit_map_中删除，否则跳过之即可（因为其在snapshot list中，而snapshotlist已经执行了overwriten）
      assert(*newi > *oldi);  // *oldi is released
      if (*oldi <= history_boundary_->load(std::memory_order_acquire)) {
        snap_invisible_versions_lower_bounds_.EarseSnapshot(*oldi);
      }
      oldi++;   // advance old_snapshots
    }
  }
  // Everything remained in old_snapshots is released and must be cleaned up
  for (; oldi != old_snapshots.end(); oldi++) {   // 如果old_snapshots中还有snapshot，那么这些snapshot一定已经Release，清理之
    if (*oldi <= history_boundary_->load(std::memory_order_acquire)) {
      snap_invisible_versions_lower_bounds_.EarseSnapshot(*oldi);
    }
  }
}

void InfiniteCommitTable::AddCommittedVersion(uint64_t prepared_version,
                                              uint64_t committed_version,
                                              uint32_t loop_cnt) {
  uint64_t index = recent_committeds_.GetLocation(prepared_version);
  RecentCommitteds::CommitCache::CommitEntry64b evicted_64b;
  RecentCommitteds::CommitCache::CommitEntry evicted;
  bool to_be_evicted =
      recent_committeds_.GetCommitEntryAt(index, &evicted_64b, &evicted);
  if (LIKELY(to_be_evicted)) {
    assert(evicted.prep_seq != prepared_version);  // must not exist before
    uint64_t prev_history_boundary = GetHistoryBoundry();   // 由于存在并发的提交事务更新max_evicted_seq_，在执行下面操作之前，先获取max_evicted_seq_的当前值
    // 处理1：本次从commit_cache_中evicted的commit entry的commit seq比当前的max_evicted_seq_大，那么需要执行max_evicted_seq_更新：
    if (prev_history_boundary < evicted.commit_seq) {
      uint64_t new_history_boundary = CalculateNewHistoryBoundry(evicted.commit_seq);
      AdvanceHistoryBoundary(prev_history_boundary, new_history_boundary);
    }
    // if the evicted entry is in the delayed prepared, then we need to keep
    // track of the evicted info in the delayed prepared
    uncommitteds_tracker_.CommitLongLiveUnCommitted(evicted.prep_seq, evicted.commit_seq);
    // After each eviction from commit cache, check if the commit entry should
    // be kept around because it overlaps with a live snapshot.
    // 处理3：如果本次从commit_cache_中evicted的commit entry和Snapshot cache中的任意Snapshot存在overlap（即evicted.prep_seq <= snapshot_seq < evicted.commit_seq），
    //  那么将该evicted commit entry的prepare seq插入到和其存在overlap的所有Snapshot的old_commit_map_中；
    history_committeds_.AddHistoryCommitted(evicted.prep_seq, evicted.commit_seq);
  }

  // 使用无锁CAS操作将本次待写入commit entry插入到commit_cache_中
  bool succ = recent_committeds_.UpdateCommitEntryAt(
      index, evicted_64b, {prepared_version, committed_version});
  // 如果CAS执行失败，表示存在并发线程在本线程更新commit_cache_之前，将其commit entry插入到commit_cache_的同一个索引处
  // 对于这种情况，那么重试本次AddCommitted（小概率事件）
  // 这里值得注意的是：即使执行了上面的处理即且本次AddCommitted需要重试，也不会影响正确性
  if (UNLIKELY(!succ)) {
    // A very rare event, in which the commit entry is updated before we do.
    // Here we apply a very simple solution of retrying.
    if (loop_cnt > MAX_CAS_RETRIES) {   // 首先AddCommitted需要重试就是一个极小概率的事件，而一个AddCommitted需要重试100次，那基本就是不可能了，这里直接抛出错误
      throw std::runtime_error("Infinite loop in AddCommitted!");
    }
    AddCommittedVersion(prepared_version, committed_version, ++loop_cnt);    // 重试本次AddCommitted（为了避免无限死循环，最多重试100次）
    return;
  }
}

// return false && *snap_exists == true: means version is sure invisible to 
// snapshot in this examine
// return false && *snap_exists == false: means version can't be determined
// whether is invisible to snapshot in this examine for the target snapshot is
// already released or the snapshot was not created ever
// return true && *snap_exists == true: means version is sure visible to
// snapshot in this examine
bool InfiniteCommitTable::IsVersionVisibleToSnapshot(uint64_t version, 
                                                     uint64_t snapshot,
                                                     uint64_t min_uncommitted,
                                                     bool* snap_exists) const {
  assert(min_uncommitted >= kUnCommittedLimitsMin);
  assert(snap_exists != nullptr);
  *snap_exists = true;

  // fast check
  if (snapshot < version) {
    return false;
  }
  if (version < min_uncommitted) {
    return true;
  }

  bool was_empty;
  SequenceNumber history_boundary_lb;
  SequenceNumber history_boundary_ub;
  size_t repeats = 0;

  do {
    repeats++;
    assert(repeats < MAX_CAS_RETRIES);
    if (UNLIKELY(repeats >= MAX_CAS_RETRIES)) {
      throw std::runtime_error(
          "The read was intrupted 100 times by update to history_boundary_. "
          "This is unexpected in all setups");
    }
    history_boundary_lb = GetHistoryBoundry();
    // 在检查commit_cache_之前先记录delayed_prepared_是否为空：
    // 1、如果delayed_prepared_为空，那么本流程在处理完毕时，delayed_prepared_依然为空，因为不允许并发的max_evicted_seq_（因为如果发生并发的delayed_prepared_更新时，会重新执行本流程）更新而导致向delayed_prepared_中添加Prepare seq
    // 2、如果delayed_prepared_不为空，那么本流程在处理完毕时，delayed_prepared_可能为空，因为允许并发的remove Prepare将Prepare seq从delayed_prepared_中删除；
    //    在不为delayed_prepared_不为空的情况下，如果在第一次commit_cache_ lookup以及delayed_prepared_中都不存在目标key的seq的情况下，需要多执行一次commit_cache_ lookup，这样就不会
    //    发生old Prepared recently committed seq的miss；
    was_empty = uncommitteds_tracker_.IsLongLiveUnCommittedsEmpty();
    // 检查目标key的seq是否在commit_cache_中
    uint64_t committed_of_version;
    bool exists_in_recent_committeds = recent_committeds_.GetCommittedOfVersion(
        version, &committed_of_version);
    // 目标key的seq在commit_cache_中，有以下情况：
    //  1、该key为old Prepared old committed（not evicted yet）；
    //  2、该key为old Prepared recently committed（not evicted yet）；
    //  3、该key为recently Prepared recently committed（not evicted yet）；
    //  那么根据commit_cache_中该key的commit seq可目标Snapshot seq进行比较
    if (exists_in_recent_committeds) {
      return committed_of_version <= snapshot;
    }
    // else it could be committed but not inserted in the map which could
    // happen after recovery, or it could be committed and evicted by another
    // commit, or never committed.

    // 目标key的seq不在commit_cache_中，有以下情况：
    //  1、该key为old Prepared old committed（already evicted）；
    //  2、该key为old Prepared recently committed（already evicted but not cleanup yet）；
    //  3、该key为old Prepared recently committed（already evicted and cleanup）；
    //  4、该key为old Prepared recently committed（miss at first lookup to commit_cache_ but catch in delayed_prepared_）；
    //  5、该key为old Prepared recently committed（miss at first lookup to commit_cache_ and already cleanup from delayed_prepared_ but recatch in commit_cache_ by one more try）
    //  6、该key为old Prepared but still not committed；
    //  7、该key为recently Prepared recently committed（already evicted）；
    //  8、该key为recently Prepared and not yet committed；
    // 在这种情况下，还不能确定该key的seq是否已经提交，需进一步判断：
    // At this point we don't know if it was committed or it is still prepared
    history_boundary_ub = GetHistoryBoundry();
    // 如果在上面处理过程中，发生max_evicted_seq_的并发更新，那么重新执行本次判断流程
    if (UNLIKELY(history_boundary_lb != history_boundary_ub)) {   // history_boundary_ updated, retry
      continue;
    }
    // Note: max_evicted_seq_ when we did GetCommitEntry <= max_evicted_seq_ub
    // 8、该key为recently Prepared and not yet committed；
    // 这种情况下，目标key还没有提交，因此对目标Snapshot不可见；
    if (history_boundary_ub < version) {
      // Not evicted from cache and also not present, so must be still
      // prepared
      return false;
    }
    
    if (!was_empty) {
      // We should not normally reach here
      bool exists_in_longlive_uncommitteds =
          uncommitteds_tracker_.GetCommittedOfVersion(version,
                                                      &committed_of_version);
      if (exists_in_longlive_uncommitteds) {
        if (committed_of_version == kSeqNumberLimitsMax) {// still not commit yet
          return false;
        }
        return committed_of_version <= snapshot;   // commit and not cleanup yet
      } else {  // try to recatch in recent_committeds_
        exists_in_recent_committeds = recent_committeds_.GetCommittedOfVersion(
            version, &committed_of_version);
        if (exists_in_recent_committeds) {
          return committed_of_version <= snapshot;
        }
        history_boundary_ub = GetHistoryBoundry();
      }
    }

    // 1、该key为old Prepared old committed（already evicted）；
    // 3、该key为old Prepared recently committed（already evicted and cleanup）；
    // 7、该key为recently Prepared recently committed（already evicted）；
    // 这三种情况需要进一步判断该key的prep seq和commit seq是否和目标Snapshot seq存在overlap；
    //   如果存在overlap，那么该key对目标Snapshot不可见，否则对目标Snapshot可见；
  } while (UNLIKELY(history_boundary_lb != history_boundary_ub));
  // When advancing max_evicted_seq_, we move older entires from prepared to
  // delayed_prepared_. Also we move evicted entries from commit cache to
  // old_commit_map_ if it overlaps with any snapshot. Since prep_seq <=
  // max_evicted_seq_, we have three cases: i) in delayed_prepared_, ii) in
  // old_commit_map_, iii) committed with no conflict with any snapshot. Case
  // (i) delayed_prepared_ is checked above
  // 以下为下面三种情况的进一步判断：
  // 1、该key为old Prepared old committed（already evicted）；
  // 3、该key为old Prepared recently committed（already evicted and cleanup）；
  // 7、该key为recently Prepared recently committed（already evicted）；
  // 这三种情况需要进一步判断该key的prep seq和commit seq是否和目标Snapshot seq存在overlap；
  //   如果存在overlap，那么该key对目标Snapshot不可见，否则对目标Snapshot可见；
  // 上面三种情况目标key的commit seq都已经从commit_cahce_中evicted，那么该key的commit seq <= max_evicted_seq_

  // snapshot_seq > max_evicted_seq_：表示本次目标Snapshot不在Snapshot cache中：
  // 由于本次判断到此处，目标key的commit seq已经从commit_cahce_中evicted，因此该key的commit seq满足：commit seq <= max_evicted_seq_
  // 而如果snapshot_seq > max_evicted_seq_，可以得到snapshot_seq > 该key的commit seq，因此该key对目标Snapshot可见
  if (history_boundary_ub < snapshot) {  // then (ii) cannot be the case
    // only (iii) is the case: committed
    // commit_seq <= max_evicted_seq_ < snapshot_seq => commit_seq <
    // snapshot_seq
    return true;
  }

  // else (ii) might be the case: check the commit data saved for this
  // snapshot. If there was no overlapping commit entry, then it is committed
  // with a commit_seq lower than any live snapshot, including snapshot_seq.

  // 如果old_commit_map_为空，目标Snapshot不在有效，不能够进一步判断目标key是否对该Snapshot可见，返回true且*snap_released==true；
  return history_committeds_.IsVersionVisibleToSnapshot(version,
                                                        snapshot,
                                                        snap_exists);
}

uint64_t InfiniteCommitTable::MiniUnCommittedVersion() const {
  uint64_t max_readable = MaxReadableVersion();
  uint64_t min_uncommitted = uncommitteds_tracker_.MiniUnCommitted();
  if (min_uncommitted == kSeqNumberLimitsMax) { // no uncommitteds currently
    // max_readable alse means max_committed
    uint64_t max_committed = max_readable;
    uint64_t curr_min_uncommitted = max_committed + 1;
    return curr_min_uncommitted;
  }

  return std::min(min_uncommitted, max_readable + 1);
}

uint64_t InfiniteCommitTable::CalculateNewHistoryBoundry(uint64_t based) const {
  uint64_t max_committed_version =
      max_committed_version_.load(std::memory_order_acquire);
  uint64_t new_history_boundary;
  if (LIKELY(based < max_committed_version)) {
    assert(max_committed_version > 0);
    // Inc max in larger steps to avoid frequent updates
    new_history_boundary =
        std::min(based + HISTORY_BOUNDARY_INC_STEP, max_committed_version - 1);
  } else {
    // legit when a commit entry in a staging write overwrite the previous one
    new_history_boundary = based;
  }
  return new_history_boundary;
}

void InfiniteCommitTable::AdvanceHistoryBoundary(
    const SequenceNumber& prev_boundary, const SequenceNumber& new_boundary) {
  uint64_t updated_future_boundary = prev_boundary;
  while (updated_future_boundary < new_boundary &&
         !history_boundary_.compare_exchange_weak(updated_future_boundary,
                                                  new_boundary,
                                                  std::memory_order_acq_rel,
                                                  std::memory_order_relaxed)) {
  };

  uncommitteds_tracker_.AdvanceHistoryBoundary(new_boundary, false);
  history_committeds_.AdvanceHistoryBoundary(new_boundary);

  uint64_t updated_boundary = prev_boundary;
  // loop until history_boundary_ >= new_boundary
  while (updated_boundary < new_boundary &&
         !history_boundary_.compare_exchange_weak(updated_boundary,
                                                  new_boundary,
                                                  std::memory_order_acq_rel,
                                                  std::memory_order_relaxed)) {
  };
}

void InfiniteCommitTable::TakeSnapshot(uint64_t* snapshot_seq,
                                       uint64_t* min_uncommitted) {
  *min_uncommitted = kUnCommittedLimitsMin;
  uint64_t beforehand_min_uncommitted = MiniUnCommittedVersion();
  uint64_t max_committed = MaxCommittedVersion();
  if (UNLIKELY(max_committed != 0 &&
               max_committed <= future_history_boundary_)) {
    // There is a very rare case in which the commit entry evicts another commit
    // entry that is not published yet thus advancing max evicted seq beyond the
    // last published seq. This case is not likely in real-world setup so we
    // handle it with a few retries.
    size_t retry = 0;
    uint64_t future_history_boundary = future_history_boundary_.load();
    while (future_history_boundary != 0 &&
            max_committed <= future_history_boundary &&
            retry < MAX_CAS_RETRIES) {
      // Wait for last visible seq to catch up with max, and also go beyond it
      // by one.
      assert(advance_max_committed_by_one_cb.get() != nullptr);
      advance_max_committed_by_one_cb->AdvanceLatestVisibleByOne();
      max_committed = MaxCommittedVersion();
      future_history_boundary = future_history_boundary_.load();
      retry++;
    }
    assert(max_committed > future_history_boundary);
    if (max_committed <= future_history_boundary) {
      throw std::runtime_error(
          "Snapshot seq " + std::to_string(max_committed) +
          " after " + std::to_string(retry) +
          " retries is still less than future_history_boundary_" +
          std::to_string(future_history_boundary));
    }
  }
  *min_uncommitted = beforehand_min_uncommitted;
  *snapshot_seq = max_committed;
}

}   // namespace COMPOSITE_STORE_NAMESPACE
