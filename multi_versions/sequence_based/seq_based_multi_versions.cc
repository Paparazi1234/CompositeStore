#include "seq_based_multi_versions.h"

#include "seq_limits.h"

namespace COMPOSITE_STORE_NAMESPACE {

const SeqBasedVersion SeqBasedMultiVersionsManager::version_limits_min_ =
    SeqBasedVersion(kSeqNumberLimitsMin);
const SeqBasedVersion SeqBasedMultiVersionsManager::version_limits_max_ =
    SeqBasedVersion(kSeqNumberLimitsMax);

void SeqBasedMultiVersionsManager::Initialize(const Version& orig) {
  const SeqBasedVersion* orig_impl =
      static_cast_with_check<const SeqBasedVersion>(&orig);
  uint64_t seq = orig_impl->Seq() + 1;
  seq_allocator_.Initialize(seq);
  max_visible_seq_.store(seq, std::memory_order_seq_cst);
  max_readable_seq_.store(seq, std::memory_order_seq_cst);
}

Version* SeqBasedMultiVersionsManager::AllocateVersion(uint32_t count,
                                                       Version* reused) {
  assert(count > 0);
  uint64_t allocated_started = seq_allocator_.Allocate(count);
  if (reused != nullptr) {
    SeqBasedVersion* version_impl =
        static_cast_with_check<SeqBasedVersion>(reused);
    version_impl->SetSeq(allocated_started);
    return reused;
  } else {
    return new SeqBasedVersion(allocated_started);
  }
}

Version* SeqBasedMultiVersionsManager::LatestVisibleVersion(
    Version* reused) const {
  uint64_t latest_visible = max_visible_seq_.load(std::memory_order_seq_cst);
  if (reused != nullptr) {
    SeqBasedVersion* version_impl =
        static_cast_with_check<SeqBasedVersion>(reused);
    version_impl->SetSeq(latest_visible);
    return reused;
  } else {
    return new SeqBasedVersion(latest_visible);
  }
}

}   // namespace COMPOSITE_STORE_NAMESPACE
