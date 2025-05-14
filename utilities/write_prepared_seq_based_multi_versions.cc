#include "write_prepared_seq_based_multi_versions.h"

#include <assert.h>

#include "seq_based_snapshot.h"

namespace MULTI_VERSIONS_NAMESPACE {

const Version& WritePreparedSeqBasedMultiVersionsManager::
    MiniUncommittedVersion() const {

}

bool WritePreparedSeqBasedMultiVersionsManager::IsVersionVisibleToSnapshot(
    const Version& v, const Snapshot& s) const {
    
}

}   // namespace MULTI_VERSIONS_NAMESPACE