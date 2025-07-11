#include "skiplist_backed/skiplist_backed_mvcc_wb.h"

namespace MULTI_VERSIONS_NAMESPACE {

MVCCWriteBuffer* SkipListBackedMVCCWriteBufferFactory::CreateMVCCWriteBuffer(
    const MultiVersionsManager* mvm) const {
  return new SkipListBackedMVCCWriteBuffer(mvm);
}

}   // namespace MULTI_VERSIONS_NAMESPACE
