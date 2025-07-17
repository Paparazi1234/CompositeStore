#include "skiplist_backed/skiplist_backed_mvcc_wb.h"

namespace COMPOSITE_STORE_NAMESPACE {

MVCCWriteBuffer* SkipListBackedMVCCWriteBufferFactory::CreateMVCCWriteBuffer(
    const MultiVersionsManager* mvm) const {
  return new SkipListBackedMVCCWriteBuffer(mvm);
}

}   // namespace COMPOSITE_STORE_NAMESPACE
