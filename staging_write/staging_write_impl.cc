#include "ordered_map_backed/ordered_map_backed_sw.h"

namespace COMPOSITE_STORE_NAMESPACE {

StagingWrite* OrderedMapBackedStagingWriteFactory::CreateStagingWrite() const {
  return new OrderedMapBackedStagingWrite();
}

}   // namespace COMPOSITE_STORE_NAMESPACE
