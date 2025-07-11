#include "ordered_map_backed/ordered_map_backed_sw.h"

namespace MULTI_VERSIONS_NAMESPACE {

StagingWrite* OrderedMapBackedStagingWriteFactory::CreateStagingWrite() {
  return new OrderedMapBackedStagingWrite();
}

}   // namespace MULTI_VERSIONS_NAMESPACE
