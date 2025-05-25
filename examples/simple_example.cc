#include <assert.h>

#include "include/store.h"

using MULTI_VERSIONS_NAMESPACE::Status;
using MULTI_VERSIONS_NAMESPACE::Store;
using MULTI_VERSIONS_NAMESPACE::StoreOptions;
using MULTI_VERSIONS_NAMESPACE::ReadOptions;
using MULTI_VERSIONS_NAMESPACE::WriteOptions;

int main() {
  Store* store_ptr;
  StoreOptions store_options;
  // open store
  Status s =  Store::Open(store_options, &store_ptr);
  assert(s.IsOK());

  ReadOptions read_options;
  WriteOptions write_options;
  std::string value;

  // simple write/read
  s = store_ptr->Put(write_options, "foo", "bar");
  assert(s.IsOK());
  s = store_ptr->Get(read_options, "foo", &value);
  assert(s.IsOK() && value == "bar");

  // read after deletion
  s = store_ptr->Delete(write_options, "foo");
  assert(s.IsOK());
  s = store_ptr->Get(read_options, "foo", &value);
  assert(s.IsNotFound());

  // read non-exists
  s = store_ptr->Get(read_options, "foo1", &value);
  assert(s.IsNotFound());

  // read after overwritten
  s = store_ptr->Put(write_options, "foo", "bar1");
  assert(s.IsOK());
  s = store_ptr->Put(write_options, "foo", "bar2");
  assert(s.IsOK());
  s = store_ptr->Get(read_options, "foo", &value);
  assert(s.IsOK() && value == "bar2");

  delete store_ptr;
  return 0;
}   // namespace MULTI_VERSIONS_NAMESPACE
