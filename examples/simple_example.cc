#include <assert.h>

#include "include/store.h"

using MULTI_VERSIONS_NAMESPACE::Status;
using MULTI_VERSIONS_NAMESPACE::Store;
using MULTI_VERSIONS_NAMESPACE::StoreOptions;
using MULTI_VERSIONS_NAMESPACE::StoreTraits;
using MULTI_VERSIONS_NAMESPACE::ReadOptions;
using MULTI_VERSIONS_NAMESPACE::WriteOptions;

int main() {
  Store* store_ptr;
  StoreOptions store_options;
  StoreTraits store_traits;
  // open store
  Status s =  Store::Open(store_options, store_traits, &store_ptr);
  assert(s.IsOK());

  ReadOptions read_options;
  WriteOptions write_options;
  std::string value;

  // simple read/write
  // read non-existence
  s = store_ptr->Get(read_options, "foo", &value);
  assert(s.IsNotFound());

  // read existence
  s = store_ptr->Put(write_options, "foo", "bar");
  assert(s.IsOK());
  s = store_ptr->Get(read_options, "foo", &value);
  assert(s.IsOK() && value == "bar");

  // read after overwritten
  s = store_ptr->Put(write_options, "foo", "bar1");
  assert(s.IsOK());
  s = store_ptr->Get(read_options, "foo", &value);
  assert(s.IsOK() && value == "bar1");

  // read after deletion
  s = store_ptr->Delete(write_options, "foo");
  assert(s.IsOK());
  s = store_ptr->Get(read_options, "foo", &value);
  assert(s.IsNotFound());

  // delete non-existence
  s = store_ptr->Delete(write_options, "foo");
  assert(s.IsOK());

  // multi keys interleaved manipulation
  s = store_ptr->Put(write_options, "foo", "bar2");
  assert(s.IsOK());
  s = store_ptr->Put(write_options, "foo1", "bar3");
  assert(s.IsOK());
  s = store_ptr->Delete(write_options, "foo");
  assert(s.IsOK());
  s = store_ptr->Put(write_options, "foo", "bar4");
  assert(s.IsOK());
  s = store_ptr->Put(write_options, "foo", "bar5");
  assert(s.IsOK());
  s = store_ptr->Delete(write_options, "foo1");
  assert(s.IsOK());
  s = store_ptr->Delete(write_options, "foo1");
  assert(s.IsOK());
  s = store_ptr->Put(write_options, "foo1", "bar6");
  assert(s.IsOK());
  s = store_ptr->Put(write_options, "foo1", "bar6");
  assert(s.IsOK());
  s = store_ptr->Get(read_options, "foo", &value);
  assert(s.IsOK() && value == "bar5");
  s = store_ptr->Get(read_options, "foo1", &value);
  assert(s.IsOK() && value == "bar6");

  // close store
  delete store_ptr;
  return 0;
}   // namespace MULTI_VERSIONS_NAMESPACE
