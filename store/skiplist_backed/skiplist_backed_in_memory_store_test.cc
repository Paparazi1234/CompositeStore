#include "skiplist_backed_in_memory_store.h"
#include "third-party/gtest/gtest.h"

namespace MULTI_VERSIONS_NAMESPACE {

class SkipListBackedInMemoryStoreTest : public testing::Test {
 public:
  SkipListBackedInMemoryStoreTest() {
    StoreOptions options;
    EmptyMultiVersionsManagerFactory mvm_factory;
    store_ptr = new SkipListBackedInMemoryStore(options, mvm_factory);
  }
  ~SkipListBackedInMemoryStoreTest() {
    delete store_ptr;
  }
 protected:
  SkipListBackedInMemoryStore* store_ptr;
};

TEST_F(SkipListBackedInMemoryStoreTest, SimpleReadWrite) {
  ReadOptions read_options;
  WriteOptions write_options;
  std::string value;
  Status s;

  // read non-existence
  s = store_ptr->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  // read existence
  s = store_ptr->Put(write_options, "foo", "bar");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  // read after overwritten
  s = store_ptr->Put(write_options, "foo", "bar1");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar1");

  // read after deletion
  s = store_ptr->Delete(write_options, "foo");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  // delete non-existence
  s = store_ptr->Delete(write_options, "foo");
  ASSERT_TRUE(s.IsOK());
}

TEST_F(SkipListBackedInMemoryStoreTest, MultiKeysInterleavedManipulation) {
  ReadOptions read_options;
  WriteOptions write_options;
  std::string value;
  Status s;
  // multi keys interleaved manipulation
  s = store_ptr->Put(write_options, "foo", "bar2");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr->Put(write_options, "foo1", "bar3");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr->Delete(write_options, "foo");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr->Put(write_options, "foo", "bar4");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr->Put(write_options, "foo", "bar5");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr->Delete(write_options, "foo1");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr->Delete(write_options, "foo1");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr->Put(write_options, "foo1", "bar6");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr->Put(write_options, "foo1", "bar6");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar5");
  s = store_ptr->Get(read_options, "foo1", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar6");
}

TEST_F(SkipListBackedInMemoryStoreTest, MultiThreadsQuery) {

}

TEST_F(SkipListBackedInMemoryStoreTest, DumpStoreKVPairs) {
  WriteOptions write_options;
  store_ptr->Put(write_options, "foo", "bar");
  store_ptr->Put(write_options, "foo1", "bar");
  store_ptr->Put(write_options, "foo2", "bar");
  store_ptr->Delete(write_options, "foo");
  store_ptr->Delete(write_options, "foo1");
  store_ptr->Delete(write_options, "foo2");
  store_ptr->Put(write_options, "foo", "bar1");
  store_ptr->Put(write_options, "foo1", "bar1");
  store_ptr->Put(write_options, "foo2", "bar1");

  std::stringstream oss;
  oss.str("");
  store_ptr->DumpKVPairs(&oss);
  std::cout<<oss.str();
}

}   // namespace MULTI_VERSIONS_NAMESPACE

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
