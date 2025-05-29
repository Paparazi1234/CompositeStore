#include "skiplist_backed_in_memory_store.h"
#include "third-party/gtest/gtest.h"

namespace MULTI_VERSIONS_NAMESPACE {

class SkipListBackedInMemoryStoreTest : public testing::Test {
 public:
  SkipListBackedInMemoryStoreTest() {
    StoreOptions options;
    EmptyMultiVersionsManagerFactory mvm_factory;
    store_ptr_ = new SkipListBackedInMemoryStore(options, mvm_factory);
  }
  ~SkipListBackedInMemoryStoreTest() {
    delete store_ptr_;
  }
 protected:
  SkipListBackedInMemoryStore* store_ptr_;
};

TEST_F(SkipListBackedInMemoryStoreTest, SimpleReadWrite) {
  ReadOptions read_options;
  WriteOptions write_options;
  std::string value;
  Status s;

  // read non-existence
  s = store_ptr_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  // read existence
  s = store_ptr_->Put(write_options, "foo", "bar");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar");

  // read after overwritten
  s = store_ptr_->Put(write_options, "foo", "bar1");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar1");

  // read after deletion
  s = store_ptr_->Delete(write_options, "foo");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  // delete non-existence
  s = store_ptr_->Delete(write_options, "foo");
  ASSERT_TRUE(s.IsOK());
}

TEST_F(SkipListBackedInMemoryStoreTest, MultiKeysInterleavedManipulation) {
  ReadOptions read_options;
  WriteOptions write_options;
  std::string value;
  Status s;
  // multi keys interleaved manipulation
  s = store_ptr_->Put(write_options, "foo", "bar2");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr_->Put(write_options, "foo1", "bar3");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr_->Delete(write_options, "foo");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr_->Put(write_options, "foo", "bar4");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr_->Put(write_options, "foo", "bar5");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr_->Delete(write_options, "foo1");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr_->Delete(write_options, "foo1");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr_->Put(write_options, "foo1", "bar6");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr_->Put(write_options, "foo1", "bar6");
  ASSERT_TRUE(s.IsOK());
  s = store_ptr_->Get(read_options, "foo", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar5");
  s = store_ptr_->Get(read_options, "foo1", &value);
  ASSERT_TRUE(s.IsOK() && value == "bar6");
}

TEST_F(SkipListBackedInMemoryStoreTest, MultiThreadsQuery) {

}

TEST_F(SkipListBackedInMemoryStoreTest, DumpStoreKVPairs) {
  WriteOptions write_options;
  store_ptr_->Put(write_options, "foo", "bar");
  store_ptr_->Put(write_options, "foo1", "bar");
  store_ptr_->Put(write_options, "foo2", "bar");
  store_ptr_->Delete(write_options, "foo");
  store_ptr_->Delete(write_options, "foo1");
  store_ptr_->Delete(write_options, "foo2");
  store_ptr_->Put(write_options, "foo", "bar1");
  store_ptr_->Put(write_options, "foo1", "bar1");
  store_ptr_->Put(write_options, "foo2", "bar1");

  std::string expected =
  "KV pairs in store:\n"
  "  {key: foo@7,\ttype: Put,\tvalue: bar1}\n"
  "  {key: foo@4,\ttype: Delete}\n"
  "  {key: foo@1,\ttype: Put,\tvalue: bar}\n"
  "  {key: foo1@8,\ttype: Put,\tvalue: bar1}\n"
  "  {key: foo1@5,\ttype: Delete}\n"
  "  {key: foo1@2,\ttype: Put,\tvalue: bar}\n"
  "  {key: foo2@9,\ttype: Put,\tvalue: bar1}\n"
  "  {key: foo2@6,\ttype: Delete}\n"
  "  {key: foo2@3,\ttype: Put,\tvalue: bar}\n"
  "  Total count in store: 9, dump count: 9\n";

  std::stringstream oss;
  oss.str("");
  store_ptr_->DumpKVPairs(&oss);
  ASSERT_STREQ(oss.str().c_str(), expected.c_str());
}

}   // namespace MULTI_VERSIONS_NAMESPACE

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
