#include "ordered_map_backed_sw.h"
#include "third-party/gtest/gtest.h"

namespace MULTI_VERSIONS_NAMESPACE {

class OrderedMapBackedStagingWriteTest : public testing::Test {
 public:
  OrderedMapBackedStagingWriteTest() {}
  ~OrderedMapBackedStagingWriteTest() {}
};

TEST_F(OrderedMapBackedStagingWriteTest, WriteToStagingWrite) {
  OrderedMapBackedStagingWrite sw;

  // write to staging write
  sw.Put("foo", "bar");
  sw.Put("foo1", "bar");
  sw.Delete("foo2");
  ASSERT_EQ(sw.Count(), 3ull);

  // overwritten in staging write
  sw.Put("foo", "bar1");
  sw.Put("foo1", "bar1");
  sw.Delete("foo2");
  ASSERT_EQ(sw.Count(), 3ull);

  // clear staging write
  sw.Clear();
  ASSERT_TRUE(sw.IsEmpty());

  // reuse after clear
  sw.Put("fo3", "bar1");
  sw.Put("foo4", "bar1");
  sw.Delete("foo5");
  ASSERT_EQ(sw.Count(), 3ull);
}

TEST_F(OrderedMapBackedStagingWriteTest, ReadFromStagingWrite) {
  OrderedMapBackedStagingWrite sw;
  std::string value;
  StagingWrite::GetReault res;

  // read non-existence
  res = sw.Get("foo", &value);
  ASSERT_TRUE(res == StagingWrite::GetReault::kNotFound);

  // read from staging write
  sw.Put("foo", "bar");
  sw.Delete("foo1");
  res = sw.Get("foo", &value);
  ASSERT_TRUE(res == StagingWrite::GetReault::kFound && value == "bar");
  res = sw.Get("foo1", &value);
  ASSERT_TRUE(res == StagingWrite::GetReault::kDeleted);

  // read after overwritten
  sw.Put("foo", "bar1");
  sw.Put("foo1", "bar1");
  res = sw.Get("foo", &value);
  ASSERT_TRUE(res == StagingWrite::GetReault::kFound && value == "bar1");
  res = sw.Get("foo1", &value);
  ASSERT_TRUE(res == StagingWrite::GetReault::kFound && value == "bar1");

  // read after clear
  sw.Clear();
  res = sw.Get("foo", &value);
  ASSERT_TRUE(res == StagingWrite::GetReault::kNotFound);
  res = sw.Get("foo1", &value);
  ASSERT_TRUE(res == StagingWrite::GetReault::kNotFound);

  // read after reuse staging write
  sw.Put("foo2", "bar2");
  sw.Delete("foo3");
  res = sw.Get("foo2", &value);
  ASSERT_TRUE(res == StagingWrite::GetReault::kFound && value == "bar2");
  res = sw.Get("foo3", &value);
  ASSERT_TRUE(res == StagingWrite::GetReault::kDeleted);
}

namespace {
class DumpStagingWriteHandler : public StagingWrite::Handler {
 public:
  ~DumpStagingWriteHandler() {}
  Status Put(const std::string& key, const std::string& value) override {
    oss<<"{key: "<<key<<",\ttype: Put\tvalue: "<<value<<"}\n";
    return Status::OK();
  }

  Status Delete(const std::string& key) override {
    oss<<"{key: "<<key<<",\ttype: Delete}\n";
    return Status::OK();
  }

  std::string GetDumpString() const {
    std::string str = oss.str();
    if (!str.empty()) {
      str.pop_back();
    }
    return str;
  }

 private:
  std::stringstream oss;
};
}   // anonymous namespace

TEST_F(OrderedMapBackedStagingWriteTest, IterateThroughStagingWrite) {
  OrderedMapBackedStagingWrite empty_sw;
  DumpStagingWriteHandler empty_sw_handler;
  Status s = empty_sw.Iterate(&empty_sw_handler);
  ASSERT_TRUE(s.IsOK());
  ASSERT_STREQ("", empty_sw_handler.GetDumpString().c_str());

  OrderedMapBackedStagingWrite sw;
  sw.Put("foo", "bar");
  sw.Delete("foo2");
  sw.Put("foo1", "bar");
  sw.Put("foo5", "bar");
  sw.Delete("foo");
  sw.Put("foo3", "bar");
  sw.Put("foo4", "bar");
  sw.Delete("foo3");
  sw.Put("foo3", "bar1");

  std::string expected =
    "{key: foo,\ttype: Delete}\n"
    "{key: foo1,\ttype: Put\tvalue: bar}\n"
    "{key: foo2,\ttype: Delete}\n"
    "{key: foo3,\ttype: Put\tvalue: bar1}\n"
    "{key: foo4,\ttype: Put\tvalue: bar}\n"
    "{key: foo5,\ttype: Put\tvalue: bar}";
  DumpStagingWriteHandler sw_handler;
  s = sw.Iterate(&sw_handler);
  ASSERT_TRUE(s.IsOK());
  ASSERT_STREQ(expected.c_str(), sw_handler.GetDumpString().c_str());
}

}   // namespace MULTI_VERSIONS_NAMESPACE

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
