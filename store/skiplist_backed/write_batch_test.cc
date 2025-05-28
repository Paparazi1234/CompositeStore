#include "write_batch.h"
#include "third-party/gtest/gtest.h"

namespace MULTI_VERSIONS_NAMESPACE {

class WriteBatchTest : public testing::Test {
 public:
  WriteBatchTest() {}
  ~WriteBatchTest() {}
};

TEST_F(WriteBatchTest, WriteToWriteBatch) {
  WriteBatch wb;

  // write to write batch
  wb.Put("foo", "bar");
  wb.Put("foo1", "bar");
  wb.Delete("foo2");
  ASSERT_EQ(wb.Count(), 3);

  // overwritten in write batch
  wb.Put("foo", "bar1");
  wb.Put("foo1", "bar1");
  wb.Delete("foo2");
  ASSERT_EQ(wb.Count(), 3);

  // clear write batch
  wb.Clear();
  ASSERT_TRUE(wb.IsEmpty());

  // reuse after clear
  wb.Put("fo3", "bar1");
  wb.Put("foo4", "bar1");
  wb.Delete("foo5");
  ASSERT_EQ(wb.Count(), 3);
}

TEST_F(WriteBatchTest, ReadFromWriteBatch) {
  WriteBatch wb;
  std::string value;
  WriteBatch::GetReault res;

  // read non-existence
  res = wb.Get("foo", &value);
  ASSERT_TRUE(res == WriteBatch::GetReault::kNotFound);

  // read from write batch
  wb.Put("foo", "bar");
  wb.Delete("foo1");
  res = wb.Get("foo", &value);
  ASSERT_TRUE(res == WriteBatch::GetReault::kFound && value == "bar");
  res = wb.Get("foo1", &value);
  ASSERT_TRUE(res == WriteBatch::GetReault::kDeleted);

  // read after overwritten
  wb.Put("foo", "bar1");
  wb.Put("foo1", "bar1");
  res = wb.Get("foo", &value);
  ASSERT_TRUE(res == WriteBatch::GetReault::kFound && value == "bar1");
  res = wb.Get("foo1", &value);
  ASSERT_TRUE(res == WriteBatch::GetReault::kFound && value == "bar1");

  // read after clear
  wb.Clear();
  res = wb.Get("foo", &value);
  ASSERT_TRUE(res == WriteBatch::GetReault::kNotFound);
  res = wb.Get("foo1", &value);
  ASSERT_TRUE(res == WriteBatch::GetReault::kNotFound);

  // read after reuse write batch
  wb.Put("foo2", "bar2");
  wb.Delete("foo3");
  res = wb.Get("foo2", &value);
  ASSERT_TRUE(res == WriteBatch::GetReault::kFound && value == "bar2");
  res = wb.Get("foo3", &value);
  ASSERT_TRUE(res == WriteBatch::GetReault::kDeleted);
}

namespace {
class WriteBatchDumpHandler : public WriteBatch::Handler {
 public:
  ~WriteBatchDumpHandler() {}
  Status Put(const std::string& key, const std::string& value) override {
    oss<<"{key: "<<key<<",\ttype: Put\tvalue: "<<value<<"}\n";
    return Status::OK();
  }

  Status Delete(const std::string& key) {
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
};   // anonymous namespace

TEST_F(WriteBatchTest, IterateThroughWriteBatch) {
  WriteBatch wb;

  wb.Put("foo", "bar");
  wb.Delete("foo2");
  wb.Put("foo1", "bar");
  wb.Put("foo5", "bar");
  wb.Delete("foo");
  wb.Put("foo3", "bar");
  wb.Put("foo4", "bar");
  wb.Delete("foo3");
  wb.Put("foo3", "bar1");

  std::string expected =
    "{key: foo,\ttype: Delete}\n"
    "{key: foo1,\ttype: Put\tvalue: bar}\n"
    "{key: foo2,\ttype: Delete}\n"
    "{key: foo3,\ttype: Put\tvalue: bar1}\n"
    "{key: foo4,\ttype: Put\tvalue: bar}\n"
    "{key: foo5,\ttype: Put\tvalue: bar}";
  WriteBatchDumpHandler handler;
  wb.Iterate(&handler);
  ASSERT_EQ(expected, handler.GetDumpString());
}

}   // namespace MULTI_VERSIONS_NAMESPACE

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
