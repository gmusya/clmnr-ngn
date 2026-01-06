#include "src/core/columnar.h"

#include <cstdio>
#include <filesystem>
#include <random>

#include "gtest/gtest.h"
#include "src/core/column.h"

namespace ngn {

TEST(ColumnarFile, Simple) {
  std::mt19937 rnd(2101);

  std::filesystem::path path = std::filesystem::temp_directory_path() / std::to_string(rnd() % 10000);

  Schema schema({Field{"a", Type::kInt64}, Field{"b", Type::kInt64}, Field{"c", Type::kInt64}});
  FileWriter writer(path, schema);

  Column col1(std::vector<int64_t>{1, 2, 3, 4});
  Column col2(std::vector<int64_t>{5, 6, 7, 8});
  Column col3(std::vector<int64_t>{9, 10, 11, 12});

  writer.AppendColumn(col1);
  writer.AppendColumn(col2);
  writer.AppendColumn(col3);

  std::move(writer).Finalize();

  FileReader reader(path);

  ASSERT_EQ(reader.ColumnCount(), 3);

  EXPECT_EQ(reader.ReadColumn(0), col1);
  EXPECT_EQ(reader.ReadColumn(1), col2);
  EXPECT_EQ(reader.ReadColumn(2), col3);

  ASSERT_ANY_THROW(reader.ReadColumn(3));
}

TEST(ColumnarFile, Types) {
  std::mt19937 rnd(2101);

  std::filesystem::path path = std::filesystem::temp_directory_path() / std::to_string(rnd() % 10000);

  Schema schema({Field{"a", Type::kInt64}, Field{"b", Type::kString}});
  FileWriter writer(path, schema);

  Column col1(std::vector<int64_t>{1, 2, 3, 4});
  Column col2(std::vector<std::string>{"abc", "def", "qwe", "xyz"});

  writer.AppendColumn(col1);
  writer.AppendColumn(col2);

  std::move(writer).Finalize();

  FileReader reader(path);

  ASSERT_EQ(reader.ColumnCount(), 2);

  EXPECT_EQ(reader.ReadColumn(0), col1);
  EXPECT_EQ(reader.ReadColumn(1), col2);
}

}  // namespace ngn
