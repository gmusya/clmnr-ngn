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

  writer.AppendRowGroup({col1, col2, col3});

  std::move(writer).Finalize();

  FileReader reader(path);

  ASSERT_EQ(reader.ColumnCount(), 3);
  ASSERT_EQ(reader.RowGroupCount(), 1);

  auto rg0 = reader.ReadRowGroup(0);
  ASSERT_EQ(rg0.size(), 3);
  EXPECT_EQ(rg0[0], col1);
  EXPECT_EQ(rg0[1], col2);
  EXPECT_EQ(rg0[2], col3);

  ASSERT_ANY_THROW(reader.ReadRowGroup(1));
  EXPECT_EQ(reader.ReadRowGroupColumn(0, 0), col1);
  EXPECT_EQ(reader.ReadRowGroupColumn(0, 1), col2);
  EXPECT_EQ(reader.ReadRowGroupColumn(0, 2), col3);
  ASSERT_ANY_THROW(reader.ReadRowGroupColumn(0, 3));
}

TEST(ColumnarFile, Types) {
  std::mt19937 rnd(2101);

  std::filesystem::path path = std::filesystem::temp_directory_path() / std::to_string(rnd() % 10000);

  Schema schema({Field{"a", Type::kInt64}, Field{"b", Type::kString}});
  FileWriter writer(path, schema);

  Column col1(std::vector<int64_t>{1, 2, 3, 4});
  Column col2(std::vector<std::string>{"abc", "def", "qwe", "xyz"});

  writer.AppendRowGroup({col1, col2});

  std::move(writer).Finalize();

  FileReader reader(path);

  ASSERT_EQ(reader.ColumnCount(), 2);
  ASSERT_EQ(reader.RowGroupCount(), 1);

  auto rg0 = reader.ReadRowGroup(0);
  ASSERT_EQ(rg0.size(), 2);
  EXPECT_EQ(rg0[0], col1);
  EXPECT_EQ(rg0[1], col2);
}

TEST(ColumnarFile, MultipleRowGroups) {
  std::mt19937 rnd(2101);

  std::filesystem::path path = std::filesystem::temp_directory_path() / std::to_string(rnd() % 10000);

  Schema schema({Field{"a", Type::kInt64}, Field{"b", Type::kString}});
  FileWriter writer(path, schema);

  Column rg0_a(std::vector<int64_t>{1, 2});
  Column rg0_b(std::vector<std::string>{"a", "b"});
  writer.AppendRowGroup({rg0_a, rg0_b});

  Column rg1_a(std::vector<int64_t>{3, 4, 5});
  Column rg1_b(std::vector<std::string>{"c", "d", "e"});
  writer.AppendRowGroup({rg1_a, rg1_b});

  std::move(writer).Finalize();

  FileReader reader(path);
  ASSERT_EQ(reader.RowGroupCount(), 2);

  auto read_rg0 = reader.ReadRowGroup(0);
  ASSERT_EQ(read_rg0.size(), 2);
  EXPECT_EQ(read_rg0[0], rg0_a);
  EXPECT_EQ(read_rg0[1], rg0_b);

  auto read_rg1 = reader.ReadRowGroup(1);
  ASSERT_EQ(read_rg1.size(), 2);
  EXPECT_EQ(read_rg1[0], rg1_a);
  EXPECT_EQ(read_rg1[1], rg1_b);

  EXPECT_EQ(reader.ReadRowGroupColumn(0, 0), rg0_a);
  EXPECT_EQ(reader.ReadRowGroupColumn(0, 1), rg0_b);
  EXPECT_EQ(reader.ReadRowGroupColumn(1, 0), rg1_a);
  EXPECT_EQ(reader.ReadRowGroupColumn(1, 1), rg1_b);
}

}  // namespace ngn
