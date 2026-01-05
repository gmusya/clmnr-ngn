#include "src/core/columnar_file.h"

#include <cstdio>
#include <filesystem>
#include <random>

#include "gtest/gtest.h"
#include "src/core/column.h"

namespace ngn {

TEST(ColumnarFile, Simple) {
  std::mt19937 rnd(2101);

  std::filesystem::path path = std::filesystem::temp_directory_path() / std::to_string(rnd() % 10000);

  FileWriter writer(path);

  Column col1(std::vector<int64_t>{1, 2, 3, 4});
  Column col2(std::vector<int64_t>{5, 6, 7, 8});
  Column col3(std::vector<int64_t>{9, 10, 11, 12});

  writer.AppendColumn(col1);
  writer.AppendColumn(col2);
  writer.AppendColumn(col3);

  std::move(writer).Finalize();

  FileReader reader(path);

  ASSERT_EQ(reader.ColumnCount(), 3);

  ASSERT_EQ(reader.Column(0), col1.Values());
  ASSERT_EQ(reader.Column(1), col2.Values());
  ASSERT_EQ(reader.Column(2), col3.Values());

  ASSERT_ANY_THROW(reader.Column(3));
}

}  // namespace ngn
