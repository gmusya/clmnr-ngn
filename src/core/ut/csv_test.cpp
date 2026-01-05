#include "src/core/csv.h"

#include <cstdio>
#include <filesystem>
#include <random>

#include "gtest/gtest.h"

namespace ngn {

TEST(CsvReader, NonExistingFile) { ASSERT_ANY_THROW(CsvReader reader("bad_path")); }

TEST(CsvReader, Simple) {
  std::mt19937 rnd(2101);

  std::filesystem::path path = std::filesystem::temp_directory_path() / std::to_string(rnd() % 10000);

  std::ofstream file(path);
  file << "a,b,c" << std::endl;
  file << "q,w,e" << std::endl;
  file << "d,,f" << std::endl;
  file << ",x," << std::endl;
  file << ",," << std::endl;
  file << ",,z" << std::endl;
  file.close();

  CsvReader reader(path);

  std::vector<CsvReader::Row> rows;
  for (auto row = reader.ReadNext(); row.has_value(); row = reader.ReadNext()) {
    rows.emplace_back(std::move(*row));
  }

  ASSERT_EQ(rows.size(), 6);
  EXPECT_EQ(rows[0], (CsvReader::Row{"a", "b", "c"}));
  EXPECT_EQ(rows[1], (CsvReader::Row{"q", "w", "e"}));
  EXPECT_EQ(rows[2], (CsvReader::Row{"d", "", "f"}));
  EXPECT_EQ(rows[3], (CsvReader::Row{"", "x", ""}));
  EXPECT_EQ(rows[4], (CsvReader::Row{"", "", ""}));
  EXPECT_EQ(rows[5], (CsvReader::Row{"", "", "z"}));
}

TEST(CsvWriter, Simple) {
  std::mt19937 rnd(2101);

  std::filesystem::path path = std::filesystem::temp_directory_path() / std::to_string(rnd() % 10000);

  CsvWriter writer(path);
  writer.WriteRow({"a", "b", "c"});
  writer.WriteRow({"q", "w", "e"});
  writer.WriteRow({"d", "", "f"});
  writer.WriteRow({"", "x", ""});
  writer.WriteRow({"", "", ""});
  writer.WriteRow({"", "", "z"});

  std::ifstream file(path);

  std::vector<std::string> lines;
  std::string line;
  while (std::getline(file, line)) {
    lines.emplace_back(line);
  }

  ASSERT_EQ(lines.size(), 6);
  EXPECT_EQ(lines[0], "a,b,c");
  EXPECT_EQ(lines[1], "q,w,e");
  EXPECT_EQ(lines[2], "d,,f");
  EXPECT_EQ(lines[3], ",x,");
  EXPECT_EQ(lines[4], ",,");
  EXPECT_EQ(lines[5], ",,z");
}

}  // namespace ngn
