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

TEST(CsvReader, QuotedAndEscaped) {
  std::mt19937 rnd(2101);

  std::filesystem::path path = std::filesystem::temp_directory_path() / std::to_string(rnd() % 10000);

  std::ofstream file(path);
  file << "\"a,b\",c" << std::endl;
  file << "\"a\"\"b\",c" << std::endl;
  file << "\"line\\nbreak\",x" << std::endl;
  file << "\"tab\\tseparated\",x" << std::endl;
  file << "\"quote\\\"here\",x" << std::endl;
  file << "\"slash\\\\here\",x" << std::endl;
  file << "a\\,b,c" << std::endl;
  file.close();

  CsvReader reader(path);

  std::vector<CsvReader::Row> rows;
  for (auto row = reader.ReadNext(); row.has_value(); row = reader.ReadNext()) {
    rows.emplace_back(std::move(*row));
  }

  ASSERT_EQ(rows.size(), 7);
  EXPECT_EQ(rows[0], (CsvReader::Row{"a,b", "c"}));
  EXPECT_EQ(rows[1], (CsvReader::Row{"a\"b", "c"}));
  EXPECT_EQ(rows[2], (CsvReader::Row{std::string("line\nbreak"), "x"}));
  EXPECT_EQ(rows[3], (CsvReader::Row{std::string("tab\tseparated"), "x"}));
  EXPECT_EQ(rows[4], (CsvReader::Row{"quote\"here", "x"}));
  EXPECT_EQ(rows[5], (CsvReader::Row{"slash\\here", "x"}));
  EXPECT_EQ(rows[6], (CsvReader::Row{"a,b", "c"}));
}

TEST(CsvReader, MultilineQuotedField) {
  std::mt19937 rnd(2101);

  std::filesystem::path path = std::filesystem::temp_directory_path() / std::to_string(rnd() % 10000);

  std::ofstream file(path);
  file << "a,\"b\nc\",d" << std::endl;
  file << "x,y,z" << std::endl;
  file.close();

  CsvReader reader(path);

  auto row1 = reader.ReadNext();
  ASSERT_TRUE(row1.has_value());
  EXPECT_EQ(*row1, (CsvReader::Row{"a", std::string("b\nc"), "d"}));

  auto row2 = reader.ReadNext();
  ASSERT_TRUE(row2.has_value());
  EXPECT_EQ(*row2, (CsvReader::Row{"x", "y", "z"}));

  auto row3 = reader.ReadNext();
  EXPECT_FALSE(row3.has_value());
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

TEST(CsvWriter, QuotedAndEscaped) {
  std::mt19937 rnd(2101);

  std::filesystem::path path = std::filesystem::temp_directory_path() / std::to_string(rnd() % 10000);

  CsvWriter writer(path);
  writer.WriteRow({"a,b", "c"});
  writer.WriteRow({"a\"b", "c"});
  writer.WriteRow({"a\\,b", "c"});
  writer.WriteRow({"a\\\"b", "c"});

  std::ifstream file(path);

  std::vector<std::string> lines;
  std::string line;
  while (std::getline(file, line)) {
    lines.emplace_back(line);
  }

  ASSERT_EQ(lines.size(), 4);
  EXPECT_EQ(lines[0], "\"a,b\",c");
  EXPECT_EQ(lines[1], "\"a\"\"b\",c");
  EXPECT_EQ(lines[2], "\"a\\,b\",c");
  EXPECT_EQ(lines[3], "\"a\\\"\"b\",c");
}
}  // namespace ngn
