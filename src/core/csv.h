#pragma once

#include <fstream>
#include <optional>
#include <string>
#include <vector>

#include "src/util/assert.h"

namespace ngn {

class CsvReader {
 public:
  struct Options {
    char delimiter = ',';

    Options() {}
  };

  CsvReader(const std::string& filename, Options options = Options{}) : file_(filename), options_(options) {
    ASSERT(file_.good());
  }

  using Row = std::vector<std::string>;

  std::optional<Row> ReadNext() {
    std::string line;
    std::getline(file_, line);

    if (file_.eof()) {
      return std::nullopt;
    }

    Row result;
    size_t current_position = 0;
    while (current_position <= line.size()) {
      size_t end_position = line.find(options_.delimiter, current_position);
      result.emplace_back(line.substr(current_position, end_position - current_position));

      if (end_position == std::string::npos) {
        break;
      }

      current_position = end_position + 1;
    }

    return result;
  }

 private:
  // TODO(gmusya): consider using ifstream wrapper with simple error
  // handling
  std::ifstream file_;

  Options options_{};
};

class CsvWriter {
 public:
  struct Options {
    char delimiter = ',';

    Options() {}
  };

  CsvWriter(const std::string& filename, Options options = Options{}) : file_(filename), options_(options) {}

  using Row = std::vector<std::string>;

  void WriteRow(const Row& row) {
    bool is_first = true;
    for (const auto& value : row) {
      if (!is_first) {
        file_ << options_.delimiter;
      }
      file_ << value;
      is_first = false;
    }
    file_ << std::endl;
  }

 private:
  std::ofstream file_;
  Options options_{};
};

}  // namespace ngn
