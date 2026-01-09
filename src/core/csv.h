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
    char quote = '"';
    char escape = '\\';
    bool unescape = true;
    // If true, then inside quoted fields two consecutive quotes ("") decode to one quote (").
    bool double_quote_escape = true;

    Options() {}
  };

  CsvReader(const std::string& filename, Options options = Options{}) : file_(filename), options_(options) {
    ASSERT(file_.good());
  }

  using Row = std::vector<std::string>;

  std::optional<Row> ReadNext() {
    std::string line;
    if (!std::getline(file_, line)) {
      return std::nullopt;
    }

    return ParseLine(line);
  }

 private:
  static void AppendEscaped(std::string& out, char c) {
    switch (c) {
      case 'n':
        out.push_back('\n');
        return;
      case 'r':
        out.push_back('\r');
        return;
      case 't':
        out.push_back('\t');
        return;
      case '\\':
        out.push_back('\\');
        return;
      case '"':
        out.push_back('"');
        return;
      default:
        // Generic escaping: "\," -> ","; "\x" -> "x"
        out.push_back(c);
        return;
    }
  }

  std::optional<Row> ParseLine(const std::string& line) const {
    Row result;
    std::string current;
    bool in_quotes = false;

    for (size_t i = 0; i < line.size(); ++i) {
      const char c = line[i];

      if (!in_quotes) {
        if (c == options_.delimiter) {
          result.emplace_back(std::move(current));
          current.clear();
          continue;
        }

        if (c == options_.quote && current.empty()) {
          in_quotes = true;
          continue;
        }

        if (options_.unescape && c == options_.escape) {
          if (i + 1 >= line.size()) {
            current.push_back(c);
            continue;
          }
          AppendEscaped(current, line[i + 1]);
          ++i;
          continue;
        }

        current.push_back(c);
        continue;
      }

      // in_quotes
      if (c == options_.quote) {
        if (options_.double_quote_escape && (i + 1 < line.size()) && line[i + 1] == options_.quote) {
          current.push_back(options_.quote);
          ++i;
          continue;
        }
        in_quotes = false;
        continue;
      }

      if (options_.unescape && c == options_.escape) {
        if (i + 1 >= line.size()) {
          current.push_back(c);
          continue;
        }
        AppendEscaped(current, line[i + 1]);
        ++i;
        continue;
      }

      current.push_back(c);
    }

    ASSERT(!in_quotes);

    result.emplace_back(std::move(current));
    return result;
  }

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
