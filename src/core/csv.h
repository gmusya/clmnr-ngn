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
    ASSERT_WITH_MESSAGE(file_.good(), "Failed to open csv file: " + filename);
  }

  using Row = std::vector<std::string>;

  std::optional<Row> ReadNext() {
    Row result;
    std::string current;

    bool in_quotes = false;
    bool saw_any = false;
    const size_t record_start_line = CurrentLineNumber();

    while (true) {
      const int ich = file_.get();
      if (ich == EOF) {
        if (!saw_any) {
          return std::nullopt;
        }

        ASSERT_WITH_MESSAGE(!in_quotes, "Unclosed quote, record started at line: " + std::to_string(record_start_line) +
                                            ", current line: " + std::to_string(CurrentLineNumber()));

        result.emplace_back(std::move(current));
        return result;
      }

      saw_any = true;
      const char c = static_cast<char>(ich);

      if (c == '\n') {
        ++line_number_;
        if (!in_quotes) {
          result.emplace_back(std::move(current));
          return result;
        }
        current.push_back('\n');
        continue;
      }

      if (c == '\r') {
        if (file_.peek() == '\n') {
          (void)file_.get();
        }
        ++line_number_;
        if (!in_quotes) {
          result.emplace_back(std::move(current));
          return result;
        }
        current.push_back('\n');
        continue;
      }

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
          const int next = file_.peek();
          if (next == EOF) {
            current.push_back(c);
            continue;
          }
          (void)file_.get();
          AppendEscaped(current, static_cast<char>(next));
          continue;
        }

        current.push_back(c);
        continue;
      }

      if (c == options_.quote) {
        if (options_.double_quote_escape && file_.peek() == options_.quote) {
          (void)file_.get();
          current.push_back(options_.quote);
          continue;
        }
        in_quotes = false;
        continue;
      }

      if (options_.unescape && c == options_.escape) {
        const int next = file_.peek();
        if (next == EOF) {
          current.push_back(c);
          continue;
        }
        (void)file_.get();
        AppendEscaped(current, static_cast<char>(next));
        continue;
      }

      current.push_back(c);
    }
  }

 private:
  size_t CurrentLineNumber() const {
    // line_number_ counts how many '\n' (or CRLF) we've consumed so far.
    // When it's 0, we're on the 1st physical line.
    return line_number_ + 1;
  }

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

  // TODO(gmusya): consider using ifstream wrapper with simple error
  // handling
  std::ifstream file_;
  size_t line_number_ = 0;

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
