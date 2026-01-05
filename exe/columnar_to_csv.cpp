#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "src/core/column.h"
#include "src/core/columnar.h"
#include "src/core/csv.h"

ABSL_FLAG(std::string, input, "", "Input columnar file");
ABSL_FLAG(std::string, output, "", "Output CSV file");

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);

  std::string input = absl::GetFlag(FLAGS_input);
  std::string output = absl::GetFlag(FLAGS_output);

  if (input.empty()) {
    std::cerr << "Input file is required" << std::endl;
    return 1;
  }

  if (output.empty()) {
    std::cerr << "Output file is required" << std::endl;
    return 1;
  }

  ngn::FileReader reader(input);
  ngn::CsvWriter writer(output);

  std::vector<ngn::Column> columns;

  for (size_t i = 0; i < reader.ColumnCount(); ++i) {
    columns.emplace_back(reader.Column(i));
  }

  if (columns.empty()) {
    std::cerr << "No columns found" << std::endl;
    return 1;
  }

  for (size_t i = 0; i < columns.size(); ++i) {
    if (columns[0].Values().size() != columns[i].Values().size()) {
      std::cerr << "All columns must have the same size" << std::endl;
      return 1;
    }
  }

  size_t rows_count = columns[0].Values().size();

  for (size_t i = 0; i < rows_count; ++i) {
    ngn::CsvWriter::Row row;
    for (size_t j = 0; j < columns.size(); ++j) {
      row.emplace_back(std::to_string(columns[j].Values()[i]));
    }
    writer.WriteRow(row);
  }

  return 0;
}
