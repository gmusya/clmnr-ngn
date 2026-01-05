#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "src/core/columnar.h"
#include "src/core/csv.h"

ABSL_FLAG(std::string, input, "", "Input CSV file");
ABSL_FLAG(std::string, output, "", "Output columnar file");

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

  ngn::CsvReader reader(input);
  ngn::FileWriter writer(output);

  std::vector<ngn::Column> columns;

  for (auto row = reader.ReadNext(); row.has_value(); row = reader.ReadNext()) {
    if (columns.empty()) {
      columns.reserve(row.value().size());
      for (size_t i = 0; i < row.value().size(); ++i) {
        columns.emplace_back(std::vector<int64_t>());
      }
    }

    for (size_t i = 0; i < row.value().size(); ++i) {
      columns[i].Values().emplace_back(std::stoll(row.value()[i]));
    }
  }

  for (auto& column : columns) {
    writer.AppendColumn(std::move(column));
  }

  std::move(writer).Finalize();

  return 0;
}
