#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "src/core/column.h"
#include "src/core/columnar.h"
#include "src/core/csv.h"

ABSL_FLAG(std::string, input, "", "Input columnar file");
ABSL_FLAG(std::string, output, "", "Output CSV file");
ABSL_FLAG(std::string, schema, "", "Schema file");

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);

  std::string input = absl::GetFlag(FLAGS_input);
  std::string output = absl::GetFlag(FLAGS_output);
  std::string schema = absl::GetFlag(FLAGS_schema);

  if (input.empty()) {
    std::cerr << "Input file is required" << std::endl;
    return 1;
  }

  if (output.empty()) {
    std::cerr << "Output file is required" << std::endl;
    return 1;
  }

  if (schema.empty()) {
    std::cerr << "Schema file is required" << std::endl;
    return 1;
  }

  ngn::FileReader reader(input);
  ngn::CsvWriter writer(output);

  const uint64_t row_group_count = reader.RowGroupCount();
  if (row_group_count == 0) {
    std::cerr << "No row groups found" << std::endl;
    return 1;
  }

  for (uint64_t rg = 0; rg < row_group_count; ++rg) {
    std::vector<ngn::Column> columns = reader.ReadRowGroup(rg);
    if (columns.empty()) {
      continue;
    }

    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[0].Size() != columns[i].Size()) {
        std::cerr << "All columns must have the same size" << std::endl;
        return 1;
      }
    }

    const size_t rows_count = columns[0].Size();
    for (size_t i = 0; i < rows_count; ++i) {
      ngn::CsvWriter::Row row;
      row.reserve(columns.size());
      for (size_t j = 0; j < columns.size(); ++j) {
        ngn::Value value = columns[j][i];
        row.emplace_back(value.ToString());
      }
      writer.WriteRow(row);
    }
  }

  return 0;
}
