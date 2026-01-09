#include <absl/log/initialize.h>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/log/log.h"
#include "src/core/column.h"
#include "src/core/columnar.h"
#include "src/core/csv.h"
#include "src/core/schema.h"

ABSL_FLAG(std::string, output, "", "Output file (without extension)");

ABSL_FLAG(int, int_columns, 1, "Number of integer columns");
ABSL_FLAG(int, string_columns, 1, "Number of string columns");
ABSL_FLAG(int, rows, 100, "Number of rows");

ABSL_FLAG(bool, write_csv, true, "Write CSV");
ABSL_FLAG(bool, write_schema, true, "Write schema");
ABSL_FLAG(bool, write_clmnr, true, "Write CLMNR");

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);

  std::string output = absl::GetFlag(FLAGS_output);
  const int int_columns = absl::GetFlag(FLAGS_int_columns);
  const int string_columns = absl::GetFlag(FLAGS_string_columns);
  const int num_columns = int_columns + string_columns;

  std::vector<ngn::Field> fields;
  for (int i = 0; i < int_columns; ++i) {
    std::string name = "f" + std::to_string(fields.size());
    fields.emplace_back(ngn::Field{.name = name, .type = ngn::Type::kInt64});
  }
  for (int i = 0; i < string_columns; ++i) {
    std::string name = "f" + std::to_string(fields.size());
    fields.emplace_back(ngn::Field{.name = name, .type = ngn::Type::kString});
  }

  ngn::Schema schema(std::move(fields));

  std::vector<ngn::Column> columns;
  for (const auto& field : schema.Fields()) {
    switch (field.type) {
      case ngn::Type::kInt64:
        columns.emplace_back(ngn::ArrayType<ngn::Type::kInt64>());
        break;
      case ngn::Type::kString:
        columns.emplace_back(ngn::ArrayType<ngn::Type::kString>());
        break;
      default:
        THROW_NOT_IMPLEMENTED;
    }
  }

  if (bool write_schema = absl::GetFlag(FLAGS_write_schema); write_schema) {
    std::string path = output + ".schema";

    LOG(INFO) << "print schema to '" + path + "' <START>";
    schema.ToFile(path);
    LOG(INFO) << "print schema to '" + path + "' <DONE>";
  }

  const int num_rows = absl::GetFlag(FLAGS_rows);

  for (int i = 0; i < num_rows; ++i) {
    for (int j = 0; j < num_columns; ++j) {
      const auto& field = schema.Fields()[j];
      switch (field.type) {
        case ngn::Type::kInt64:
          std::get<ngn::ArrayType<ngn::Type::kInt64>>(columns[j].Values()).emplace_back(i * num_columns + j);
          break;
        case ngn::Type::kString:
          std::get<ngn::ArrayType<ngn::Type::kString>>(columns[j].Values())
              .emplace_back("str" + std::to_string(i * num_columns + j));
          break;
        default:
          THROW_NOT_IMPLEMENTED;
      }
    }
  }

  if (bool write_clmnr = absl::GetFlag(FLAGS_write_clmnr); write_clmnr) {
    std::string path = output + ".clmnr";
    LOG(INFO) << "print file to '" + path + "' <START>";

    ngn::FileWriter writer(path, schema);
    writer.AppendRowGroup(columns);

    std::move(writer).Finalize();

    LOG(INFO) << "print file to '" + path + "' <DONE>";
  }

  if (bool write_csv = absl::GetFlag(FLAGS_write_schema); write_csv) {
    std::string path = output + ".csv";
    LOG(INFO) << "print file to '" + path + "' <START>";

    ngn::CsvWriter writer(path);

    for (int i = 0; i < num_rows; ++i) {
      ngn::CsvWriter::Row row;
      for (size_t j = 0; j < columns.size(); ++j) {
        ngn::Value value = columns[j][i];
        row.emplace_back(value.ToString());
      }
      writer.WriteRow(row);
    }

    LOG(INFO) << "print file to '" + path + "' <DONE>";
  }

  return 0;
}
