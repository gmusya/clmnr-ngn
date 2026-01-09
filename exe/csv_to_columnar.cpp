#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/log/log.h"
#include "src/core/columnar.h"
#include "src/core/csv.h"
#include "src/core/schema.h"
#include "src/core/type.h"
#include "src/util/macro.h"

ABSL_FLAG(std::string, input, "", "Input CSV file");
ABSL_FLAG(std::string, output, "", "Output columnar file");
ABSL_FLAG(std::string, schema, "", "Schema file");

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);

  std::string input = absl::GetFlag(FLAGS_input);
  std::string output = absl::GetFlag(FLAGS_output);
  std::string schema_file = absl::GetFlag(FLAGS_schema);

  if (input.empty()) {
    std::cerr << "Input file is required" << std::endl;
    return 1;
  }

  if (output.empty()) {
    std::cerr << "Output file is required" << std::endl;
    return 1;
  }

  if (schema_file.empty()) {
    std::cerr << "Schema file is required" << std::endl;
    return 1;
  }

  ngn::Schema schema = ngn::Schema::FromFile(schema_file);
  ASSERT(!schema.Fields().empty());

  ngn::CsvReader reader(input);
  ngn::FileWriter writer(output, schema);

  std::vector<ngn::Column> columns;
  for (const auto& field : schema.Fields()) {
    switch (field.type) {
      case ngn::Type::kInt16:
        columns.emplace_back(ngn::ArrayType<ngn::Type::kInt16>());
        break;
      case ngn::Type::kInt32:
        columns.emplace_back(ngn::ArrayType<ngn::Type::kInt32>());
        break;
      case ngn::Type::kInt64:
        columns.emplace_back(ngn::ArrayType<ngn::Type::kInt64>());
        break;
      case ngn::Type::kString:
        columns.emplace_back(ngn::ArrayType<ngn::Type::kString>());
        break;
      case ngn::Type::kDate:
        columns.emplace_back(ngn::ArrayType<ngn::Type::kDate>());
        break;
      case ngn::Type::kTimestamp:
        columns.emplace_back(ngn::ArrayType<ngn::Type::kTimestamp>());
        break;
      case ngn::Type::kChar:
        columns.emplace_back(ngn::ArrayType<ngn::Type::kChar>());
        break;
      default:
        THROW_NOT_IMPLEMENTED;
    }
  }

  for (auto row = reader.ReadNext(); row.has_value(); row = reader.ReadNext()) {
    ASSERT(row->size() == columns.size());

    for (size_t i = 0; i < row.value().size(); ++i) {
      const auto& field = schema.Fields()[i];
      switch (field.type) {
        case ngn::Type::kInt64:
          std::get<ngn::ArrayType<ngn::Type::kInt64>>(columns[i].Values()).emplace_back(std::stoll(row.value()[i]));
          break;
        case ngn::Type::kString:
          std::get<ngn::ArrayType<ngn::Type::kString>>(columns[i].Values()).emplace_back(row.value()[i]);
          break;
        case ngn::Type::kDate:
          // TODO(gmusya): parse date
          std::get<ngn::ArrayType<ngn::Type::kDate>>(columns[i].Values()).emplace_back(ngn::Date{0});
          break;
        case ngn::Type::kTimestamp:
          // TODO(gmusya): parse timestamp
          std::get<ngn::ArrayType<ngn::Type::kTimestamp>>(columns[i].Values()).emplace_back(ngn::Timestamp{0});
          break;
        case ngn::Type::kChar:
          std::get<ngn::ArrayType<ngn::Type::kChar>>(columns[i].Values()).emplace_back(row.value()[i][0]);
          break;
        case ngn::Type::kInt16:
          std::get<ngn::ArrayType<ngn::Type::kInt16>>(columns[i].Values()).emplace_back(std::stoll(row.value()[i]));
          break;
        case ngn::Type::kInt32:
          std::get<ngn::ArrayType<ngn::Type::kInt32>>(columns[i].Values()).emplace_back(std::stoll(row.value()[i]));
          break;
        default:
          THROW_NOT_IMPLEMENTED;
      }
    }
  }

  for (auto& column : columns) {
    writer.AppendColumn(std::move(column));
  }

  std::move(writer).Finalize();

  return 0;
}
