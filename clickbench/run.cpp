#include <exception>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/log/initialize.h"
#include "absl/log/log.h"
#include "src/core/csv.h"
#include "src/execution/aggregation.h"
#include "src/execution/operator.h"

ABSL_FLAG(std::string, input, "", "Input columnar file (.clmnr)");
ABSL_FLAG(std::string, schema, "", "Schema file (.schema)");
ABSL_FLAG(std::string, output_dir, "", "Output directory for CSV results. Files will be named q{i}.csv");

namespace ngn {

static void WriteBatchCsv(const Batch& batch, const std::string& output_path) {
  CsvWriter writer(output_path);

  const auto& fields = batch.GetSchema().Fields();
  const auto& cols = batch.Columns();

  CsvWriter::Row header;
  header.reserve(fields.size());
  for (const auto& f : fields) {
    header.emplace_back(f.name);
  }
  writer.WriteRow(header);

  for (int64_t r = 0; r < batch.Rows(); ++r) {
    CsvWriter::Row row;
    row.reserve(cols.size());
    for (const auto& col : cols) {
      row.emplace_back(col[r].ToString());
    }
    writer.WriteRow(row);
  }
}

struct QueryInfo {
  std::shared_ptr<ngn::Operator> plan;
  std::string name;
};

class QueryMaker {
 public:
  QueryMaker(std::string input, ngn::Schema schema) : input_(std::move(input)), schema_(std::move(schema)) {}

  QueryInfo MakeQ0() {
    // SELECT COUNT(*) FROM hits;

    std::shared_ptr<Operator> plan = MakeAggregate(
        MakeScan(input_, schema_),
        MakeAggregation(
            {Aggregation::AggregationInfo{AggregationType::kCount, MakeVariable("*", Type::kInt64), "count"}}, {}));

    return QueryInfo{.plan = plan, .name = "Q0"};
  }

  QueryInfo MakeQ1() {
    // SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;

    std::shared_ptr<Operator> plan = MakeAggregate(
        MakeFilter(
            MakeScan(input_, schema_),
            MakeBinary(BinaryFunction::kNotEqual, MakeVariable("AdvEngineID", Type::kInt64), MakeConst(Value(0)))),
        MakeAggregation(
            {Aggregation::AggregationInfo{AggregationType::kCount, MakeVariable("*", Type::kInt64), "count"}}, {}));

    return QueryInfo{.plan = plan, .name = "Q1"};
  }

 private:
  std::string input_;
  ngn::Schema schema_;
};

}  // namespace ngn

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  absl::InitializeLog();

  const std::string input = absl::GetFlag(FLAGS_input);
  if (input.empty()) {
    std::cerr << "--input is required\n";
    return 1;
  }

  const std::string schema = absl::GetFlag(FLAGS_schema);
  if (schema.empty()) {
    std::cerr << "--schema is required\n";
    return 1;
  }

  const std::string output_dir = absl::GetFlag(FLAGS_output_dir);
  if (output_dir.empty()) {
    std::cerr << "--output_dir is required\n";
    return 1;
  }
  std::filesystem::create_directories(output_dir);

  ngn::QueryMaker query_maker(input, ngn::Schema::FromFile(schema));

  std::vector<ngn::QueryInfo> queries = {
      query_maker.MakeQ0(),
      query_maker.MakeQ1(),
  };

  for (size_t i = 0; i < queries.size(); ++i) {
    auto& q = queries[i];
    LOG(INFO) << "Running " << q.name;
    try {
      ngn::Batch result = ngn::Execute(q.plan);
      const std::filesystem::path out_path = std::filesystem::path(output_dir) / ("q" + std::to_string(i) + ".csv");
      ngn::WriteBatchCsv(result, out_path.string());
      LOG(INFO) << "Wrote " << out_path.string();
    } catch (const std::exception& e) {
      LOG(ERROR) << q.name << " failed: " << e.what();
    }
  }

  return 0;
}
