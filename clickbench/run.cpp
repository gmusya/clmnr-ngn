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
#include "src/execution/expression.h"
#include "src/execution/operator.h"

ABSL_FLAG(std::string, input, "", "Input columnar file (.clmnr)");
ABSL_FLAG(std::string, schema, "", "Schema file (.schema)");
ABSL_FLAG(std::string, output_dir, "", "Output directory for CSV results. Files will be named q{i}.csv");

namespace ngn {

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
        MakeAggregation({AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "count"}},
                        {}));

    return QueryInfo{.plan = plan, .name = "Q0"};
  }

  QueryInfo MakeQ1() {
    // SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;

    std::shared_ptr<Operator> plan = MakeAggregate(
        MakeFilter(MakeScan(input_, schema_),
                   MakeBinary(BinaryFunction::kNotEqual, MakeVariable("AdvEngineID", Type::kInt16),
                              MakeConst(Value(static_cast<int16_t>(0))))),
        MakeAggregation({AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "count"}},
                        {}));

    return QueryInfo{.plan = plan, .name = "Q1"};
  }

  QueryInfo MakeQ2() {
    // SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;

    std::shared_ptr<Operator> plan = MakeProject(
        MakeAggregate(
            MakeScan(input_, schema_),
            MakeAggregation(
                {AggregationUnit{AggregationType::kSum, MakeVariable("AdvEngineID", Type::kInt16), "sum"},
                 AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "count"},
                 AggregationUnit{AggregationType::kSum, MakeVariable("ResolutionWidth", Type::kInt16), "total"}},
                {})),
        {ProjectionUnit{MakeVariable("sum", Type::kInt64), "sum"},
         ProjectionUnit{MakeVariable("count", Type::kInt64), "count"},
         ProjectionUnit{
             MakeBinary(BinaryFunction::kDiv, MakeVariable("total", Type::kInt64), MakeVariable("count", Type::kInt64)),
             "total"}});

    return QueryInfo{.plan = plan, .name = "Q2"};
  }

  QueryInfo MakeQ3() {
    // SELECT AVG(UserID) FROM hits;

    std::shared_ptr<Operator> plan = MakeProject(
        MakeAggregate(
            MakeScan(input_, schema_),
            MakeAggregation(
                {AggregationUnit{AggregationType::kSum, MakeVariable("UserID", Type::kInt64), "sum"},
                 AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "count"}},
                {})),
        {ProjectionUnit{
            MakeBinary(BinaryFunction::kDiv, MakeVariable("sum", Type::kInt128), MakeVariable("count", Type::kInt64)),
            "total"}});

    return QueryInfo{.plan = plan, .name = "Q3"};
  }

  QueryInfo MakeQ4() {
    // SELECT COUNT(DISTINCT UserID) FROM hits;

    std::shared_ptr<Operator> plan = MakeAggregate(
        MakeScan(input_, schema_),
        MakeAggregation({AggregationUnit{AggregationType::kDistinct, MakeVariable("UserID", Type::kInt64), "distinct"}},
                        {}));

    return QueryInfo{.plan = plan, .name = "Q4"};
  }

  QueryInfo MakeQ5() {
    // SELECT COUNT(DISTINCT SearchPhrase) FROM hits;

    std::shared_ptr<Operator> plan = MakeAggregate(
        MakeScan(input_, schema_),
        MakeAggregation(
            {AggregationUnit{AggregationType::kDistinct, MakeVariable("SearchPhrase", Type::kString), "distinct"}},
            {}));

    return QueryInfo{.plan = plan, .name = "Q5"};
  }

  QueryInfo MakeQ6() {
    // SELECT MIN(EventDate), MAX(EventDate) FROM hits;

    std::shared_ptr<Operator> plan = MakeAggregate(
        MakeScan(input_, schema_),
        MakeAggregation({AggregationUnit{AggregationType::kMin, MakeVariable("EventDate", Type::kDate), "min"},
                         AggregationUnit{AggregationType::kMax, MakeVariable("EventDate", Type::kDate), "max"}},
                        {}));

    return QueryInfo{.plan = plan, .name = "Q6"};
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

  std::vector<ngn::QueryInfo> queries = {query_maker.MakeQ0(), query_maker.MakeQ1(), query_maker.MakeQ2(),
                                         query_maker.MakeQ3(), query_maker.MakeQ4(), query_maker.MakeQ5(),
                                         query_maker.MakeQ6()};

  for (size_t i = 0; i < queries.size(); ++i) {
    auto& q = queries[i];
    LOG(INFO) << "Running " << q.name;
    try {
      const std::filesystem::path out_path = std::filesystem::path(output_dir) / ("q" + std::to_string(i) + ".csv");
      ngn::CsvWriter writer(out_path.string());
      auto stream = ngn::Execute(q.plan);
      while (const auto& batch = stream->Next()) {
        for (int64_t r = 0; r < batch.value()->Rows(); ++r) {
          ngn::CsvWriter::Row row;
          for (const auto& col : batch.value()->Columns()) {
            row.emplace_back(col[r].ToString());
          }
          writer.WriteRow(row);
        }
      }
    } catch (const std::exception& e) {
      LOG(ERROR) << q.name << " failed: " << e.what();
    }
  }

  return 0;
}
