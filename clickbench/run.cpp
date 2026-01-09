#include <exception>
#include <iostream>
#include <memory>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/log/initialize.h"
#include "absl/log/log.h"
#include "src/execution/aggregation.h"
#include "src/execution/operator.h"

ABSL_FLAG(std::string, input, "", "Input columnar file (.clmnr)");
ABSL_FLAG(std::string, schema, "", "Schema file (.schema)");

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

  ngn::QueryMaker query_maker(input, ngn::Schema::FromFile(schema));

  std::vector<ngn::QueryInfo> queries = {
      query_maker.MakeQ0(),
      query_maker.MakeQ1(),
  };

  for (auto& q : queries) {
    LOG(INFO) << "Running " << q.name;
    try {
      ngn::Execute(q.plan);
      std::cout.flush();
    } catch (const std::exception& e) {
      LOG(ERROR) << q.name << " failed: " << e.what();
    }
  }

  return 0;
}
