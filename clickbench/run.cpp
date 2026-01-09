#include <exception>
#include <iostream>
#include <memory>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/log/initialize.h"
#include "absl/log/log.h"
#include "src/execution/operator.h"

ABSL_FLAG(std::string, input, "", "Input columnar file (.clmnr)");

struct QueryInfo {
  std::shared_ptr<ngn::Operator> plan;
  std::string name;
};

inline QueryInfo MakeQ0() { return QueryInfo{.plan = std::make_shared<ngn::ScanOperator>(), .name = "Q0"}; }

inline QueryInfo MakeQ1() { return QueryInfo{.plan = std::make_shared<ngn::AggregateOperator>(), .name = "Q1"}; }

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  absl::InitializeLog();

  const std::string input = absl::GetFlag(FLAGS_input);
  if (input.empty()) {
    std::cerr << "--input is required\n";
    return 1;
  }

  std::vector<QueryInfo> queries = {
      MakeQ0(),
      MakeQ1(),
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
