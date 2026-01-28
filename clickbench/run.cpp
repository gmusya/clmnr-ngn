#include <chrono>
#include <exception>
#include <filesystem>
#include <iostream>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/log/log.h"
#include "src/core/csv.h"
#include "src/execution/aggregation.h"
#include "src/execution/expression.h"
#include "src/execution/operator.h"

ABSL_FLAG(std::string, input, "", "Input columnar file (.clmnr)");
ABSL_FLAG(std::string, schema, "", "Schema file (.schema)");
ABSL_FLAG(std::string, output_dir, "", "Output directory for CSV results. Files will be named q{i}.csv");
ABSL_FLAG(std::string, queries, "", "Comma-separated list of queries to run (e.g., '0,5,10' or 'Q0,Q5,Q10')");
ABSL_FLAG(std::string, skip, "", "Comma-separated list of queries to skip (e.g., '0,5,10' or 'Q0,Q5,Q10')");
ABSL_FLAG(int32_t, from, -1, "First query index to run (inclusive)");
ABSL_FLAG(int32_t, to, -1, "Last query index to run (inclusive)");

namespace {

std::set<int> ParseQueryList(const std::string& str) {
  std::set<int> result;
  if (str.empty()) {
    return result;
  }
  std::stringstream ss(str);
  std::string token;
  while (std::getline(ss, token, ',')) {
    // Remove leading/trailing whitespace
    size_t start = token.find_first_not_of(" \t");
    size_t end = token.find_last_not_of(" \t");
    if (start == std::string::npos) {
      continue;
    }
    token = token.substr(start, end - start + 1);
    // Remove 'Q' or 'q' prefix if present
    if (!token.empty() && (token[0] == 'Q' || token[0] == 'q')) {
      token = token.substr(1);
    }
    try {
      result.insert(std::stoi(token));
    } catch (...) {
      std::cerr << "Warning: ignoring invalid query number: " << token << "\n";
    }
  }
  return result;
}

bool ShouldRunQuery(size_t index, const std::set<int>& only_queries, const std::set<int>& skip_queries, int32_t from,
                    int32_t to) {
  int idx = static_cast<int>(index);

  // Check --skip first
  if (skip_queries.count(idx)) {
    return false;
  }

  // If --queries is specified, only run those
  if (!only_queries.empty()) {
    return only_queries.count(idx) > 0;
  }

  // Check --from/--to range
  if (from >= 0 && idx < from) {
    return false;
  }
  if (to >= 0 && idx > to) {
    return false;
  }

  return true;
}

}  // namespace

namespace ngn {

struct QueryInfo {
  std::shared_ptr<ngn::Operator> plan;
  std::string name;
};

class QueryMaker {
 public:
  QueryMaker(std::string input, ngn::Schema schema) : input_(std::move(input)), schema_(std::move(schema)) {}

  // Helper to build schema from column names
  Schema S(std::initializer_list<std::string> names) {
    std::vector<Field> fields;
    for (const auto& name : names) {
      for (const auto& f : schema_.Fields()) {
        if (f.name == name) {
          fields.push_back(f);
          break;
        }
      }
    }
    return Schema(std::move(fields));
  }

  QueryInfo MakeQ0() {
    // SELECT COUNT(*) FROM hits;

    std::shared_ptr<Operator> plan = MakeCountTable(input_, "count");

    return QueryInfo{.plan = plan, .name = "Q0"};
  }

  QueryInfo MakeQ1() {
    // SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;

    std::shared_ptr<Operator> plan = MakeAggregate(
        MakeFilter(MakeScan(input_, S({"AdvEngineID"})),
                   MakeBinary(BinaryFunction::kNotEqual, MakeVariable("AdvEngineID", Type::kInt16),
                              MakeConst(Value(static_cast<int16_t>(0))))),
        MakeAggregation({AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "count"}},
                        {}));

    return QueryInfo{.plan = plan, .name = "Q1"};
  }

  QueryInfo MakeQ2() {
    // SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;

    auto sums = MakeGlobalAggregation(
        MakeScan(input_, S({"AdvEngineID", "ResolutionWidth"})),
        {AggregationUnit{AggregationType::kSum, MakeVariable("AdvEngineID", Type::kInt16), "sum"},
         AggregationUnit{AggregationType::kSum, MakeVariable("ResolutionWidth", Type::kInt16), "total"}});

    auto cnt = MakeCountTable(input_, "count");

    std::shared_ptr<Operator> plan = MakeProject(
        MakeConcat({sums, cnt}), {ProjectionUnit{MakeVariable("sum", Type::kInt64), "sum"},
                                  ProjectionUnit{MakeVariable("count", Type::kInt64), "count"},
                                  ProjectionUnit{MakeBinary(BinaryFunction::kDiv, MakeVariable("total", Type::kInt64),
                                                            MakeVariable("count", Type::kInt64)),
                                                 "total"}});

    return QueryInfo{.plan = plan, .name = "Q2"};
  }

  QueryInfo MakeQ3() {
    // SELECT AVG(UserID) FROM hits;

    auto sum_user =
        MakeGlobalAggregation(MakeScan(input_, S({"UserID"})),
                              {AggregationUnit{AggregationType::kSum, MakeVariable("UserID", Type::kInt64), "sum"}});

    auto cnt = MakeCountTable(input_, "count");

    std::shared_ptr<Operator> plan = MakeProject(
        MakeConcat({sum_user, cnt}),
        {ProjectionUnit{
            MakeBinary(BinaryFunction::kDiv, MakeVariable("sum", Type::kInt128), MakeVariable("count", Type::kInt64)),
            "total"}});

    return QueryInfo{.plan = plan, .name = "Q3"};
  }

  QueryInfo MakeQ4() {
    // SELECT COUNT(DISTINCT UserID) FROM hits;

    std::shared_ptr<Operator> plan = MakeGlobalAggregation(
        MakeScan(input_, S({"UserID"})),
        {AggregationUnit{AggregationType::kDistinct, MakeVariable("UserID", Type::kInt64), "distinct"}});

    return QueryInfo{.plan = plan, .name = "Q4"};
  }

  QueryInfo MakeQ5() {
    // SELECT COUNT(DISTINCT SearchPhrase) FROM hits;

    std::shared_ptr<Operator> plan = MakeAggregate(
        MakeScan(input_, S({"SearchPhrase"})),
        MakeAggregation(
            {AggregationUnit{AggregationType::kDistinct, MakeVariable("SearchPhrase", Type::kString), "distinct"}},
            {}));

    return QueryInfo{.plan = plan, .name = "Q5"};
  }

  QueryInfo MakeQ6() {
    // SELECT MIN(EventDate), MAX(EventDate) FROM hits;

    std::shared_ptr<Operator> plan = MakeAggregate(
        MakeScan(input_, S({"EventDate"})),
        MakeAggregation({AggregationUnit{AggregationType::kMin, MakeVariable("EventDate", Type::kDate), "min"},
                         AggregationUnit{AggregationType::kMax, MakeVariable("EventDate", Type::kDate), "max"}},
                        {}));

    return QueryInfo{.plan = plan, .name = "Q6"};
  }

  QueryInfo MakeQ7() {
    // SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;

    std::shared_ptr<Operator> plan = MakeSort(
        MakeAggregate(MakeFilter(MakeScan(input_, S({"AdvEngineID"})),
                                 MakeBinary(BinaryFunction::kNotEqual, MakeVariable("AdvEngineID", Type::kInt16),
                                            MakeConst(Value(static_cast<int16_t>(0))))),
                      MakeAggregation({AggregationUnit{AggregationType::kCount,
                                                       MakeConst(Value(static_cast<int64_t>(0))), "count"}},
                                      {GroupByUnit{MakeVariable("AdvEngineID", Type::kInt16), "AdvEngineID"}})),
        {SortUnit{MakeVariable("count", Type::kInt64), false}});

    return QueryInfo{.plan = plan, .name = "Q7"};
  }

  QueryInfo MakeQ8() {
    // SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeAggregate(
            MakeScan(input_, S({"RegionID", "UserID"})),
            MakeAggregation({AggregationUnit{AggregationType::kDistinct, MakeVariable("UserID", Type::kInt64), "u"}},
                            {GroupByUnit{MakeVariable("RegionID", Type::kInt32), "RegionID"}})),
        {SortUnit{MakeVariable("u", Type::kInt64), false}}, 10);

    return QueryInfo{.plan = plan, .name = "Q8"};
  }

  QueryInfo MakeQ9() {
    // SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY
    // RegionID ORDER BY c DESC LIMIT 10;

    std::shared_ptr<Operator> plan = MakeProject(
        MakeTopK(
            MakeAggregate(
                MakeScan(input_, S({"RegionID", "AdvEngineID", "ResolutionWidth", "UserID"})),
                MakeAggregation(
                    {AggregationUnit{AggregationType::kSum, MakeVariable("AdvEngineID", Type::kInt16), "sum_adv"},
                     AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "c"},
                     AggregationUnit{AggregationType::kSum, MakeVariable("ResolutionWidth", Type::kInt16), "sum_res"},
                     AggregationUnit{AggregationType::kDistinct, MakeVariable("UserID", Type::kInt64), "distinct_u"}},
                    {GroupByUnit{MakeVariable("RegionID", Type::kInt32), "RegionID"}})),
            {SortUnit{MakeVariable("c", Type::kInt64), false}}, 10),
        {ProjectionUnit{MakeVariable("RegionID", Type::kInt32), "RegionID"},
         ProjectionUnit{MakeVariable("sum_adv", Type::kInt64), "sum_adv"},
         ProjectionUnit{MakeVariable("c", Type::kInt64), "c"},
         ProjectionUnit{
             MakeBinary(BinaryFunction::kDiv, MakeVariable("sum_res", Type::kInt64), MakeVariable("c", Type::kInt64)),
             "avg_res"},
         ProjectionUnit{MakeVariable("distinct_u", Type::kInt64), "distinct_u"}});

    return QueryInfo{.plan = plan, .name = "Q9"};
  }

  QueryInfo MakeQ10() {
    // SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY
    // MobilePhoneModel ORDER BY u DESC LIMIT 10;

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeAggregate(
            MakeFilter(MakeScan(input_, S({"MobilePhoneModel", "UserID"})),
                       MakeBinary(BinaryFunction::kNotEqual, MakeVariable("MobilePhoneModel", Type::kString),
                                  MakeConst(Value(std::string(""))))),
            MakeAggregation({AggregationUnit{AggregationType::kDistinct, MakeVariable("UserID", Type::kInt64), "u"}},
                            {GroupByUnit{MakeVariable("MobilePhoneModel", Type::kString), "MobilePhoneModel"}})),
        {SortUnit{MakeVariable("u", Type::kInt64), false}}, 10);

    return QueryInfo{.plan = plan, .name = "Q10"};
  }

  QueryInfo MakeQ11() {
    // SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY
    // MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeAggregate(
            MakeFilter(MakeScan(input_, S({"MobilePhone", "MobilePhoneModel", "UserID"})),
                       MakeBinary(BinaryFunction::kNotEqual, MakeVariable("MobilePhoneModel", Type::kString),
                                  MakeConst(Value(std::string(""))))),
            MakeAggregation({AggregationUnit{AggregationType::kDistinct, MakeVariable("UserID", Type::kInt64), "u"}},
                            {GroupByUnit{MakeVariable("MobilePhone", Type::kInt16), "MobilePhone"},
                             GroupByUnit{MakeVariable("MobilePhoneModel", Type::kString), "MobilePhoneModel"}})),
        {SortUnit{MakeVariable("u", Type::kInt64), false}}, 10);

    return QueryInfo{.plan = plan, .name = "Q11"};
  }

  QueryInfo MakeQ12() {
    // SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT
    // 10;

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeAggregate(
            MakeFilter(MakeScan(input_, S({"SearchPhrase"})),
                       MakeBinary(BinaryFunction::kNotEqual, MakeVariable("SearchPhrase", Type::kString),
                                  MakeConst(Value(std::string(""))))),
            MakeAggregation({AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "c"}},
                            {GroupByUnit{MakeVariable("SearchPhrase", Type::kString), "SearchPhrase"}})),
        {SortUnit{MakeVariable("c", Type::kInt64), false}}, 10);

    return QueryInfo{.plan = plan, .name = "Q12"};
  }

  QueryInfo MakeQ13() {
    // SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER
    // BY u DESC LIMIT 10;

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeAggregate(
            MakeFilter(MakeScan(input_, S({"SearchPhrase", "UserID"})),
                       MakeBinary(BinaryFunction::kNotEqual, MakeVariable("SearchPhrase", Type::kString),
                                  MakeConst(Value(std::string(""))))),
            MakeAggregation({AggregationUnit{AggregationType::kDistinct, MakeVariable("UserID", Type::kInt64), "u"}},
                            {GroupByUnit{MakeVariable("SearchPhrase", Type::kString), "SearchPhrase"}})),
        {SortUnit{MakeVariable("u", Type::kInt64), false}}, 10);

    return QueryInfo{.plan = plan, .name = "Q13"};
  }

  QueryInfo MakeQ14() {
    // SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID,
    // SearchPhrase ORDER BY c DESC LIMIT 10;

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeAggregate(
            MakeFilter(MakeScan(input_, S({"SearchEngineID", "SearchPhrase"})),
                       MakeBinary(BinaryFunction::kNotEqual, MakeVariable("SearchPhrase", Type::kString),
                                  MakeConst(Value(std::string(""))))),
            MakeAggregation({AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "c"}},
                            {GroupByUnit{MakeVariable("SearchEngineID", Type::kInt16), "SearchEngineID"},
                             GroupByUnit{MakeVariable("SearchPhrase", Type::kString), "SearchPhrase"}})),
        {SortUnit{MakeVariable("c", Type::kInt64), false}}, 10);

    return QueryInfo{.plan = plan, .name = "Q14"};
  }

  QueryInfo MakeQ15() {
    // SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeAggregate(
            MakeScan(input_, S({"UserID"})),
            MakeAggregation({AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "c"}},
                            {GroupByUnit{MakeVariable("UserID", Type::kInt64), "UserID"}})),
        {SortUnit{MakeVariable("c", Type::kInt64), false}}, 10);

    return QueryInfo{.plan = plan, .name = "Q15"};
  }

  QueryInfo MakeQ16() {
    // SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeAggregate(
            MakeScan(input_, S({"UserID", "SearchPhrase"})),
            MakeAggregation({AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "c"}},
                            {GroupByUnit{MakeVariable("UserID", Type::kInt64), "UserID"},
                             GroupByUnit{MakeVariable("SearchPhrase", Type::kString), "SearchPhrase"}})),
        {SortUnit{MakeVariable("c", Type::kInt64), false}}, 10);

    return QueryInfo{.plan = plan, .name = "Q16"};
  }

  QueryInfo MakeQ17() {
    // SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10;

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeAggregate(
            MakeScan(input_, S({"UserID", "SearchPhrase"})),
            MakeAggregation({AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "c"}},
                            {GroupByUnit{MakeVariable("UserID", Type::kInt64), "UserID"},
                             GroupByUnit{MakeVariable("SearchPhrase", Type::kString), "SearchPhrase"}})),
        {SortUnit{MakeVariable("UserID", Type::kInt64), true}}, 10);

    return QueryInfo{.plan = plan, .name = "Q17"};
  }

  QueryInfo MakeQ18() {
    // SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m,
    // SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeAggregate(
            MakeProject(
                MakeScan(input_, S({"UserID", "EventTime", "SearchPhrase"})),
                {ProjectionUnit{MakeVariable("UserID", Type::kInt64), "UserID"},
                 ProjectionUnit{MakeUnary(UnaryFunction::kExtractMinute, MakeVariable("EventTime", Type::kTimestamp)),
                                "m"},
                 ProjectionUnit{MakeVariable("SearchPhrase", Type::kString), "SearchPhrase"}}),
            MakeAggregation({AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "c"}},
                            {GroupByUnit{MakeVariable("UserID", Type::kInt64), "UserID"},
                             GroupByUnit{MakeVariable("m", Type::kInt16), "m"},
                             GroupByUnit{MakeVariable("SearchPhrase", Type::kString), "SearchPhrase"}})),
        {SortUnit{MakeVariable("c", Type::kInt64), false}}, 10);

    return QueryInfo{.plan = plan, .name = "Q18"};
  }

  QueryInfo MakeQ19() {
    // SELECT UserID FROM hits WHERE UserID = 435090932899640449;

    std::vector<ZoneMapPredicate> zm_preds = {
        ZoneMapPredicate::Equal("UserID", Value(static_cast<int64_t>(435090932899640449LL))),
    };

    std::shared_ptr<Operator> plan =
        MakeProject(MakeFilter(MakeScan(input_, S({"UserID"}), std::move(zm_preds)),
                               MakeBinary(BinaryFunction::kEqual, MakeVariable("UserID", Type::kInt64),
                                          MakeConst(Value(static_cast<int64_t>(435090932899640449LL))))),
                    {ProjectionUnit{MakeVariable("UserID", Type::kInt64), "UserID"}});

    return QueryInfo{.plan = plan, .name = "Q19"};
  }

  QueryInfo MakeQ20() {
    // SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';

    std::shared_ptr<Operator> plan = MakeAggregate(
        MakeFilter(MakeScan(input_, S({"URL"})), MakeContains(MakeVariable("URL", Type::kString), "google")),
        MakeAggregation({AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "c"}},
                        {}));

    return QueryInfo{.plan = plan, .name = "Q20"};
  }

  QueryInfo MakeQ21() {
    // SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY
    // SearchPhrase ORDER BY c DESC LIMIT 10;

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeAggregate(
            MakeFilter(MakeScan(input_, S({"URL", "SearchPhrase"})),
                       MakeBinary(BinaryFunction::kAnd, MakeContains(MakeVariable("URL", Type::kString), "google"),
                                  MakeBinary(BinaryFunction::kNotEqual, MakeVariable("SearchPhrase", Type::kString),
                                             MakeConst(Value(std::string("")))))),
            MakeAggregation({AggregationUnit{AggregationType::kMin, MakeVariable("URL", Type::kString), "min_url"},
                             AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "c"}},
                            {GroupByUnit{MakeVariable("SearchPhrase", Type::kString), "SearchPhrase"}})),
        {SortUnit{MakeVariable("c", Type::kInt64), false}}, 10);

    return QueryInfo{.plan = plan, .name = "Q21"};
  }

  QueryInfo MakeQ22() {
    // SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE
    // '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeAggregate(
            MakeFilter(MakeScan(input_, S({"Title", "URL", "SearchPhrase", "UserID"})),
                       MakeBinary(BinaryFunction::kAnd,
                                  MakeBinary(BinaryFunction::kAnd,
                                             MakeContains(MakeVariable("Title", Type::kString), "Google"),
                                             MakeContains(MakeVariable("URL", Type::kString), ".google.", true)),
                                  MakeBinary(BinaryFunction::kNotEqual, MakeVariable("SearchPhrase", Type::kString),
                                             MakeConst(Value(std::string("")))))),
            MakeAggregation(
                {AggregationUnit{AggregationType::kMin, MakeVariable("URL", Type::kString), "min_url"},
                 AggregationUnit{AggregationType::kMin, MakeVariable("Title", Type::kString), "min_title"},
                 AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "c"},
                 AggregationUnit{AggregationType::kDistinct, MakeVariable("UserID", Type::kInt64), "distinct_u"}},
                {GroupByUnit{MakeVariable("SearchPhrase", Type::kString), "SearchPhrase"}})),
        {SortUnit{MakeVariable("c", Type::kInt64), false}}, 10);

    return QueryInfo{.plan = plan, .name = "Q22"};
  }

  QueryInfo MakeQ23() {
    // SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;
    // Note: projecting subset of columns for demonstration

    std::shared_ptr<Operator> plan =
        MakeTopK(MakeProject(MakeFilter(MakeScan(input_, S({"WatchID", "EventTime", "URL", "Title"})),
                                        MakeContains(MakeVariable("URL", Type::kString), "google")),
                             {ProjectionUnit{MakeVariable("WatchID", Type::kInt64), "WatchID"},
                              ProjectionUnit{MakeVariable("EventTime", Type::kTimestamp), "EventTime"},
                              ProjectionUnit{MakeVariable("URL", Type::kString), "URL"},
                              ProjectionUnit{MakeVariable("Title", Type::kString), "Title"}}),
                 {SortUnit{MakeVariable("EventTime", Type::kTimestamp), true}}, 10);

    return QueryInfo{.plan = plan, .name = "Q23"};
  }

  QueryInfo MakeQ24() {
    // SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeProject(MakeFilter(MakeScan(input_, S({"SearchPhrase", "EventTime"})),
                               MakeBinary(BinaryFunction::kNotEqual, MakeVariable("SearchPhrase", Type::kString),
                                          MakeConst(Value(std::string(""))))),
                    {ProjectionUnit{MakeVariable("SearchPhrase", Type::kString), "SearchPhrase"},
                     ProjectionUnit{MakeVariable("EventTime", Type::kTimestamp), "EventTime"}}),
        {SortUnit{MakeVariable("EventTime", Type::kTimestamp), true}}, 10);

    return QueryInfo{.plan = plan, .name = "Q24"};
  }

  QueryInfo MakeQ25() {
    // SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeProject(MakeFilter(MakeScan(input_, S({"SearchPhrase"})),
                               MakeBinary(BinaryFunction::kNotEqual, MakeVariable("SearchPhrase", Type::kString),
                                          MakeConst(Value(std::string(""))))),
                    {ProjectionUnit{MakeVariable("SearchPhrase", Type::kString), "SearchPhrase"}}),
        {SortUnit{MakeVariable("SearchPhrase", Type::kString), true}}, 10);

    return QueryInfo{.plan = plan, .name = "Q25"};
  }

  QueryInfo MakeQ26() {
    // SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10;

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeProject(MakeFilter(MakeScan(input_, S({"SearchPhrase", "EventTime"})),
                               MakeBinary(BinaryFunction::kNotEqual, MakeVariable("SearchPhrase", Type::kString),
                                          MakeConst(Value(std::string(""))))),
                    {ProjectionUnit{MakeVariable("SearchPhrase", Type::kString), "SearchPhrase"},
                     ProjectionUnit{MakeVariable("EventTime", Type::kTimestamp), "EventTime"}}),
        {SortUnit{MakeVariable("EventTime", Type::kTimestamp), true},
         SortUnit{MakeVariable("SearchPhrase", Type::kString), true}},
        10);

    return QueryInfo{.plan = plan, .name = "Q26"};
  }

  QueryInfo MakeQ27() {
    // SELECT CounterID, AVG(STRLEN(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING
    // COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeProject(
            MakeFilter(
                MakeAggregate(
                    MakeProject(MakeFilter(MakeScan(input_, S({"CounterID", "URL"})),
                                           MakeBinary(BinaryFunction::kNotEqual, MakeVariable("URL", Type::kString),
                                                      MakeConst(Value(std::string(""))))),
                                {ProjectionUnit{MakeVariable("CounterID", Type::kInt32), "CounterID"},
                                 ProjectionUnit{MakeUnary(UnaryFunction::kStrLen, MakeVariable("URL", Type::kString)),
                                                "url_len"}}),
                    MakeAggregation(
                        {AggregationUnit{AggregationType::kSum, MakeVariable("url_len", Type::kInt64), "sum_len"},
                         AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "c"}},
                        {GroupByUnit{MakeVariable("CounterID", Type::kInt32), "CounterID"}})),
                MakeBinary(BinaryFunction::kGreater, MakeVariable("c", Type::kInt64),
                           MakeConst(Value(static_cast<int64_t>(100000))))),
            {ProjectionUnit{MakeVariable("CounterID", Type::kInt32), "CounterID"},
             ProjectionUnit{MakeBinary(BinaryFunction::kDiv, MakeVariable("sum_len", Type::kInt128),
                                       MakeVariable("c", Type::kInt64)),
                            "l"},
             ProjectionUnit{MakeVariable("c", Type::kInt64), "c"}}),
        {SortUnit{MakeVariable("l", Type::kInt128), false}}, 25);

    return QueryInfo{.plan = plan, .name = "Q27"};
  }

  QueryInfo MakeQ28() {
    // SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(STRLEN(Referer)) AS l, COUNT(*)
    // AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeProject(
            MakeFilter(
                MakeAggregate(
                    MakeProject(
                        MakeFilter(MakeScan(input_, S({"Referer"})),
                                   MakeBinary(BinaryFunction::kNotEqual, MakeVariable("Referer", Type::kString),
                                              MakeConst(Value(std::string(""))))),
                        {ProjectionUnit{MakeRegexReplace(MakeVariable("Referer", Type::kString),
                                                         R"(^https?://(?:www\.)?([^/]+)/.*$)", "$1"),
                                        "k"},
                         ProjectionUnit{MakeUnary(UnaryFunction::kStrLen, MakeVariable("Referer", Type::kString)),
                                        "ref_len"},
                         ProjectionUnit{MakeVariable("Referer", Type::kString), "Referer"}}),
                    MakeAggregation(
                        {AggregationUnit{AggregationType::kSum, MakeVariable("ref_len", Type::kInt64), "sum_len"},
                         AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "c"},
                         AggregationUnit{AggregationType::kMin, MakeVariable("Referer", Type::kString), "min_ref"}},
                        {GroupByUnit{MakeVariable("k", Type::kString), "k"}})),
                MakeBinary(BinaryFunction::kGreater, MakeVariable("c", Type::kInt64),
                           MakeConst(Value(static_cast<int64_t>(100000))))),
            {ProjectionUnit{MakeVariable("k", Type::kString), "k"},
             ProjectionUnit{MakeBinary(BinaryFunction::kDiv, MakeVariable("sum_len", Type::kInt128),
                                       MakeVariable("c", Type::kInt64)),
                            "l"},
             ProjectionUnit{MakeVariable("c", Type::kInt64), "c"},
             ProjectionUnit{MakeVariable("min_ref", Type::kString), "min_ref"}}),
        {SortUnit{MakeVariable("l", Type::kInt128), false}}, 25);

    return QueryInfo{.plan = plan, .name = "Q28"};
  }

  QueryInfo MakeQ29() {
    // SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), ..., SUM(ResolutionWidth + 89) FROM hits;

    std::vector<AggregationUnit> aggregations{
        AggregationUnit{AggregationType::kSum, MakeVariable("ResolutionWidth", Type::kInt16), "sum_width"},
        AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "c"},
    };

    std::vector<ProjectionUnit> projections;
    projections.reserve(90);
    for (int i = 0; i < 90; ++i) {
      std::string out_name = "s" + std::to_string(i);
      if (i == 0) {
        projections.push_back(ProjectionUnit{MakeVariable("sum_width", Type::kInt64), out_name});
        continue;
      }

      projections.push_back(ProjectionUnit{MakeBinary(BinaryFunction::kAdd, MakeVariable("sum_width", Type::kInt64),
                                                      MakeBinary(BinaryFunction::kMult, MakeVariable("c", Type::kInt64),
                                                                 MakeConst(Value(static_cast<int64_t>(i))))),
                                           out_name});
    }

    std::shared_ptr<Operator> plan = MakeProject(
        MakeAggregate(MakeScan(input_, S({"ResolutionWidth"})), MakeAggregation(std::move(aggregations), {})),
        std::move(projections));

    return QueryInfo{.plan = plan, .name = "Q29"};
  }

  QueryInfo MakeQ30() {
    // SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase
    // <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeProject(
            MakeAggregate(
                MakeFilter(
                    MakeScan(input_, S({"SearchPhrase", "SearchEngineID", "ClientIP", "IsRefresh", "ResolutionWidth"})),
                    MakeBinary(BinaryFunction::kNotEqual, MakeVariable("SearchPhrase", Type::kString),
                               MakeConst(Value(std::string(""))))),
                MakeAggregation(
                    {AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "c"},
                     AggregationUnit{AggregationType::kSum, MakeVariable("IsRefresh", Type::kInt16), "sum_refresh"},
                     AggregationUnit{AggregationType::kSum, MakeVariable("ResolutionWidth", Type::kInt16), "sum_width"},
                     AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "cnt_width"}},
                    {GroupByUnit{MakeVariable("SearchEngineID", Type::kInt16), "SearchEngineID"},
                     GroupByUnit{MakeVariable("ClientIP", Type::kInt32), "ClientIP"}})),
            {ProjectionUnit{MakeVariable("SearchEngineID", Type::kInt16), "SearchEngineID"},
             ProjectionUnit{MakeVariable("ClientIP", Type::kInt32), "ClientIP"},
             ProjectionUnit{MakeVariable("c", Type::kInt64), "c"},
             ProjectionUnit{MakeVariable("sum_refresh", Type::kInt64), "sum_refresh"},
             ProjectionUnit{MakeBinary(BinaryFunction::kDiv, MakeVariable("sum_width", Type::kInt64),
                                       MakeVariable("cnt_width", Type::kInt64)),
                            "avg_width"}}),
        {SortUnit{MakeVariable("c", Type::kInt64), false}}, 10);

    return QueryInfo{.plan = plan, .name = "Q30"};
  }

  QueryInfo MakeQ31() {
    // SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> ''
    // GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeProject(
            MakeAggregate(
                MakeFilter(MakeScan(input_, S({"SearchPhrase", "WatchID", "ClientIP", "IsRefresh", "ResolutionWidth"})),
                           MakeBinary(BinaryFunction::kNotEqual, MakeVariable("SearchPhrase", Type::kString),
                                      MakeConst(Value(std::string(""))))),
                MakeAggregation(
                    {AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "c"},
                     AggregationUnit{AggregationType::kSum, MakeVariable("IsRefresh", Type::kInt16), "sum_refresh"},
                     AggregationUnit{AggregationType::kSum, MakeVariable("ResolutionWidth", Type::kInt16), "sum_width"},
                     AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "cnt_width"}},
                    {GroupByUnit{MakeVariable("WatchID", Type::kInt64), "WatchID"},
                     GroupByUnit{MakeVariable("ClientIP", Type::kInt32), "ClientIP"}})),
            {ProjectionUnit{MakeVariable("WatchID", Type::kInt64), "WatchID"},
             ProjectionUnit{MakeVariable("ClientIP", Type::kInt32), "ClientIP"},
             ProjectionUnit{MakeVariable("c", Type::kInt64), "c"},
             ProjectionUnit{MakeVariable("sum_refresh", Type::kInt64), "sum_refresh"},
             ProjectionUnit{MakeBinary(BinaryFunction::kDiv, MakeVariable("sum_width", Type::kInt64),
                                       MakeVariable("cnt_width", Type::kInt64)),
                            "avg_width"}}),
        {SortUnit{MakeVariable("c", Type::kInt64), false}}, 10);

    return QueryInfo{.plan = plan, .name = "Q31"};
  }

  QueryInfo MakeQ32() {
    // SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID,
    // ClientIP ORDER BY c DESC LIMIT 10;

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeProject(
            MakeAggregateCompact(
                MakeScan(input_, S({"WatchID", "ClientIP", "IsRefresh", "ResolutionWidth"})),
                MakeAggregation(
                    {AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "c"},
                     AggregationUnit{AggregationType::kSum, MakeVariable("IsRefresh", Type::kInt16), "sum_refresh"},
                     AggregationUnit{AggregationType::kSum, MakeVariable("ResolutionWidth", Type::kInt16), "sum_width"},
                     AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "cnt_width"}},
                    {GroupByUnit{MakeVariable("WatchID", Type::kInt64), "WatchID"},
                     GroupByUnit{MakeVariable("ClientIP", Type::kInt32), "ClientIP"}})),
            {ProjectionUnit{MakeVariable("WatchID", Type::kInt64), "WatchID"},
             ProjectionUnit{MakeVariable("ClientIP", Type::kInt32), "ClientIP"},
             ProjectionUnit{MakeVariable("c", Type::kInt64), "c"},
             ProjectionUnit{MakeVariable("sum_refresh", Type::kInt64), "sum_refresh"},
             ProjectionUnit{MakeBinary(BinaryFunction::kDiv, MakeVariable("sum_width", Type::kInt64),
                                       MakeVariable("cnt_width", Type::kInt64)),
                            "avg_width"}}),
        {SortUnit{MakeVariable("c", Type::kInt64), false}}, 10);

    return QueryInfo{.plan = plan, .name = "Q32"};
  }

  QueryInfo MakeQ33() {
    // SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeAggregate(
            MakeScan(input_, S({"URL"})),
            MakeAggregation({AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "c"}},
                            {GroupByUnit{MakeVariable("URL", Type::kString), "URL"}})),
        {SortUnit{MakeVariable("c", Type::kInt64), false}}, 10);

    return QueryInfo{.plan = plan, .name = "Q33"};
  }

  QueryInfo MakeQ34() {
    // SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeProject(MakeAggregate(MakeScan(input_, S({"URL"})),
                                  MakeAggregation({AggregationUnit{AggregationType::kCount,
                                                                   MakeConst(Value(static_cast<int64_t>(0))), "c"}},
                                                  {GroupByUnit{MakeVariable("URL", Type::kString), "URL"}})),
                    {ProjectionUnit{MakeConst(Value(static_cast<int64_t>(1))), "const_1"},
                     ProjectionUnit{MakeVariable("URL", Type::kString), "URL"},
                     ProjectionUnit{MakeVariable("c", Type::kInt64), "c"}}),
        {SortUnit{MakeVariable("c", Type::kInt64), false}}, 10);

    return QueryInfo{.plan = plan, .name = "Q34"};
  }

  QueryInfo MakeQ35() {
    // SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP -
    // 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10;

    std::shared_ptr<Operator> plan = MakeProject(
        MakeTopK(MakeAggregate(MakeScan(input_, S({"ClientIP"})),
                               MakeAggregation({AggregationUnit{AggregationType::kCount,
                                                                MakeConst(Value(static_cast<int64_t>(0))), "c"}},
                                               {GroupByUnit{MakeVariable("ClientIP", Type::kInt32), "ClientIP"}})),
                 {SortUnit{MakeVariable("c", Type::kInt64), false}}, 10),
        {ProjectionUnit{MakeVariable("ClientIP", Type::kInt32), "ClientIP"},
         ProjectionUnit{MakeBinary(BinaryFunction::kSub, MakeVariable("ClientIP", Type::kInt32),
                                   MakeConst(Value(static_cast<int32_t>(1)))),
                        "ClientIP_1"},
         ProjectionUnit{MakeBinary(BinaryFunction::kSub, MakeVariable("ClientIP", Type::kInt32),
                                   MakeConst(Value(static_cast<int32_t>(2)))),
                        "ClientIP_2"},
         ProjectionUnit{MakeBinary(BinaryFunction::kSub, MakeVariable("ClientIP", Type::kInt32),
                                   MakeConst(Value(static_cast<int32_t>(3)))),
                        "ClientIP_3"},
         ProjectionUnit{MakeVariable("c", Type::kInt64), "c"}});

    return QueryInfo{.plan = plan, .name = "Q35"};
  }

  QueryInfo MakeQ36() {
    // SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <=
    // '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10;

    auto filter_cond = MakeBinary(
        BinaryFunction::kAnd,
        MakeBinary(
            BinaryFunction::kAnd,
            MakeBinary(BinaryFunction::kAnd,
                       MakeBinary(BinaryFunction::kAnd,
                                  MakeBinary(BinaryFunction::kAnd,
                                             MakeBinary(BinaryFunction::kEqual, MakeVariable("CounterID", Type::kInt32),
                                                        MakeConst(Value(static_cast<int32_t>(62)))),
                                             MakeBinary(BinaryFunction::kGreaterOrEqual,
                                                        MakeVariable("EventDate", Type::kDate),
                                                        MakeConst(Value(Date{15887})))),
                                  MakeBinary(BinaryFunction::kLessOrEqual, MakeVariable("EventDate", Type::kDate),
                                             MakeConst(Value(Date{15917})))),
                       MakeBinary(BinaryFunction::kEqual, MakeVariable("DontCountHits", Type::kInt16),
                                  MakeConst(Value(static_cast<int16_t>(0))))),
            MakeBinary(BinaryFunction::kEqual, MakeVariable("IsRefresh", Type::kInt16),
                       MakeConst(Value(static_cast<int16_t>(0))))),
        MakeBinary(BinaryFunction::kNotEqual, MakeVariable("URL", Type::kString), MakeConst(Value(std::string("")))));

    // Zone map predicates for CounterID = 62 and EventDate range
    std::vector<ZoneMapPredicate> zm_preds = {
        ZoneMapPredicate::Equal("CounterID", Value(static_cast<int32_t>(62))),
        ZoneMapPredicate::Range("EventDate", Value(Date{15887}), Value(Date{15917}))};

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeAggregate(
            MakeFilter(MakeScan(input_, S({"CounterID", "EventDate", "DontCountHits", "IsRefresh", "URL"}), zm_preds),
                       filter_cond),
            MakeAggregation(
                {AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "PageViews"}},
                {GroupByUnit{MakeVariable("URL", Type::kString), "URL"}})),
        {SortUnit{MakeVariable("PageViews", Type::kInt64), false}}, 10);

    return QueryInfo{.plan = plan, .name = "Q36"};
  }

  QueryInfo MakeQ37() {
    // SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <=
    // '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT
    // 10;

    auto filter_cond = MakeBinary(
        BinaryFunction::kAnd,
        MakeBinary(
            BinaryFunction::kAnd,
            MakeBinary(BinaryFunction::kAnd,
                       MakeBinary(BinaryFunction::kAnd,
                                  MakeBinary(BinaryFunction::kAnd,
                                             MakeBinary(BinaryFunction::kEqual, MakeVariable("CounterID", Type::kInt32),
                                                        MakeConst(Value(static_cast<int32_t>(62)))),
                                             MakeBinary(BinaryFunction::kGreaterOrEqual,
                                                        MakeVariable("EventDate", Type::kDate),
                                                        MakeConst(Value(Date{15887})))),
                                  MakeBinary(BinaryFunction::kLessOrEqual, MakeVariable("EventDate", Type::kDate),
                                             MakeConst(Value(Date{15917})))),
                       MakeBinary(BinaryFunction::kEqual, MakeVariable("DontCountHits", Type::kInt16),
                                  MakeConst(Value(static_cast<int16_t>(0))))),
            MakeBinary(BinaryFunction::kEqual, MakeVariable("IsRefresh", Type::kInt16),
                       MakeConst(Value(static_cast<int16_t>(0))))),
        MakeBinary(BinaryFunction::kNotEqual, MakeVariable("Title", Type::kString), MakeConst(Value(std::string("")))));

    // Zone map predicates for CounterID = 62 and EventDate range
    std::vector<ZoneMapPredicate> zm_preds = {
        ZoneMapPredicate::Equal("CounterID", Value(static_cast<int32_t>(62))),
        ZoneMapPredicate::Range("EventDate", Value(Date{15887}), Value(Date{15917}))};

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeAggregate(
            MakeFilter(MakeScan(input_, S({"CounterID", "EventDate", "DontCountHits", "IsRefresh", "Title"}), zm_preds),
                       filter_cond),
            MakeAggregation(
                {AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "PageViews"}},
                {GroupByUnit{MakeVariable("Title", Type::kString), "Title"}})),
        {SortUnit{MakeVariable("PageViews", Type::kInt64), false}}, 10);

    return QueryInfo{.plan = plan, .name = "Q37"};
  }

  QueryInfo MakeQ38() {
    // SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <=
    // '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10
    // OFFSET 1000;

    auto filter_cond = MakeBinary(
        BinaryFunction::kAnd,
        MakeBinary(
            BinaryFunction::kAnd,
            MakeBinary(BinaryFunction::kAnd,
                       MakeBinary(BinaryFunction::kAnd,
                                  MakeBinary(BinaryFunction::kAnd,
                                             MakeBinary(BinaryFunction::kEqual, MakeVariable("CounterID", Type::kInt32),
                                                        MakeConst(Value(static_cast<int32_t>(62)))),
                                             MakeBinary(BinaryFunction::kGreaterOrEqual,
                                                        MakeVariable("EventDate", Type::kDate),
                                                        MakeConst(Value(Date{15887})))),
                                  MakeBinary(BinaryFunction::kLessOrEqual, MakeVariable("EventDate", Type::kDate),
                                             MakeConst(Value(Date{15917})))),
                       MakeBinary(BinaryFunction::kEqual, MakeVariable("IsRefresh", Type::kInt16),
                                  MakeConst(Value(static_cast<int16_t>(0))))),
            MakeBinary(BinaryFunction::kNotEqual, MakeVariable("IsLink", Type::kInt16),
                       MakeConst(Value(static_cast<int16_t>(0))))),
        MakeBinary(BinaryFunction::kEqual, MakeVariable("IsDownload", Type::kInt16),
                   MakeConst(Value(static_cast<int16_t>(0)))));

    // Zone map predicates for CounterID = 62 and EventDate range
    std::vector<ZoneMapPredicate> zm_preds = {
        ZoneMapPredicate::Equal("CounterID", Value(static_cast<int32_t>(62))),
        ZoneMapPredicate::Range("EventDate", Value(Date{15887}), Value(Date{15917}))};

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeAggregate(
            MakeFilter(
                MakeScan(input_, S({"CounterID", "EventDate", "IsRefresh", "IsLink", "IsDownload", "URL"}), zm_preds),
                filter_cond),
            MakeAggregation(
                {AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "PageViews"}},
                {GroupByUnit{MakeVariable("URL", Type::kString), "URL"}})),
        {SortUnit{MakeVariable("PageViews", Type::kInt64), false}}, 10, 1000);

    return QueryInfo{.plan = plan, .name = "Q38"};
  }

  QueryInfo MakeQ39() {
    // SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN
    // Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >=
    // '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID,
    // AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;

    auto filter_cond = MakeBinary(
        BinaryFunction::kAnd,
        MakeBinary(BinaryFunction::kAnd,
                   MakeBinary(BinaryFunction::kAnd,
                              MakeBinary(BinaryFunction::kEqual, MakeVariable("CounterID", Type::kInt32),
                                         MakeConst(Value(static_cast<int32_t>(62)))),
                              MakeBinary(BinaryFunction::kGreaterOrEqual, MakeVariable("EventDate", Type::kDate),
                                         MakeConst(Value(Date{15887})))),
                   MakeBinary(BinaryFunction::kLessOrEqual, MakeVariable("EventDate", Type::kDate),
                              MakeConst(Value(Date{15917})))),
        MakeBinary(BinaryFunction::kEqual, MakeVariable("IsRefresh", Type::kInt16),
                   MakeConst(Value(static_cast<int16_t>(0)))));

    auto case_condition = MakeBinary(BinaryFunction::kAnd,
                                     MakeBinary(BinaryFunction::kEqual, MakeVariable("SearchEngineID", Type::kInt16),
                                                MakeConst(Value(static_cast<int16_t>(0)))),
                                     MakeBinary(BinaryFunction::kEqual, MakeVariable("AdvEngineID", Type::kInt16),
                                                MakeConst(Value(static_cast<int16_t>(0)))));

    // Zone map predicates for CounterID = 62 and EventDate range
    std::vector<ZoneMapPredicate> zm_preds = {
        ZoneMapPredicate::Equal("CounterID", Value(static_cast<int32_t>(62))),
        ZoneMapPredicate::Range("EventDate", Value(Date{15887}), Value(Date{15917}))};

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeAggregate(MakeProject(MakeFilter(MakeScan(input_,
                                                      S({"CounterID", "EventDate", "IsRefresh", "TraficSourceID",
                                                         "SearchEngineID", "AdvEngineID", "Referer", "URL"}),
                                                      zm_preds),
                                             filter_cond),
                                  {ProjectionUnit{MakeVariable("TraficSourceID", Type::kInt16), "TraficSourceID"},
                                   ProjectionUnit{MakeVariable("SearchEngineID", Type::kInt16), "SearchEngineID"},
                                   ProjectionUnit{MakeVariable("AdvEngineID", Type::kInt16), "AdvEngineID"},
                                   ProjectionUnit{MakeCase(case_condition, MakeVariable("Referer", Type::kString),
                                                           MakeConst(Value(std::string("")))),
                                                  "Src"},
                                   ProjectionUnit{MakeVariable("URL", Type::kString), "Dst"}}),
                      MakeAggregation({AggregationUnit{AggregationType::kCount,
                                                       MakeConst(Value(static_cast<int64_t>(0))), "PageViews"}},
                                      {GroupByUnit{MakeVariable("TraficSourceID", Type::kInt16), "TraficSourceID"},
                                       GroupByUnit{MakeVariable("SearchEngineID", Type::kInt16), "SearchEngineID"},
                                       GroupByUnit{MakeVariable("AdvEngineID", Type::kInt16), "AdvEngineID"},
                                       GroupByUnit{MakeVariable("Src", Type::kString), "Src"},
                                       GroupByUnit{MakeVariable("Dst", Type::kString), "Dst"}})),
        {SortUnit{MakeVariable("PageViews", Type::kInt64), false}}, 10, 1000);

    return QueryInfo{.plan = plan, .name = "Q39"};
  }

  QueryInfo MakeQ40() {
    // SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND
    // EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465
    // GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100;

    auto filter_cond = MakeBinary(
        BinaryFunction::kAnd,
        MakeBinary(
            BinaryFunction::kAnd,
            MakeBinary(BinaryFunction::kAnd,
                       MakeBinary(BinaryFunction::kAnd,
                                  MakeBinary(BinaryFunction::kAnd,
                                             MakeBinary(BinaryFunction::kEqual, MakeVariable("CounterID", Type::kInt32),
                                                        MakeConst(Value(static_cast<int32_t>(62)))),
                                             MakeBinary(BinaryFunction::kGreaterOrEqual,
                                                        MakeVariable("EventDate", Type::kDate),
                                                        MakeConst(Value(Date{15887})))),
                                  MakeBinary(BinaryFunction::kLessOrEqual, MakeVariable("EventDate", Type::kDate),
                                             MakeConst(Value(Date{15917})))),
                       MakeBinary(BinaryFunction::kEqual, MakeVariable("IsRefresh", Type::kInt16),
                                  MakeConst(Value(static_cast<int16_t>(0))))),
            MakeBinary(BinaryFunction::kOr,
                       MakeBinary(BinaryFunction::kEqual, MakeVariable("TraficSourceID", Type::kInt16),
                                  MakeConst(Value(static_cast<int16_t>(-1)))),
                       MakeBinary(BinaryFunction::kEqual, MakeVariable("TraficSourceID", Type::kInt16),
                                  MakeConst(Value(static_cast<int16_t>(6)))))),
        MakeBinary(BinaryFunction::kEqual, MakeVariable("RefererHash", Type::kInt64),
                   MakeConst(Value(static_cast<int64_t>(3594120000172545465LL)))));

    // Zone map predicates for CounterID = 62 and EventDate range
    std::vector<ZoneMapPredicate> zm_preds = {
        ZoneMapPredicate::Equal("CounterID", Value(static_cast<int32_t>(62))),
        ZoneMapPredicate::Range("EventDate", Value(Date{15887}), Value(Date{15917}))};

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeAggregate(
            MakeFilter(
                MakeScan(input_, S({"CounterID", "EventDate", "IsRefresh", "TraficSourceID", "RefererHash", "URLHash"}),
                         zm_preds),
                filter_cond),
            MakeAggregation(
                {AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "PageViews"}},
                {GroupByUnit{MakeVariable("URLHash", Type::kInt64), "URLHash"},
                 GroupByUnit{MakeVariable("EventDate", Type::kDate), "EventDate"}})),
        {SortUnit{MakeVariable("PageViews", Type::kInt64), false}}, 10, 100);

    return QueryInfo{.plan = plan, .name = "Q40"};
  }

  QueryInfo MakeQ41() {
    // SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate
    // >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash =
    // 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000;

    auto filter_cond = MakeBinary(
        BinaryFunction::kAnd,
        MakeBinary(
            BinaryFunction::kAnd,
            MakeBinary(BinaryFunction::kAnd,
                       MakeBinary(BinaryFunction::kAnd,
                                  MakeBinary(BinaryFunction::kAnd,
                                             MakeBinary(BinaryFunction::kEqual, MakeVariable("CounterID", Type::kInt32),
                                                        MakeConst(Value(static_cast<int32_t>(62)))),
                                             MakeBinary(BinaryFunction::kGreaterOrEqual,
                                                        MakeVariable("EventDate", Type::kDate),
                                                        MakeConst(Value(Date{15887})))),
                                  MakeBinary(BinaryFunction::kLessOrEqual, MakeVariable("EventDate", Type::kDate),
                                             MakeConst(Value(Date{15917})))),
                       MakeBinary(BinaryFunction::kEqual, MakeVariable("IsRefresh", Type::kInt16),
                                  MakeConst(Value(static_cast<int16_t>(0))))),
            MakeBinary(BinaryFunction::kEqual, MakeVariable("DontCountHits", Type::kInt16),
                       MakeConst(Value(static_cast<int16_t>(0))))),
        MakeBinary(BinaryFunction::kEqual, MakeVariable("URLHash", Type::kInt64),
                   MakeConst(Value(static_cast<int64_t>(2868770270353813622LL)))));

    // Zone map predicates for CounterID = 62 and EventDate range
    std::vector<ZoneMapPredicate> zm_preds = {
        ZoneMapPredicate::Equal("CounterID", Value(static_cast<int32_t>(62))),
        ZoneMapPredicate::Range("EventDate", Value(Date{15887}), Value(Date{15917}))};

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeAggregate(
            MakeFilter(MakeScan(input_,
                                S({"CounterID", "EventDate", "IsRefresh", "DontCountHits", "URLHash",
                                   "WindowClientWidth", "WindowClientHeight"}),
                                zm_preds),
                       filter_cond),
            MakeAggregation(
                {AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "PageViews"}},
                {GroupByUnit{MakeVariable("WindowClientWidth", Type::kInt16), "WindowClientWidth"},
                 GroupByUnit{MakeVariable("WindowClientHeight", Type::kInt16), "WindowClientHeight"}})),
        {SortUnit{MakeVariable("PageViews", Type::kInt64), false}}, 10, 10000);

    return QueryInfo{.plan = plan, .name = "Q41"};
  }

  QueryInfo MakeQ42() {
    // SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate
    // >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY
    // DATE_TRUNC('minute', EventTime) ORDER BY DATE_TRUNC('minute', EventTime) LIMIT 10 OFFSET 1000;

    auto filter_cond = MakeBinary(
        BinaryFunction::kAnd,
        MakeBinary(
            BinaryFunction::kAnd,
            MakeBinary(BinaryFunction::kAnd,
                       MakeBinary(BinaryFunction::kAnd,
                                  MakeBinary(BinaryFunction::kEqual, MakeVariable("CounterID", Type::kInt32),
                                             MakeConst(Value(static_cast<int32_t>(62)))),
                                  MakeBinary(BinaryFunction::kGreaterOrEqual, MakeVariable("EventDate", Type::kDate),
                                             MakeConst(Value(Date{15900})))),
                       MakeBinary(BinaryFunction::kLessOrEqual, MakeVariable("EventDate", Type::kDate),
                                  MakeConst(Value(Date{15901})))),
            MakeBinary(BinaryFunction::kEqual, MakeVariable("IsRefresh", Type::kInt16),
                       MakeConst(Value(static_cast<int16_t>(0))))),
        MakeBinary(BinaryFunction::kEqual, MakeVariable("DontCountHits", Type::kInt16),
                   MakeConst(Value(static_cast<int16_t>(0)))));

    // Zone map predicates for CounterID = 62 and EventDate range (2013-07-14 to 2013-07-15)
    std::vector<ZoneMapPredicate> zm_preds = {
        ZoneMapPredicate::Equal("CounterID", Value(static_cast<int32_t>(62))),
        ZoneMapPredicate::Range("EventDate", Value(Date{15900}), Value(Date{15901}))};

    std::shared_ptr<Operator> plan = MakeTopK(
        MakeAggregate(
            MakeProject(
                MakeFilter(MakeScan(input_, S({"CounterID", "EventDate", "IsRefresh", "DontCountHits", "EventTime"}),
                                    zm_preds),
                           filter_cond),
                {ProjectionUnit{MakeUnary(UnaryFunction::kDateTruncMinute, MakeVariable("EventTime", Type::kTimestamp)),
                                "M"}}),
            MakeAggregation(
                {AggregationUnit{AggregationType::kCount, MakeConst(Value(static_cast<int64_t>(0))), "PageViews"}},
                {GroupByUnit{MakeVariable("M", Type::kTimestamp), "M"}})),
        {SortUnit{MakeVariable("M", Type::kTimestamp), true}}, 10, 1000);

    return QueryInfo{.plan = plan, .name = "Q42"};
  }

 private:
  std::string input_;
  ngn::Schema schema_;
};

}  // namespace ngn

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);

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
      query_maker.MakeQ0(),  query_maker.MakeQ1(),  query_maker.MakeQ2(),  query_maker.MakeQ3(),  query_maker.MakeQ4(),
      query_maker.MakeQ5(),  query_maker.MakeQ6(),  query_maker.MakeQ7(),  query_maker.MakeQ8(),  query_maker.MakeQ9(),
      query_maker.MakeQ10(), query_maker.MakeQ11(), query_maker.MakeQ12(), query_maker.MakeQ13(), query_maker.MakeQ14(),
      query_maker.MakeQ15(), query_maker.MakeQ16(), query_maker.MakeQ17(), query_maker.MakeQ18(), query_maker.MakeQ19(),
      query_maker.MakeQ20(), query_maker.MakeQ21(), query_maker.MakeQ22(), query_maker.MakeQ23(), query_maker.MakeQ24(),
      query_maker.MakeQ25(), query_maker.MakeQ26(), query_maker.MakeQ27(), query_maker.MakeQ28(), query_maker.MakeQ29(),
      query_maker.MakeQ30(), query_maker.MakeQ31(), query_maker.MakeQ32(), query_maker.MakeQ33(), query_maker.MakeQ34(),
      query_maker.MakeQ35(), query_maker.MakeQ36(), query_maker.MakeQ37(), query_maker.MakeQ38(), query_maker.MakeQ39(),
      query_maker.MakeQ40(), query_maker.MakeQ41(), query_maker.MakeQ42()};

  const auto only_queries = ParseQueryList(absl::GetFlag(FLAGS_queries));
  const auto skip_queries = ParseQueryList(absl::GetFlag(FLAGS_skip));
  const int32_t from = absl::GetFlag(FLAGS_from);
  const int32_t to = absl::GetFlag(FLAGS_to);

  for (size_t i = 0; i < queries.size(); ++i) {
    if (!ShouldRunQuery(i, only_queries, skip_queries, from, to)) {
      continue;
    }

    auto& q = queries[i];
    LOG(INFO) << "Running " << q.name;
    try {
      const auto start = std::chrono::steady_clock::now();

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

      const auto end = std::chrono::steady_clock::now();
      const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      LOG(INFO) << q.name << " completed in " << elapsed_ms << " ms";
    } catch (const std::exception& e) {
      LOG(ERROR) << q.name << " failed: " << e.what();
    }
  }

  return 0;
}
