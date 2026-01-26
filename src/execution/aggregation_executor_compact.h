#pragma once

#include <memory>

#include "src/execution/aggregation.h"
#include "src/execution/stream.h"

namespace ngn {

// A memory-lean aggregation path intended for very high-cardinality GROUP BY.
// It currently specializes the ClickBench Q32 aggregation shape (two integer keys and simple COUNT/SUMs).
//
// If the aggregation is not recognized as supported, it MUST fall back to the generic Evaluate().
std::shared_ptr<Batch> EvaluateCompact(std::shared_ptr<IStream<std::shared_ptr<Batch>>> stream,
                                       std::shared_ptr<Aggregation> aggregation);

}  // namespace ngn
