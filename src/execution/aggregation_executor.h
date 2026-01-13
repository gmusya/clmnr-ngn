#pragma once

#include <memory>

#include "src/execution/aggregation.h"
#include "src/execution/stream.h"

namespace ngn {

std::shared_ptr<Batch> Evaluate(std::shared_ptr<IStream<std::shared_ptr<Batch>>> batch,
                                std::shared_ptr<Aggregation> aggregation);

}  // namespace ngn
