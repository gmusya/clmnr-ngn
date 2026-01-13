#include "src/execution/operator.h"

#include "src/util/assert.h"
#include "src/util/macro.h"

namespace ngn {

Batch Execute(std::shared_ptr<Operator> op) {
  ASSERT(op != nullptr);

  switch (op->type) {
    case OperatorType::kAggregate:
    case OperatorType::kScan:
    case OperatorType::kFilter:
    case OperatorType::kProject:
    case OperatorType::kSort:
    case OperatorType::kLimit:
    default:
      THROW_NOT_IMPLEMENTED;
  }

  THROW_NOT_IMPLEMENTED;
}

}  // namespace ngn
