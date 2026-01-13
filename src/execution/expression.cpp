#include "src/execution/expression.h"

#include "src/core/column.h"
#include "src/execution/kernel.h"
#include "src/util/macro.h"

namespace ngn {
namespace {

Column EvaluateConst(int64_t rows, std::shared_ptr<Const> expression) {
  return std::visit(
      [rows]<typename T>(const T& value) -> Column {
        if constexpr (std::is_same_v<T, PhysicalType<Type::kBool>>) {
          return Column(ArrayType<Type::kBool>(rows, value));
        } else if constexpr (std::is_same_v<T, PhysicalType<Type::kInt16>>) {
          return Column(ArrayType<Type::kInt16>(rows, value));
        } else if constexpr (std::is_same_v<T, PhysicalType<Type::kInt32>>) {
          return Column(ArrayType<Type::kInt32>(rows, value));
        } else if constexpr (std::is_same_v<T, PhysicalType<Type::kDate>>) {
          return Column(ArrayType<Type::kDate>(rows, value));
        } else if constexpr (std::is_same_v<T, PhysicalType<Type::kTimestamp>>) {
          return Column(ArrayType<Type::kTimestamp>(rows, value));
        } else if constexpr (std::is_same_v<T, PhysicalType<Type::kInt64>>) {
          return Column(ArrayType<Type::kInt64>(rows, value));
        } else if constexpr (std::is_same_v<T, PhysicalType<Type::kString>>) {
          return Column(ArrayType<Type::kString>(rows, value));
        } else if constexpr (std::is_same_v<T, PhysicalType<Type::kChar>>) {
          return Column(ArrayType<Type::kChar>(rows, value));
        } else {
          static_assert(false, "Unknown type");
        }
      },
      expression->value.GetValue());
}

Column EvaluateVariable(std::shared_ptr<Batch> batch, std::shared_ptr<Variable> expression) {
  std::string name = expression->name;
  Type type = expression->type;

  Column result = batch->ColumnByName(name);
  ASSERT(result.GetType() == type);
  return result;
}

Column EvaluateBinary(std::shared_ptr<Batch> batch, std::shared_ptr<Binary> expression) {
  Column lhs = Evaluate(batch, expression->lhs);
  Column rhs = Evaluate(batch, expression->rhs);

  BinaryFunction function = expression->function;

  switch (function) {
    case BinaryFunction::kAdd:
      return Add(lhs, rhs);
    case BinaryFunction::kSub:
      return Sub(lhs, rhs);
    case BinaryFunction::kMult:
      return Mult(lhs, rhs);
    case BinaryFunction::kDiv:
      return Div(lhs, rhs);
    case BinaryFunction::kAnd:
      return And(lhs, rhs);
    case BinaryFunction::kOr:
      return Or(lhs, rhs);
    case BinaryFunction::kLess:
      return Less(lhs, rhs);
    case BinaryFunction::kGreater:
      return Greater(lhs, rhs);
    case BinaryFunction::kEqual:
      return Equal(lhs, rhs);
    case BinaryFunction::kNotEqual:
      return NotEqual(lhs, rhs);
    case BinaryFunction::kLessOrEqual:
      return LessOrEqual(lhs, rhs);
    case BinaryFunction::kGreaterOrEqual:
      return GreaterOrEqual(lhs, rhs);
    default:
      THROW_NOT_IMPLEMENTED;
  }
}

}  // namespace

Column Evaluate(std::shared_ptr<Batch> batch, std::shared_ptr<Expression> expression) {
  switch (expression->expr_type) {
    case ExpressionType::kConst:
      return EvaluateConst(batch->Rows(), std::static_pointer_cast<Const>(expression));
    case ExpressionType::kVariable:
      return EvaluateVariable(batch, std::static_pointer_cast<Variable>(expression));
    case ExpressionType::kBinary:
      return EvaluateBinary(batch, std::static_pointer_cast<Binary>(expression));
    default:
      THROW_NOT_IMPLEMENTED;
  }
}

}  // namespace ngn
