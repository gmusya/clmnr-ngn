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
        } else if constexpr (std::is_same_v<T, PhysicalType<Type::kInt128>>) {
          return Column(ArrayType<Type::kInt128>(rows, value));
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

Column EvaluateUnary(std::shared_ptr<Batch> batch, std::shared_ptr<Unary> expression) {
  Column operand = Evaluate(batch, expression->operand);

  switch (expression->function) {
    case UnaryFunction::kNot:
      return Not(operand);
    case UnaryFunction::kExtractMinute:
      return ExtractMinute(operand);
    case UnaryFunction::kStrLen:
      return StrLen(operand);
    case UnaryFunction::kDateTruncMinute:
      return DateTruncMinute(operand);
    default:
      THROW_NOT_IMPLEMENTED;
  }
}

Column EvaluateContains(std::shared_ptr<Batch> batch, std::shared_ptr<Contains> expression) {
  Column operand = Evaluate(batch, expression->operand);
  return StrContains(operand, expression->substring, expression->negated);
}

Column EvaluateIn(std::shared_ptr<Batch>, std::shared_ptr<In>) { THROW_NOT_IMPLEMENTED; }

Column EvaluateCase(std::shared_ptr<Batch> batch, std::shared_ptr<Case> expression) {
  Column cond_col = Evaluate(batch, expression->condition);
  ASSERT(cond_col.GetType() == Type::kBool);

  Column then_col = Evaluate(batch, expression->then_expr);
  Column else_col = Evaluate(batch, expression->else_expr);
  ASSERT(then_col.GetType() == else_col.GetType());

  const auto& cond_values = std::get<ArrayType<Type::kBool>>(cond_col.Values());

  return Dispatch(
      [&]<Type type>(Tag<type>) -> Column {
        const auto& then_values = std::get<ArrayType<type>>(then_col.Values());
        const auto& else_values = std::get<ArrayType<type>>(else_col.Values());

        ArrayType<type> result(cond_values.size());
        for (size_t i = 0; i < cond_values.size(); ++i) {
          result[i] = cond_values[i].value ? then_values[i] : else_values[i];
        }
        return Column(std::move(result));
      },
      then_col.GetType());
}

Column EvaluateRegexReplace(std::shared_ptr<Batch>, std::shared_ptr<RegexReplace>) { THROW_NOT_IMPLEMENTED; }

}  // namespace

Column Evaluate(std::shared_ptr<Batch> batch, std::shared_ptr<Expression> expression) {
  switch (expression->expr_type) {
    case ExpressionType::kConst:
      return EvaluateConst(batch->Rows(), std::static_pointer_cast<Const>(expression));
    case ExpressionType::kVariable:
      return EvaluateVariable(batch, std::static_pointer_cast<Variable>(expression));
    case ExpressionType::kUnary:
      return EvaluateUnary(batch, std::static_pointer_cast<Unary>(expression));
    case ExpressionType::kBinary:
      return EvaluateBinary(batch, std::static_pointer_cast<Binary>(expression));
    case ExpressionType::kContains:
      return EvaluateContains(batch, std::static_pointer_cast<Contains>(expression));
    case ExpressionType::kIn:
      return EvaluateIn(batch, std::static_pointer_cast<In>(expression));
    case ExpressionType::kCase:
      return EvaluateCase(batch, std::static_pointer_cast<Case>(expression));
    case ExpressionType::kRegexReplace:
      return EvaluateRegexReplace(batch, std::static_pointer_cast<RegexReplace>(expression));
    default:
      THROW_NOT_IMPLEMENTED;
  }
}

}  // namespace ngn
