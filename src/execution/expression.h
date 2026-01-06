#pragma once

#include <memory>

#include "src/core/column.h"
#include "src/core/type.h"
#include "src/core/value.h"
#include "src/execution/batch.h"

namespace ngn {

enum class ExpressionType {
  kConst,
  kVariable,
  kBinary,
};

struct Expression {
  explicit Expression(ExpressionType et) : expr_type(et) {}

  const ExpressionType expr_type;

 protected:
  ~Expression() = default;
};

struct Const : public Expression {
  explicit Const(Value d) : Expression(ExpressionType::kConst), value(std::move(d)) {}

  Value value;
};

struct Variable : public Expression {
  explicit Variable(std::string n, Type t) : Expression(ExpressionType::kVariable), name(std::move(n)), type(t) {}

  std::string name;
  Type type;
};

enum class BinaryFunction {
  kAdd,
  kSub,
  kMult,
  kDiv,
  kAnd,
  kOr,
  kLess,
  kGreater,
  kEqual,
  kNotEqual,
  kLessOrEqual,
  kGreaterOrEqual,
};

struct Binary : public Expression {
  explicit Binary(BinaryFunction f, std::shared_ptr<Expression> l, std::shared_ptr<Expression> r)
      : Expression(ExpressionType::kBinary), function(f), lhs(std::move(l)), rhs(std::move(r)) {}

  BinaryFunction function;
  std::shared_ptr<Expression> lhs;
  std::shared_ptr<Expression> rhs;
};

Column Evaluate(std::shared_ptr<Batch> batch, std::shared_ptr<Expression> expression);

}  // namespace ngn
