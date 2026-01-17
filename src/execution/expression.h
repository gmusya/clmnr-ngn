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
  kUnary,
  kBinary,
  kLike,
  kIn,
  kCase,
  kRegexReplace,
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

enum class UnaryFunction {
  kNot,
  kExtractMinute,
  kStrLen,
  kDateTruncMinute,
};

struct Unary : public Expression {
  explicit Unary(UnaryFunction f, std::shared_ptr<Expression> op)
      : Expression(ExpressionType::kUnary), function(f), operand(std::move(op)) {}

  UnaryFunction function;
  std::shared_ptr<Expression> operand;
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

struct Like : public Expression {
  explicit Like(std::shared_ptr<Expression> op, std::string pat, bool neg)
      : Expression(ExpressionType::kLike), operand(std::move(op)), pattern(std::move(pat)), negated(neg) {}

  std::shared_ptr<Expression> operand;
  std::string pattern;
  bool negated;
};

struct In : public Expression {
  explicit In(std::shared_ptr<Expression> op, std::vector<Value> vals)
      : Expression(ExpressionType::kIn), operand(std::move(op)), values(std::move(vals)) {}

  std::shared_ptr<Expression> operand;
  std::vector<Value> values;
};

struct Case : public Expression {
  explicit Case(std::shared_ptr<Expression> cond, std::shared_ptr<Expression> then_e,
                std::shared_ptr<Expression> else_e)
      : Expression(ExpressionType::kCase),
        condition(std::move(cond)),
        then_expr(std::move(then_e)),
        else_expr(std::move(else_e)) {}

  std::shared_ptr<Expression> condition;
  std::shared_ptr<Expression> then_expr;
  std::shared_ptr<Expression> else_expr;
};

struct RegexReplace : public Expression {
  explicit RegexReplace(std::shared_ptr<Expression> op, std::string pat, std::string repl)
      : Expression(ExpressionType::kRegexReplace),
        operand(std::move(op)),
        pattern(std::move(pat)),
        replacement(std::move(repl)) {}

  std::shared_ptr<Expression> operand;
  std::string pattern;
  std::string replacement;
};

inline std::shared_ptr<Const> MakeConst(Value value) { return std::make_shared<Const>(std::move(value)); }

inline std::shared_ptr<Variable> MakeVariable(std::string name, Type type) {
  return std::make_shared<Variable>(std::move(name), std::move(type));
}

inline std::shared_ptr<Unary> MakeUnary(UnaryFunction function, std::shared_ptr<Expression> operand) {
  return std::make_shared<Unary>(std::move(function), std::move(operand));
}

inline std::shared_ptr<Binary> MakeBinary(BinaryFunction function, std::shared_ptr<Expression> lhs,
                                          std::shared_ptr<Expression> rhs) {
  return std::make_shared<Binary>(std::move(function), std::move(lhs), std::move(rhs));
}

inline std::shared_ptr<Like> MakeLike(std::shared_ptr<Expression> operand, std::string pattern, bool negated = false) {
  return std::make_shared<Like>(std::move(operand), std::move(pattern), negated);
}

inline std::shared_ptr<In> MakeIn(std::shared_ptr<Expression> operand, std::vector<Value> values) {
  return std::make_shared<In>(std::move(operand), std::move(values));
}

inline std::shared_ptr<Case> MakeCase(std::shared_ptr<Expression> condition, std::shared_ptr<Expression> then_expr,
                                      std::shared_ptr<Expression> else_expr) {
  return std::make_shared<Case>(std::move(condition), std::move(then_expr), std::move(else_expr));
}

inline std::shared_ptr<RegexReplace> MakeRegexReplace(std::shared_ptr<Expression> operand, std::string pattern,
                                                      std::string replacement) {
  return std::make_shared<RegexReplace>(std::move(operand), std::move(pattern), std::move(replacement));
}

Column Evaluate(std::shared_ptr<Batch> batch, std::shared_ptr<Expression> expression);

}  // namespace ngn
