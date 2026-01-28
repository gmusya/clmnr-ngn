#pragma once

#include <string>

#include "src/core/column.h"
#include "src/core/type.h"
#include "src/core/value.h"

namespace ngn {

Column Add(const Column&, const Column&);
Column Sub(const Column&, const Column&);
Column Mult(const Column&, const Column&);
Column Div(const Column&, const Column&);

// Reduction kernels (return a single scalar value)
Value ReduceSum(const Column& operand, Type output_type);
Value ReduceMin(const Column& operand);
Value ReduceMax(const Column& operand);

Column And(const Column& lhs, const Column& rhs);
Column Or(const Column& lhs, const Column& rhs);
Column Less(const Column& lhs, const Column& rhs);
Column Greater(const Column& lhs, const Column& rhs);
Column Equal(const Column& lhs, const Column& rhs);
Column NotEqual(const Column& lhs, const Column& rhs);
Column LessOrEqual(const Column& lhs, const Column& rhs);
Column GreaterOrEqual(const Column& lhs, const Column& rhs);
Column StrContains(const Column& operand, const std::string& substring, bool negated);

// Unary operations
Column Not(const Column& operand);
Column ExtractMinute(const Column& operand);
Column StrLen(const Column& operand);
Column DateTruncMinute(const Column& operand);

// String operations
Column StrRegexReplace(const Column& operand, const std::string& pattern, const std::string& replacement);

}  // namespace ngn
