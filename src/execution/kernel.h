#pragma once

#include "src/core/column.h"

namespace ngn {

Column Add(const Column&, const Column&);
Column Sub(const Column&, const Column&);
Column Mult(const Column&, const Column&);
Column Div(const Column&, const Column&);

Column And(const Column& lhs, const Column& rhs);
Column Or(const Column& lhs, const Column& rhs);
Column Less(const Column& lhs, const Column& rhs);
Column Greater(const Column& lhs, const Column& rhs);
Column Equal(const Column& lhs, const Column& rhs);
Column NotEqual(const Column& lhs, const Column& rhs);
Column LessOrEqual(const Column& lhs, const Column& rhs);
Column GreaterOrEqual(const Column& lhs, const Column& rhs);

}  // namespace ngn
