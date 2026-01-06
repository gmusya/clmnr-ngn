#pragma once

#include "src/core/column.h"

namespace ngn {

Column Add(const Column&, const Column&);
Column Sub(const Column&, const Column&);
Column Mult(const Column&, const Column&);
Column Div(const Column&, const Column&);

Column And(Column lhs, Column rhs);
Column Or(Column lhs, Column rhs);
Column Less(Column lhs, Column rhs);
Column Greater(Column lhs, Column rhs);
Column Equal(Column lhs, Column rhs);
Column NotEqual(Column lhs, Column rhs);
Column LessOrEqual(Column lhs, Column rhs);
Column GreaterOrEqual(Column lhs, Column rhs);

}  // namespace ngn
