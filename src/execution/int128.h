#pragma once

#include <algorithm>
#include <string>

namespace ngn {

using Int128 = __int128_t;

inline std::string Int128ToString(Int128 value) {
  if (value == 0) {
    return "0";
  }

  bool negative = value < 0;
  unsigned __int128 tmp = 0;
  if (negative) {
    tmp = static_cast<unsigned __int128>(-(value + 1));
    tmp += 1;
  } else {
    tmp = static_cast<unsigned __int128>(value);
  }

  std::string out;
  while (tmp > 0) {
    const unsigned digit = static_cast<unsigned>(tmp % 10);
    out.push_back(static_cast<char>('0' + digit));
    tmp /= 10;
  }

  if (negative) {
    out.push_back('-');
  }

  std::reverse(out.begin(), out.end());
  return out;
}

inline Int128 ParseInt128(const std::string& value) {
  size_t idx = 0;
  bool negative = false;
  if (!value.empty() && (value[0] == '-' || value[0] == '+')) {
    negative = (value[0] == '-');
    idx = 1;
  }

  unsigned __int128 result = 0;
  for (; idx < value.size(); ++idx) {
    const char ch = value[idx];
    if (ch < '0' || ch > '9') {
      break;
    }
    result = result * 10 + static_cast<unsigned>(ch - '0');
  }

  if (negative) {
    return -static_cast<Int128>(result);
  }
  return static_cast<Int128>(result);
}

}  // namespace ngn
