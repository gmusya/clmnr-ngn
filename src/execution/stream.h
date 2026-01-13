#pragma once

#include <optional>

namespace ngn {

template <typename Value>
class IStream {
 public:
  virtual std::optional<Value> Next() = 0;

  virtual ~IStream() = default;
};

}  // namespace ngn
