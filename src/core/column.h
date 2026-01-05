#pragma once

#include <cstdint>
#include <vector>

namespace ngn {

class Column {
 public:
  explicit Column(std::vector<int64_t> values) : values_(std::move(values)) {}

  std::vector<int64_t>& Values() { return values_; }
  const std::vector<int64_t>& Values() const { return values_; }

 private:
  std::vector<int64_t> values_;
};

}  // namespace ngn
