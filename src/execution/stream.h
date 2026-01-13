#pragma once

#include <optional>
#include <vector>
namespace ngn {

template <typename Value>
class IStream {
 public:
  virtual std::optional<Value> Next() = 0;

  virtual ~IStream() = default;
};

template <typename Value>
class VectorStream : public IStream<Value> {
 public:
  VectorStream(std::vector<Value> values) : values_(std::move(values)) { ASSERT(!values_.empty()); }

  std::optional<Value> Next() override {
    if (index_ >= values_.size()) {
      return std::nullopt;
    }
    Value value = std::move(values_[index_]);
    ++index_;
    return value;
  }

 private:
  size_t index_ = 0;
  std::vector<Value> values_;
};

}  // namespace ngn
