#pragma once

#include <iostream>
#include <string>

namespace ngn {

template <typename T>
void Write(const T& value, std::ostream& out);

template <typename T>
T Read(std::istream& in);

////////////////////////////////////////////////////////////////////////////////

template <>
void Write(const int64_t& value, std::ostream& out) {
  out.write(reinterpret_cast<const char*>(&value), sizeof(value));
}

template <>
int64_t Read(std::istream& in) {
  int64_t value;
  in.read(reinterpret_cast<char*>(&value), sizeof(value));
  return value;
}

////////////////////////////////////////////////////////////////////////////////

template <>
void Write(const std::string& value, std::ostream& out) {
  int64_t sz = value.size();
  Write(sz, out);
  out.write(value.data(), value.size());
}

template <>
std::string Read(std::istream& in) {
  int64_t sz = Read<int64_t>(in);
  std::string value(sz, '0');
  in.read(value.data(), value.size());

  return value;
}

}  // namespace ngn
