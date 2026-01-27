#pragma once

#include <iostream>
#include <string>

#include "src/core/type.h"

namespace ngn {

template <typename T>
void Write(const T& value, std::ostream& out);

template <typename T>
T Read(std::istream& in);

////////////////////////////////////////////////////////////////////////////////

template <>
inline void Write(const Boolean& value, std::ostream& out) {
  out.write(reinterpret_cast<const char*>(&value.value), sizeof(value.value));
}

template <>
inline Boolean Read(std::istream& in) {
  Boolean value;
  in.read(reinterpret_cast<char*>(&value.value), sizeof(value.value));
  return value;
}

////////////////////////////////////////////////////////////////////////////////

template <>
inline void Write(const int16_t& value, std::ostream& out) {
  out.write(reinterpret_cast<const char*>(&value), sizeof(value));
}

template <>
inline int16_t Read(std::istream& in) {
  int16_t value;
  in.read(reinterpret_cast<char*>(&value), sizeof(value));
  return value;
}

////////////////////////////////////////////////////////////////////////////////

template <>
inline void Write(const int32_t& value, std::ostream& out) {
  out.write(reinterpret_cast<const char*>(&value), sizeof(value));
}

template <>
inline int32_t Read(std::istream& in) {
  int32_t value;
  in.read(reinterpret_cast<char*>(&value), sizeof(value));
  return value;
}

////////////////////////////////////////////////////////////////////////////////

template <>
inline void Write(const int64_t& value, std::ostream& out) {
  out.write(reinterpret_cast<const char*>(&value), sizeof(value));
}

template <>
inline int64_t Read(std::istream& in) {
  int64_t value;
  in.read(reinterpret_cast<char*>(&value), sizeof(value));
  return value;
}

////////////////////////////////////////////////////////////////////////////////

template <>
inline void Write(const Int128& value, std::ostream& out) {
  out.write(reinterpret_cast<const char*>(&value), sizeof(value));
}

template <>
inline Int128 Read(std::istream& in) {
  Int128 value;
  in.read(reinterpret_cast<char*>(&value), sizeof(value));
  return value;
}

////////////////////////////////////////////////////////////////////////////////

template <>
inline void Write(const std::string& value, std::ostream& out) {
  int64_t sz = value.size();
  Write(sz, out);
  out.write(value.data(), value.size());
}

template <>
inline std::string Read(std::istream& in) {
  int64_t sz = Read<int64_t>(in);
  std::string value(sz, '0');
  in.read(value.data(), value.size());

  return value;
}

////////////////////////////////////////////////////////////////////////////////

template <>
inline void Write(const Date& value, std::ostream& out) {
  Write(value.value, out);
}

template <>
inline Date Read(std::istream& in) {
  return Date{Read<int64_t>(in)};
}

////////////////////////////////////////////////////////////////////////////////

template <>
inline void Write(const Timestamp& value, std::ostream& out) {
  Write(value.value, out);
}

template <>
inline Timestamp Read(std::istream& in) {
  return Timestamp{Read<int64_t>(in)};
}

////////////////////////////////////////////////////////////////////////////////

template <>
inline void Write(const char& value, std::ostream& out) {
  out.write(reinterpret_cast<const char*>(&value), sizeof(value));
}

template <>
inline char Read(std::istream& in) {
  char value;
  in.read(reinterpret_cast<char*>(&value), sizeof(value));
  return value;
}

}  // namespace ngn
