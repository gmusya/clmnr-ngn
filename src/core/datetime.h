#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

#include "src/core/type.h"

namespace ngn {

namespace datetime {

inline bool IsLeapYear(int year) { return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0); }

inline int DaysInMonth(int year, int month) {
  static constexpr int kDaysInMonth[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  if (month == 2 && IsLeapYear(year)) {
    return 29;
  }
  return kDaysInMonth[month];
}

// Days from start of year 1 to start of given year (using proleptic Gregorian calendar)
inline int64_t DaysFromYear1(int year) {
  int64_t y = year - 1;
  return 365 * y + y / 4 - y / 100 + y / 400;
}

// Days since 1970-01-01 for a given date
inline int64_t DateToDays(int year, int month, int day) {
  // DaysFromYear1(1970) = 719162
  static constexpr int64_t kEpochDays = 719162;

  int64_t days = DaysFromYear1(year);
  for (int m = 1; m < month; ++m) {
    days += DaysInMonth(year, m);
  }
  days += day - 1;  // day is 1-indexed, so day 1 = 0 extra days
  return days - kEpochDays;
}

namespace internal {

inline int ParseInt(std::string_view s, size_t& pos, size_t len) {
  int result = 0;
  for (size_t i = 0; i < len && pos < s.size(); ++i, ++pos) {
    char c = s[pos];
    if (c < '0' || c > '9') {
      throw std::runtime_error("Invalid character in date/timestamp: " + std::string(s));
    }
    result = result * 10 + (c - '0');
  }
  return result;
}

inline void ExpectChar(std::string_view s, size_t& pos, char expected) {
  if (pos >= s.size() || s[pos] != expected) {
    throw std::runtime_error("Expected '" + std::string(1, expected) + "' in date/timestamp: " + std::string(s));
  }
  ++pos;
}

}  // namespace internal

}  // namespace datetime

// Parse date in format "YYYY-MM-DD"
// Returns Date with days since 1970-01-01
inline Date ParseDate(std::string_view s) {
  size_t pos = 0;
  int year = datetime::internal::ParseInt(s, pos, 4);
  datetime::internal::ExpectChar(s, pos, '-');
  int month = datetime::internal::ParseInt(s, pos, 2);
  datetime::internal::ExpectChar(s, pos, '-');
  int day = datetime::internal::ParseInt(s, pos, 2);

  if (month < 1 || month > 12) {
    throw std::runtime_error("Invalid month in date: " + std::string(s));
  }
  if (day < 1 || day > datetime::DaysInMonth(year, month)) {
    throw std::runtime_error("Invalid day in date: " + std::string(s));
  }

  return Date{datetime::DateToDays(year, month, day)};
}

// Parse timestamp in format "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD HH:MM:SS.ffffff"
// Returns Timestamp with microseconds since 1970-01-01 00:00:00 UTC
inline Timestamp ParseTimestamp(std::string_view s) {
  size_t pos = 0;

  // Parse date part
  int year = datetime::internal::ParseInt(s, pos, 4);
  datetime::internal::ExpectChar(s, pos, '-');
  int month = datetime::internal::ParseInt(s, pos, 2);
  datetime::internal::ExpectChar(s, pos, '-');
  int day = datetime::internal::ParseInt(s, pos, 2);

  // Expect space or 'T' separator
  if (pos < s.size() && (s[pos] == ' ' || s[pos] == 'T')) {
    ++pos;
  } else {
    throw std::runtime_error("Expected space or 'T' after date in timestamp: " + std::string(s));
  }

  // Parse time part
  int hour = datetime::internal::ParseInt(s, pos, 2);
  datetime::internal::ExpectChar(s, pos, ':');
  int minute = datetime::internal::ParseInt(s, pos, 2);
  datetime::internal::ExpectChar(s, pos, ':');
  int second = datetime::internal::ParseInt(s, pos, 2);

  // Parse optional microseconds
  int64_t microseconds = 0;
  if (pos < s.size() && s[pos] == '.') {
    ++pos;
    // Parse up to 6 digits of fractional seconds
    int64_t frac = 0;
    int digits = 0;
    while (pos < s.size() && s[pos] >= '0' && s[pos] <= '9' && digits < 6) {
      frac = frac * 10 + (s[pos] - '0');
      ++pos;
      ++digits;
    }
    // Pad to 6 digits (microseconds)
    while (digits < 6) {
      frac *= 10;
      ++digits;
    }
    // Skip remaining digits if any
    while (pos < s.size() && s[pos] >= '0' && s[pos] <= '9') {
      ++pos;
    }
    microseconds = frac;
  }

  // Validate
  if (month < 1 || month > 12) {
    throw std::runtime_error("Invalid month in timestamp: " + std::string(s));
  }
  if (day < 1 || day > datetime::DaysInMonth(year, month)) {
    throw std::runtime_error("Invalid day in timestamp: " + std::string(s));
  }
  if (hour < 0 || hour > 23) {
    throw std::runtime_error("Invalid hour in timestamp: " + std::string(s));
  }
  if (minute < 0 || minute > 59) {
    throw std::runtime_error("Invalid minute in timestamp: " + std::string(s));
  }
  if (second < 0 || second > 59) {
    throw std::runtime_error("Invalid second in timestamp: " + std::string(s));
  }

  int64_t days = datetime::DateToDays(year, month, day);
  int64_t time_us = static_cast<int64_t>(hour) * 3600000000LL + static_cast<int64_t>(minute) * 60000000LL +
                    static_cast<int64_t>(second) * 1000000LL + microseconds;

  return Timestamp{days * 86400000000LL + time_us};
}

}  // namespace ngn
