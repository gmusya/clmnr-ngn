#include "src/core/datetime.h"

#include "gtest/gtest.h"

namespace ngn {

TEST(DateTime, IsLeapYear) {
  // Regular years divisible by 4 are leap years
  EXPECT_TRUE(datetime::IsLeapYear(2004));
  EXPECT_TRUE(datetime::IsLeapYear(2008));
  EXPECT_TRUE(datetime::IsLeapYear(2012));

  // Years divisible by 100 but not 400 are not leap years
  EXPECT_FALSE(datetime::IsLeapYear(1900));
  EXPECT_FALSE(datetime::IsLeapYear(2100));

  // Years divisible by 400 are leap years
  EXPECT_TRUE(datetime::IsLeapYear(2000));
  EXPECT_TRUE(datetime::IsLeapYear(1600));

  // Regular non-leap years
  EXPECT_FALSE(datetime::IsLeapYear(2001));
  EXPECT_FALSE(datetime::IsLeapYear(2023));
}

TEST(DateTime, DaysInMonth) {
  // Regular months
  EXPECT_EQ(datetime::DaysInMonth(2023, 1), 31);
  EXPECT_EQ(datetime::DaysInMonth(2023, 3), 31);
  EXPECT_EQ(datetime::DaysInMonth(2023, 4), 30);
  EXPECT_EQ(datetime::DaysInMonth(2023, 5), 31);
  EXPECT_EQ(datetime::DaysInMonth(2023, 6), 30);
  EXPECT_EQ(datetime::DaysInMonth(2023, 7), 31);
  EXPECT_EQ(datetime::DaysInMonth(2023, 8), 31);
  EXPECT_EQ(datetime::DaysInMonth(2023, 9), 30);
  EXPECT_EQ(datetime::DaysInMonth(2023, 10), 31);
  EXPECT_EQ(datetime::DaysInMonth(2023, 11), 30);
  EXPECT_EQ(datetime::DaysInMonth(2023, 12), 31);

  // February in non-leap year
  EXPECT_EQ(datetime::DaysInMonth(2023, 2), 28);
  EXPECT_EQ(datetime::DaysInMonth(1900, 2), 28);

  // February in leap year
  EXPECT_EQ(datetime::DaysInMonth(2024, 2), 29);
  EXPECT_EQ(datetime::DaysInMonth(2000, 2), 29);
}

TEST(DateTime, DateToDaysEpoch) {
  // Unix epoch
  EXPECT_EQ(datetime::DateToDays(1970, 1, 1), 0);
}

TEST(DateTime, DateToDaysBeforeEpoch) {
  // Day before epoch
  EXPECT_EQ(datetime::DateToDays(1969, 12, 31), -1);
  EXPECT_EQ(datetime::DateToDays(1969, 12, 30), -2);

  // Year before epoch
  EXPECT_EQ(datetime::DateToDays(1969, 1, 1), -365);
}

TEST(DateTime, DateToDaysAfterEpoch) {
  // Days after epoch
  EXPECT_EQ(datetime::DateToDays(1970, 1, 2), 1);
  EXPECT_EQ(datetime::DateToDays(1970, 2, 1), 31);

  EXPECT_EQ(datetime::DateToDays(2000, 1, 1), 10957);

  EXPECT_EQ(datetime::DateToDays(2013, 7, 1), 15887);
  EXPECT_EQ(datetime::DateToDays(2013, 7, 15), 15901);
}

TEST(DateTime, ParseDateBasic) {
  Date d = ParseDate("1970-01-01");
  EXPECT_EQ(d.value, 0);

  d = ParseDate("2000-01-01");
  EXPECT_EQ(d.value, 10957);

  d = ParseDate("2013-07-15");
  EXPECT_EQ(d.value, 15901);
}

TEST(DateTime, ParseDateBeforeEpoch) {
  Date d = ParseDate("1969-12-31");
  EXPECT_EQ(d.value, -1);
}

TEST(DateTime, ParseDateLeapYear) {
  // Feb 29 in leap year should work
  Date d = ParseDate("2000-02-29");
  EXPECT_EQ(d.value, datetime::DateToDays(2000, 2, 29));

  d = ParseDate("2024-02-29");
  EXPECT_EQ(d.value, datetime::DateToDays(2024, 2, 29));
}

TEST(DateTime, ParseDateInvalid) {
  // Invalid month
  EXPECT_THROW(ParseDate("2023-13-01"), std::runtime_error);
  EXPECT_THROW(ParseDate("2023-00-01"), std::runtime_error);

  // Invalid day
  EXPECT_THROW(ParseDate("2023-01-32"), std::runtime_error);
  EXPECT_THROW(ParseDate("2023-01-00"), std::runtime_error);

  // Feb 29 in non-leap year
  EXPECT_THROW(ParseDate("2023-02-29"), std::runtime_error);
  EXPECT_THROW(ParseDate("1900-02-29"), std::runtime_error);

  // Invalid format
  EXPECT_THROW(ParseDate("2023/01/01"), std::runtime_error);
  EXPECT_THROW(ParseDate("01-01-2023"), std::runtime_error);
  EXPECT_THROW(ParseDate("2023-1-1"), std::runtime_error);
}

TEST(DateTime, ParseTimestampBasic) {
  Timestamp ts = ParseTimestamp("1970-01-01 00:00:00");
  EXPECT_EQ(ts.value, 0);

  ts = ParseTimestamp("1970-01-01T00:00:00");
  EXPECT_EQ(ts.value, 0);
}

TEST(DateTime, ParseTimestampWithTime) {
  // 1 hour = 3600 seconds = 3600000000 microseconds
  Timestamp ts = ParseTimestamp("1970-01-01 01:00:00");
  EXPECT_EQ(ts.value, 3600000000LL);

  // 1 minute = 60 seconds = 60000000 microseconds
  ts = ParseTimestamp("1970-01-01 00:01:00");
  EXPECT_EQ(ts.value, 60000000LL);

  // 1 second = 1000000 microseconds
  ts = ParseTimestamp("1970-01-01 00:00:01");
  EXPECT_EQ(ts.value, 1000000LL);

  // Combined
  ts = ParseTimestamp("1970-01-01 12:30:45");
  int64_t expected = 12LL * 3600000000LL + 30LL * 60000000LL + 45LL * 1000000LL;
  EXPECT_EQ(ts.value, expected);
}

TEST(DateTime, ParseTimestampWithMicroseconds) {
  Timestamp ts = ParseTimestamp("1970-01-01 00:00:00.000001");
  EXPECT_EQ(ts.value, 1);

  ts = ParseTimestamp("1970-01-01 00:00:00.123456");
  EXPECT_EQ(ts.value, 123456);

  ts = ParseTimestamp("1970-01-01 00:00:01.500000");
  EXPECT_EQ(ts.value, 1500000);
}

TEST(DateTime, ParseTimestampMicrosecondsPadding) {
  // Less than 6 digits should be padded
  Timestamp ts = ParseTimestamp("1970-01-01 00:00:00.1");
  EXPECT_EQ(ts.value, 100000);

  ts = ParseTimestamp("1970-01-01 00:00:00.12");
  EXPECT_EQ(ts.value, 120000);

  ts = ParseTimestamp("1970-01-01 00:00:00.123");
  EXPECT_EQ(ts.value, 123000);
}

TEST(DateTime, ParseTimestampMicrosecondsExtraDigits) {
  // More than 6 digits should be truncated (only first 6 used)
  Timestamp ts = ParseTimestamp("1970-01-01 00:00:00.1234567");
  EXPECT_EQ(ts.value, 123456);

  ts = ParseTimestamp("1970-01-01 00:00:00.123456789");
  EXPECT_EQ(ts.value, 123456);
}

TEST(DateTime, ParseTimestampWithDate) {
  // 1 day = 86400 seconds = 86400000000 microseconds
  Timestamp ts = ParseTimestamp("1970-01-02 00:00:00");
  EXPECT_EQ(ts.value, 86400000000LL);

  ts = ParseTimestamp("2000-01-01 00:00:00");
  EXPECT_EQ(ts.value, 10957LL * 86400000000LL);
}

TEST(DateTime, ParseTimestampTSeparator) {
  Timestamp ts1 = ParseTimestamp("2023-07-15 10:30:45.123456");
  Timestamp ts2 = ParseTimestamp("2023-07-15T10:30:45.123456");
  EXPECT_EQ(ts1.value, ts2.value);
}

TEST(DateTime, ParseTimestampInvalid) {
  // Missing time part
  EXPECT_THROW(ParseTimestamp("2023-01-01"), std::runtime_error);

  // Invalid hour
  EXPECT_THROW(ParseTimestamp("2023-01-01 24:00:00"), std::runtime_error);

  // Invalid minute
  EXPECT_THROW(ParseTimestamp("2023-01-01 00:60:00"), std::runtime_error);

  // Invalid second
  EXPECT_THROW(ParseTimestamp("2023-01-01 00:00:60"), std::runtime_error);

  // Invalid separator
  EXPECT_THROW(ParseTimestamp("2023-01-01X00:00:00"), std::runtime_error);
}

TEST(DateTime, ParseTimestampBeforeEpoch) {
  Timestamp ts = ParseTimestamp("1969-12-31 23:59:59");
  EXPECT_EQ(ts.value, -1000000LL);

  ts = ParseTimestamp("1969-12-31 00:00:00");
  EXPECT_EQ(ts.value, -86400000000LL);
}

}  // namespace ngn
