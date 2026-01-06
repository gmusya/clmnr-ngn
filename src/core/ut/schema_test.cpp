#include "src/core/schema.h"

#include "gtest/gtest.h"

namespace ngn {

TEST(Schema, Serialize) {
  Schema schema({Field{"a", Type::kInt64}, Field{"b", Type::kString}});
  EXPECT_EQ(schema.Serialize(), "a,int64\nb,string\n");
}

TEST(Schema, Deserialize) {
  Schema schema = Schema::Deserialize("a,int64\nb,string\n");
  EXPECT_EQ(schema.Fields(), (std::vector<Field>{{"a", Type::kInt64}, {"b", Type::kString}}));
}

}  // namespace ngn
