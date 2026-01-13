#pragma once

#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "src/core/type.h"
#include "src/util/assert.h"
#include "src/util/macro.h"

namespace ngn {

struct Field {
  std::string name;
  Type type;

  bool operator==(const Field& other) const = default;
};

class Schema {
 public:
  explicit Schema(std::vector<Field> fields) : fields_(std::move(fields)) {}

  bool operator==(const Schema& other) const = default;
  bool operator!=(const Schema& other) const = default;

  const std::vector<Field>& Fields() const { return fields_; }

  std::string Serialize() const {
    std::string serialized;
    for (const auto& field : fields_) {
      serialized += SerializeField(field) + "\n";
    }
    return serialized;
  }

  static Schema Deserialize(const std::string& serialized) {
    std::vector<Field> fields;
    std::stringstream ss(serialized);
    std::string field;
    while (std::getline(ss, field, '\n')) {
      fields.emplace_back(DeserializeField(field));
    }
    return Schema(fields);
  }

  void ToFile(const std::string& path) {
    std::ofstream file(path);
    ASSERT_WITH_MESSAGE(file.good(), "Failed to open schema file: " + path);
    file << Serialize();
  }

  static Schema FromFile(const std::string& path) {
    std::ifstream file(path);
    ASSERT_WITH_MESSAGE(file.good(), "Failed to open schema file: " + path);
    std::stringstream ss;
    ss << file.rdbuf();
    return Deserialize(ss.str());
  }

 private:
  static constexpr char kDelimiter = ',';

  static std::string SerializeField(const Field& field) {
    std::string type_as_string = [](Type type) {
      switch (type) {
        case Type::kBool:
          return "bool";
        case Type::kInt16:
          return "int16";
        case Type::kInt32:
          return "int32";
        case Type::kInt64:
          return "int64";
        case Type::kString:
          return "string";
        case Type::kDate:
          return "date";
        case Type::kTimestamp:
          return "timestamp";
        case Type::kChar:
          return "char";
        default:
          THROW_RUNTIME_ERROR("Unknown type: " + std::to_string(static_cast<int>(type)));
      }
    }(field.type);
    return field.name + kDelimiter + type_as_string;
  }

  static Field DeserializeField(const std::string& serialized) {
    std::stringstream ss(serialized);
    std::string name;
    std::string type;
    std::getline(ss, name, kDelimiter);
    std::getline(ss, type, kDelimiter);
    if (type == "bool") {
      return Field{name, Type::kBool};
    } else if (type == "int16") {
      return Field{name, Type::kInt16};
    } else if (type == "int32") {
      return Field{name, Type::kInt32};
    } else if (type == "int64") {
      return Field{name, Type::kInt64};
    } else if (type == "string") {
      return Field{name, Type::kString};
    } else if (type == "date") {
      return Field{name, Type::kDate};
    } else if (type == "timestamp") {
      return Field{name, Type::kTimestamp};
    } else if (type == "char") {
      return Field{name, Type::kChar};
    } else {
      THROW_RUNTIME_ERROR("Unknown type: " + type);
    }
  }

  std::vector<Field> fields_;
};

}  // namespace ngn
