#pragma once

#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "src/core/type.h"

namespace ngn {

struct Field {
  std::string name;
  Type type;

  bool operator==(const Field& other) const = default;
};

class Schema {
 public:
  explicit Schema(std::vector<Field> fields) : fields_(std::move(fields)) {}

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

  static Schema FromFile(const std::string& path) {
    std::ifstream file(path);
    std::stringstream ss;
    ss << file.rdbuf();
    return Deserialize(ss.str());
  }

 private:
  static constexpr char kDelimiter = ',';

  static std::string SerializeField(const Field& field) {
    return field.name + kDelimiter + (field.type == Type::kInt64 ? "int64" : "string");
  }

  static Field DeserializeField(const std::string& serialized) {
    std::stringstream ss(serialized);
    std::string name;
    std::string type;
    std::getline(ss, name, kDelimiter);
    std::getline(ss, type, kDelimiter);
    return Field{name, type == "int64" ? Type::kInt64 : Type::kString};
  }

  std::vector<Field> fields_;
};

}  // namespace ngn
