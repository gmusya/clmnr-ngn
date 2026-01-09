#pragma once

#include <stdexcept>
#include <string>

#define ASSERT(cond)                                                                                              \
  do {                                                                                                            \
    if (!(cond)) {                                                                                                \
      throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + ": condition '" + #cond + \
                               "' is not satistfied");                                                            \
    }                                                                                                             \
  } while (false)

#define ASSERT_WITH_MESSAGE(cond, message)                                                                        \
  do {                                                                                                            \
    if (!(cond)) {                                                                                                \
      throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + ": condition '" + #cond + \
                               "' is not satistfied: " + (message));                                              \
    }                                                                                                             \
  } while (false)
