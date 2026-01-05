#pragma once

#include <stdexcept>

#define ASSERT(cond)                                                                                              \
  do {                                                                                                            \
    if (!(cond)) {                                                                                                \
      throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + ": condition '" + #cond + \
                               "' is not satistfied");                                                            \
    }                                                                                                             \
  } while (false)
