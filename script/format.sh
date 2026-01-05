#!/bin/bash

# This script must be called from the root folder of the repository
find src/ exe/ -type f \( -name '*.c' -o -name '*.h' -o -name '*.cpp' -o -name '*.cc' \) -print0 | xargs -0 clang-format --dry-run -Werror
