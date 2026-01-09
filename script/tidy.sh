#!/bin/bash

# This script must be called from the root folder of the repository
find src/ exe/ clickbench/ -type f \( -name '*.c' -o -name '*.h' -o -name '*.cpp' -o -name '*.cc' \) -print0 | xargs -0 clang-tidy -p build
