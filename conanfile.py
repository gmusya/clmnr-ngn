from conan import ConanFile
from conan.errors import ConanInvalidConfiguration
from conan.tools.cmake import CMakeDeps, CMakeToolchain, cmake_layout


class ColumnarEngineConan(ConanFile):
    """
    This recipe is primarily used as a *consumer* (to generate toolchain/deps files)
    so the project can be built with CMake via Conan.
    """

    settings = "os", "compiler", "build_type", "arch"

    options = {
        "with_coverage": [True, False],
        "with_profiling": [True, False],
    }
    default_options = {
        "with_coverage": False,
        "with_profiling": False,
    }

    def requirements(self):
        self.requires("abseil/20250814.1")
        self.requires("benchmark/1.9.4")
        self.requires("gtest/1.17.0")
        self.requires("simde/0.8.2")

    def validate(self):
        if self.options.with_coverage and str(self.settings.compiler) != "clang":
            raise ConanInvalidConfiguration("with_coverage=True is only supported with clang.")

    def layout(self):
        cmake_layout(self)

    def generate(self):
        CMakeDeps(self).generate()

        tc = CMakeToolchain(self)
        tc.variables["CMAKE_EXPORT_COMPILE_COMMANDS"] = True

        if self.options.with_coverage:
            compiler = str(self.settings.compiler)
            if compiler == "clang":
                flags = "-O0 -g -fprofile-instr-generate -fcoverage-mapping"
                tc.variables["CMAKE_C_FLAGS"] = flags
                tc.variables["CMAKE_CXX_FLAGS"] = flags
                tc.variables["CMAKE_EXE_LINKER_FLAGS"] = "-fprofile-instr-generate"

        if self.options.with_profiling:
            tc.variables["CMAKE_C_FLAGS_RELWITHDEBINFO"] = "-O2 -g -DNDEBUG -fno-omit-frame-pointer"
            tc.variables["CMAKE_CXX_FLAGS_RELWITHDEBINFO"] = "-O2 -g -DNDEBUG -fno-omit-frame-pointer"

        tc.generate()
