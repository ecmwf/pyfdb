########################################################################################################################
cmake_minimum_required( VERSION 3.15 FATAL_ERROR )

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(eckit REQUIRED)
find_package(eccodes REQUIRED)
find_package(metkit REQUIRED)
find_package(fdb5 REQUIRED)

# Find the module development requirements (requires FindPython from 3.17 or
# scikit-build-core's built-in backport)
set(PYBIND11_FINDPYTHON ON)
find_package(pybind11 CONFIG REQUIRED)
# find_package(Python REQUIRED COMPONENTS Interpreter Development.Module)

# Scikit-build-core sets these values for you, or you can just hard-code the
# name and version.
project(
  ${SKBUILD_PROJECT_NAME}
  VERSION ${SKBUILD_PROJECT_VERSION}
  LANGUAGES CXX)

# Copy the fdb_py.cc source file from fdb to the pyfdb project
# file(COPY $ENV{FDB_HOME}/include/fdb5/api/fdb_py.cc DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/src/pyfdb)

# Add a library using FindPython's tooling (pybind11 also provides a helper like
# this)

# pybind11_add_module(pyfdb ${CMAKE_CURRENT_SOURCE_DIR}/src/pyfdb/fdb_py.cc)
pybind11_add_module(pyfdb $ENV{FDB_HOME}/include/fdb5/api/fdb_py.cc)

# This is passing in the version as a define just as an example
target_compile_definitions(pyfdb PRIVATE VERSION_INFO=${PROJECT_VERSION})

target_link_libraries(pyfdb PRIVATE
                    eckit
                    eccodes
                    metkit
                    fdb5
                    pybind11::module
                    pybind11::lto
                    pybind11::windows_extras)


pybind11_extension(pyfdb)
if(NOT MSVC AND NOT ${CMAKE_BUILD_TYPE} MATCHES Debug|RelWithDebInfo)
    # Strip unnecessary sections of the binary on Linux/macOS
    pybind11_strip(pyfdb)
endif()

set_target_properties(pyfdb PROPERTIES CXX_VISIBILITY_PRESET "hidden"
                                         CUDA_VISIBILITY_PRESET "hidden")

# The install directory is the output (wheel) directory
install(TARGETS pyfdb DESTINATION ${SKBUILD_PROJECT_NAME})
